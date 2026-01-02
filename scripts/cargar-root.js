// db/scripts/cargar-root.js
const hana = require('@sap/hana-client');
const fs = require('fs');
const path = require('path');
const readline = require('readline');

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout
});

console.log('Iniciando validación y carga de datos ROOT desde CSV...');

// Conexión a HANA Cloud (con nuevas credenciales del HDI)
function getHDIConnectionParams() {
  return {
    serverNode: '573e2afa-fd4a-472e-833d-52bb344eb3ca.hna0.prod-us10.hanacloud.ondemand.com:443',
    uid: '9AB8507A3A88430AB19DFEBD39578A62_9IV3F7YDSOECJ7D0TKUAF50TP_RT',
    pwd: 'Na4QNTnLUqNbwELAp.tlDsP-uAQD_4qw7vLk9WjORswtbfW2Wa9TN8gNyPBz.xyhL0WNLQuIB6vKN0gifXNunGNKXqCrpoxfwdyHRkfXfwTM2m21TeZhVzHdaeXdsans',
    schema: '9AB8507A3A88430AB19DFEBD39578A62',
    encrypt: true,
    sslValidateCertificate: false
  };
}

// Obtener columnas de la tabla
async function getDbColumns(conn, schema, tableName) {
  const query = `
    SELECT COLUMN_NAME 
    FROM SYS.TABLE_COLUMNS 
    WHERE SCHEMA_NAME = ? AND TABLE_NAME = ?
    ORDER BY POSITION
  `;
  return new Promise((resolve, reject) => {
    conn.exec(query, [schema, tableName], (err, rows) => {
      if (err) return reject(new Error(`Error al leer columnas: ${err.message}`));
      if (rows.length === 0) return reject(new Error(`Tabla sin columnas: ${tableName}`));
      resolve(new Set(rows.map(r => r.COLUMN_NAME)));
    });
  });
}

// Obtener OBJECTID existentes (para evitar duplicados)
async function getExistingObjectIds(conn, tableRef) {
  const query = `SELECT UPPER(TRIM("OBJECTID")) AS OBJECTID FROM ${tableRef} WHERE "OBJECTID" IS NOT NULL`;
  return new Promise((resolve, reject) => {
    conn.exec(query, (err, rows) => {
      if (err) return reject(err);
      const ids = new Set(rows.map(row => row.OBJECTID));
      resolve(ids);
    });
  });
}

// Convertir valor booleano a 1 o 0
function toBoolean(value) {
  if (value === null || value === undefined || value === '') return null;
  const str = String(value).trim().toLowerCase();
  return ['true', '1', 'yes', 'on', 'x'].includes(str) ? 1 : 0;
}

// Parsear línea CSV con manejo seguro de comillas y saltos de línea
function parseCSVLine(line) {
  const result = [];
  let inQuotes = false;
  let field = '';
  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    if (char === '"') {
      inQuotes = !inQuotes;
    } else if (char === ',' && !inQuotes) {
      result.push(field.trim());
      field = '';
    } else {
      field += char;
    }
  }
  result.push(field.trim());
  return result;
}

// Cargar un archivo CSV a una tabla
async function cargarCSV(conn, filePath, tableRef, dbColumns, dateTimeColumns, booleanColumns) {
  const contenido = fs.readFileSync(filePath, 'utf-8');
  const lineas = contenido.split('\n').filter(l => l.trim());

  if (lineas.length < 2) return { registros: 0, primerId: null, ultimoId: null };

  // Leer encabezados
  const headers = parseCSVLine(lineas[0]).map(h => h.toUpperCase());
  const filteredHeaders = headers.filter(h => dbColumns.has(h));

  if (filteredHeaders.length === 0) {
    console.warn(`Ninguna columna del CSV coincide con la tabla`);
    return { registros: 0, primerId: null, ultimoId: null };
  }

  // Preparar consulta SQL
  const fieldList = filteredHeaders.map(col => `"${col}"`).join(',');
  const placeholders = filteredHeaders.map(() => '?').join(',');
  const query = `INSERT INTO ${tableRef} (${fieldList}) VALUES (${placeholders})`;

  // Obtener IDs existentes para evitar duplicados
  const existingIds = await getExistingObjectIds(conn, tableRef);

  const batchSize = 1000;
  const batch = [];
  let registrosInsertados = 0;
  let primerId = null;
  let ultimoId = null;

  for (let i = 1; i < lineas.length; i++) {
    const valores = parseCSVLine(lineas[i]);
    const row = {};

    headers.forEach((header, idx) => {
      let value = idx < valores.length ? valores[idx] : null;

      if (value !== null && value !== '') {
        const upperHeader = header.toUpperCase();

        // Limpieza de HTML en campos de texto
        if (typeof value === 'string' && (value.includes('<') || value.includes('>'))) {
          value = value.replace(/<[^>]*>/g, '');
        }

        // Truncar campos problemáticos
        if (upperHeader === 'ZCELULAR1OSV_KUT' && value.length > 50) {
          value = value.substring(0, 50);
        }
        if (upperHeader === 'ZIDDEPARTAMENTO_SDK' && value.length > 64) {
          value = value.substring(0, 64);
        }
        if (upperHeader === 'ZRUC_KUT' && value.length > 20) {
          value = value.trim().replace(/^"(.*)"$/, '$1').substring(0, 20);
        }
        if (upperHeader === 'ZDOCUMENTACIONCOMPLETA_KUT' && value.length > 1000) {
          value = value.substring(0, 1000);
        }
        if (upperHeader === 'ZDIRECCOMERCIAL_KUT' && value.length > 1000) {
          value = value.substring(0, 1000);
        }
        if (upperHeader === 'ZSWITCHCUMPLIMIENTO_SDK' && value.length > 500) {
          value = value.substring(0, 500);
        }

        // Convertir fechas
        if (dateTimeColumns.has(upperHeader)) {
          const date = new Date(value);
          value = isNaN(date) ? null : date.toISOString().slice(0, 19).replace('T', ' ');
        }
        // Convertir booleanos
        else if (booleanColumns.has(upperHeader)) {
          value = toBoolean(value);
        }
        // Otros valores
        else {
          value = String(value);
        }
      } else {
        value = null;
      }

      row[header] = value;
    });

    const objectId = row['OBJECTID'] ? String(row['OBJECTID']).trim().toUpperCase() : null;
    if (!objectId || existingIds.has(objectId)) continue;

    existingIds.add(objectId); // Marcar como insertado

    if (!primerId) primerId = objectId;
    ultimoId = objectId;

    const values = filteredHeaders.map(col => row[col] || null);
    batch.push(values);

    if (batch.length >= batchSize) {
      const stmt = conn.prepare(query);
      const rowsAffected = await new Promise((resolve, reject) => {
        stmt.execBatch(batch, (err, count) => {
          if (err) return reject(new Error(`Error en lote: ${err.message}`));
          resolve(count);
        });
      });
      registrosInsertados += rowsAffected;
      batch.length = 0;
    }
  }

  if (batch.length > 0) {
    const stmt = conn.prepare(query);
    const rowsAffected = await new Promise((resolve, reject) => {
      stmt.execBatch(batch, (err, count) => {
        if (err) return reject(new Error(`Error en lote final: ${err.message}`));
        resolve(count);
      });
    });
    registrosInsertados += rowsAffected;
  }

  return { registros: registrosInsertados, primerId, ultimoId };
}

// Leer archivos CSV de un año
function getCSVFiles(dataDir, anio) {
  const anioDir = path.join(dataDir, anio.toString());
  if (!fs.existsSync(anioDir)) return [];

  return fs.readdirSync(anioDir)
    .filter(f => f.startsWith(`Solicitud_de_servicio_full_${anio}_`) && f.endsWith('.csv'))
    .sort((a, b) => {
      const numA = parseInt(a.match(/_(\d+)\.csv$/)?.[1] || '0', 10);
      const numB = parseInt(b.match(/_(\d+)\.csv$/)?.[1] || '0', 10);
      return numA - numB;
    });
}

// Preguntar al usuario
function pregunta(texto) {
  return new Promise(resolve => {
    rl.question(texto, (respuesta) => {
      resolve(respuesta.trim());
    });
  });
}

// Función principal
async function main() {
  const conn = hana.createConnection();
  const conn_params = getHDIConnectionParams();
  const dataDir = path.join(__dirname, '..', 'data', 'root');
  const anios = [2017, 2018, 2019, 2020, 2021, 2022, 2023];

  try {
    await conn.connect(conn_params);
    console.log(`Conectado a HDI: ${conn_params.schema}\n`);

    const reporteFinal = [];

    // === Preguntar qué año cargar ===
    console.log('Años disponibles:');
    anios.forEach(a => console.log(`   ${a}`));
    console.log('   todos (para cargar todos los años)');

    const respuesta = await pregunta('\n¿Qué año deseas cargar? (ej: 2017, todos): ');

    let aniosSeleccionados = [];
    if (respuesta.toLowerCase() === 'todos') {
      aniosSeleccionados = anios;
    } else {
      const anio = parseInt(respuesta, 10);
      if (isNaN(anio) || !anios.includes(anio)) {
        console.error('Año no válido o no disponible.');
        rl.close();
        conn.disconnect();
        return;
      }
      aniosSeleccionados = [anio];
    }

    for (const anio of aniosSeleccionados) {
      const tableName = `NIUBIZ_CRM_TICKET_NB_ROOT_${anio}`;
      const tableRef = `"${conn_params.schema}"."${tableName}"`;
      const anioDir = path.join(dataDir, anio.toString());
      const archivos = getCSVFiles(dataDir, anio);

      if (archivos.length === 0) {
        console.log(`Año ${anio}: No se encontraron archivos CSV.`);
        continue;
      }

      const dbColumns = new Set(await getDbColumns(conn, conn_params.schema, tableName));
      const dateTimeColumns = new Set(['CREATIONDATETIME', 'REQUESTINPROCESSDATETIMECONTENT']);
      const booleanColumns = new Set(['CHANGEDBYCUSTOMERINDICATOR', 'ESAFILIACION_SDK', 'ZFLAGGUARDADOZ_SDK']);

      console.log(`\nAño ${anio} - Archivos disponibles: ${archivos.length}`);
      console.log(`   → Tabla destino: ${tableName}`);

      // === Cargar primer archivo ===
      const primerArchivo = archivos[0];
      const primerFilePath = path.join(anioDir, primerArchivo);

      console.log(`Cargando primer archivo: ${primerArchivo}`);
      const resultadoPrimero = await cargarCSV(conn, primerFilePath, tableRef, dbColumns, dateTimeColumns, booleanColumns);

      if (resultadoPrimero.registros === 0) {
        console.warn(`No se insertaron registros del primer archivo.`);
        continue;
      }

      console.log(`Primer registro: ${resultadoPrimero.primerId}`);
      console.log(`Último registro: ${resultadoPrimero.ultimoId}`);
      console.log(`Registros cargados: ${resultadoPrimero.registros}`);

      // === Preguntar si cargar el resto ===
      const continuar = await pregunta(`¿Deseas cargar los siguientes ${archivos.length - 1} archivos de ${anio}? (s/n): `);

      let totalRegistros = resultadoPrimero.registros;

      if (continuar.toLowerCase() === 's') {
        for (let i = 1; i < archivos.length; i++) {
          const archivo = archivos[i];
          const filePath = path.join(anioDir, archivo);
          console.log(`Cargando archivo ${i + 1}/${archivos.length}: ${archivo}`);
          const resultado = await cargarCSV(conn, filePath, tableRef, dbColumns, dateTimeColumns, booleanColumns);

          if (resultado.registros > 0) {
            console.log(`Primer registro: ${resultado.primerId}`);
            console.log(`Último registro: ${resultado.ultimoId}`);
          } else {
            console.warn(`No se insertaron registros`);
          }

          totalRegistros += resultado.registros;
        }
      } else {
        console.log(`Carga detenida para el año ${anio}.`);
      }

      reporteFinal.push({
        Año: anio,
        Tabla: tableName,
        'Primer registro': resultadoPrimero.primerId,
        'Último registro': resultadoPrimero.ultimoId,
        '# Registros': totalRegistros
      });
    }

    // Mostrar reporte final
    console.log('\nREPORTE DE CARGA FINAL - ROOT');
    console.table(reporteFinal);

    // Guardar reporte
    const reportePath = path.join(__dirname, 'reporte-carga-root.json');
    fs.writeFileSync(reportePath, JSON.stringify(reporteFinal, null, 2));
    console.log(`Reporte guardado en: ${reportePath}`);

    rl.close();
    conn.disconnect();

  } catch (error) {
    console.error('Error crítico:', error.message);
    rl.close();
    conn.disconnect();
    process.exit(1);
  }
}

main().catch(err => {
  console.error('Error inesperado:', err.message);
  process.exit(1);
});
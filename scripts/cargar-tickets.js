// db/scripts/cargar-tickets.js
const hana = require('@sap/hana-client');
const XLSX = require('xlsx');
const fs = require('fs');
const path = require('path');
const readline = require('readline');

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

console.log('Iniciando carga de datos desde Excel a SAP HANA...');

// Conexión a HANA Cloud
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
            if (rows.length === 0) return reject(new Error(`Tabla no encontrada o sin columnas: ${schema}.${tableName}`));
            resolve(rows.map(row => row.COLUMN_NAME));
        });
    });
}

// Cargar grupo de archivos a una tabla
async function cargarGrupoATabla(archivos, tableName) {
    const conn = hana.createConnection();
    let conn_params;

    try {
        conn_params = getHDIConnectionParams();
        await conn.connect(conn_params);
        console.log(`Conectado a HDI: ${conn_params.schema}`);

        const tableRef = `"${conn_params.schema}"."${tableName}"`;
        const columnNames = await getDbColumns(conn, conn_params.schema, tableName);
        const dateTimeColumns = new Set(['CREATEDON', 'UPDATEDON', 'TEXTCREATEDON']);

        let todosLosDatos = [];
        const archivosCargados = [];

        for (const archivo of archivos) {
            const filePath = path.join(__dirname, '../data', archivo);
            if (!fs.existsSync(filePath)) {
                console.warn(`Archivo no encontrado: ${filePath}`);
                continue;
            }

            try {
                fs.accessSync(filePath, fs.constants.R_OK);
            } catch (err) {
                console.warn(`Archivo no legible: ${filePath}`);
                continue;
            }

            console.log(`Procesando: ${archivo}`);
            archivosCargados.push(archivo);

            const workbook = XLSX.readFile(filePath);
            const sheet = workbook.Sheets[workbook.SheetNames[0]];
            let jsonData = XLSX.utils.sheet_to_json(sheet);

            // Normalizar: claves mayúsculas, truncar TEXT
            jsonData = jsonData.map(row => {
                const newRow = {};
                for (const key in row) {
                    const upperKey = key.trim().toUpperCase();
                    if (upperKey === 'TEXT' && typeof row[key] === 'string' && row[key].length > 2000) {
                        newRow[upperKey] = row[key].substring(0, 2000);
                    } else {
                        newRow[upperKey] = row[key];
                    }
                }
                return newRow;
            });

            todosLosDatos = todosLosDatos.concat(jsonData);
        }

        if (todosLosDatos.length === 0) {
            return {
                Contador: tableName,
                'Primer registro': 'VACIO',
                'Último registro': 'VACIO',
                '# Registros': 0,
                'Archivos cargados': archivosCargados
            };
        }

        // Preparar inserción
        const filteredColumnNames = columnNames.filter(col => col in todosLosDatos[0]);
        const placeholders = filteredColumnNames.map(() => '?').join(',');
        const fieldList = filteredColumnNames.map(col => `"${col}"`).join(',');
        const query = `INSERT INTO ${tableRef} (${fieldList}) VALUES (${placeholders})`;

        // Insertar en lotes
        const batchSize = 1000;
        let registrosInsertados = 0;
        const primerId = todosLosDatos[0]?.OBJECTID || 'N/A';
        const ultimoId = todosLosDatos[todosLosDatos.length - 1]?.OBJECTID || 'N/A';

        for (let i = 0; i < todosLosDatos.length; i += batchSize) {
            const batch = todosLosDatos.slice(i, i + batchSize);
            const params = batch.map(row =>
                filteredColumnNames.map(col => {
                    let value = row[col];
                    if (value === undefined || value === '') return null;

                    // Convertir BOOLEAN
                    if (col.includes('FLAG') || col.includes('BOOLEAN')) {
                        const str = String(value).trim().toLowerCase();
                        return ['true', '1', 'yes', 'on'].includes(str) ? 1 : 0;
                    }

                    // Convertir TIMESTAMP
                    if (dateTimeColumns.has(col)) {
                        const date = new Date(value);
                        return isNaN(date) ? null : date.toISOString().slice(0, 19).replace('T', ' ');
                    }

                    return value;
                })
            );

            const stmt = conn.prepare(query);
            const rowsAffected = await new Promise((resolve, reject) => {
                stmt.execBatch(params, (err, count) => {
                    if (err) return reject(new Error(`Error en lote: ${err.message}`));
                    resolve(count);
                });
            });

            registrosInsertados += rowsAffected;
        }

        return {
            Contador: tableName,
            'Primer registro': primerId,
            'Último registro': ultimoId,
            '# Registros': registrosInsertados,
            'Archivos cargados': archivosCargados
        };

    } catch (error) {
        console.error(`Error cargando ${tableName}:`, error.message);
        return {
            Contador: tableName,
            'Primer registro': 'ERROR',
            'Último registro': 'ERROR',
            '# Registros': 0,
            'Archivos cargados': []
        };
    } finally {
        if (conn) conn.disconnect();
    }
}

// Menú principal
async function main() {
    const dataDir = path.join(__dirname, '../data');

    if (!fs.existsSync(dataDir)) {
        console.error('Carpeta ./data no encontrada. Coloca tus Excel en: db/data/');
        process.exit(1);
    }

    const todosLosArchivos = fs.readdirSync(dataDir)
        .filter(f => f.startsWith('Interacciones_Ticket_') && f.endsWith('.xlsx'))
        .sort((a, b) => {
            const numA = parseInt(a.match(/Interacciones_Ticket_(\d+)/)?.[1] || '0');
            const numB = parseInt(b.match(/Interacciones_Ticket_(\d+)/)?.[1] || '0');
            return numA - numB;
        });

    console.log(`Encontrados ${todosLosArchivos.length} archivos de interacciones`);

    const opciones = `
¿Qué tabla deseas cargar?
1) NIUBIZ_CRM_TICKET_NB_INTERACCION_1  (archivos 1-14)
2) NIUBIZ_CRM_TICKET_NB_INTERACCION_2  (archivos 15-28)
3) NIUBIZ_CRM_TICKET_NB_INTERACCION_3  (archivos 29-42)
4) NIUBIZ_CRM_TICKET_NB_INTERACCION_4  (archivos 43-56)
5) NIUBIZ_CRM_TICKET_NB_INTERACCION_5  (archivos 57-70)
6) NIUBIZ_CRM_TICKET_NB_INTERACCION_6  (archivos 71-84)
7) NIUBIZ_CRM_TICKET_NB_INTERACCION_7  (archivos 85-98)
8) NIUBIZ_CRM_TICKET_NB_INTERACCION_8  (archivos 99-112)
9) NIUBIZ_CRM_TICKET_NB_INTERACCION_9  (archivos 113-126)
10) NIUBIZ_CRM_TICKET_NB_INTERACCION_10 (archivos 127-132)
11) todos
`;

    rl.question(opciones + '\nTu elección (1-11): ', async (respuesta) => {
        const eleccion = parseInt(respuesta.trim());
        const reporteInteracciones = [];

        if (eleccion >= 1 && eleccion <= 10) {
            const i = eleccion - 1;
            const inicio = i * 14;
            const fin = Math.min(inicio + 14, todosLosArchivos.length);
            const grupo = todosLosArchivos.slice(inicio, fin);
            const tableName = `NIUBIZ_CRM_TICKET_NB_INTERACCION_${i + 1}`;

            if (grupo.length > 0) {
                console.log(`Cargando grupo ${i + 1} (${inicio + 1}-${fin}) → ${tableName}`);
                const reporte = await cargarGrupoATabla(grupo, tableName);
                reporteInteracciones.push(reporte);
            }
        } else if (eleccion === 11) {
            for (let i = 0; i < 10; i++) {
                const inicio = i * 14;
                const fin = Math.min(inicio + 14, todosLosArchivos.length);
                const grupo = todosLosArchivos.slice(inicio, fin);
                const tableName = `NIUBIZ_CRM_TICKET_NB_INTERACCION_${i + 1}`;

                if (grupo.length > 0) {
                    console.log(`Cargando grupo ${i + 1} (${inicio + 1}-${fin}) → ${tableName}`);
                    const reporte = await cargarGrupoATabla(grupo, tableName);
                    reporteInteracciones.push(reporte);
                }
            }
        } else {
            console.log('Opción no válida.');
            rl.close();
            process.exit(1);
        }

        // Mostrar reporte final
        console.log('\n\nREPORTE DE CARGA FINAL - INTERACCIONES');
        console.table(reporteInteracciones);

        // Guardar reporte
        const reportePath = path.join(__dirname, 'reporte-carga-interacciones.json');
        fs.writeFileSync(reportePath, JSON.stringify(reporteInteracciones, null, 2));
        console.log(`Reporte guardado en: ${reportePath}`);

        rl.close();
    });
}

main().catch(err => {
    console.error('Error crítico:', err.message);
    process.exit(1);
});

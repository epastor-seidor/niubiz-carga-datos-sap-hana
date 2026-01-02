# Niubiz CRM Data Loader

Este proyecto permite cargar datos de interacciones (Tickets) y solicitudes de servicio (Root) desde archivos locales (Excel y CSV) hacia un contenedor HDI en SAP HANA Cloud.

## Estructura de Datos

Para que los scripts funcionen correctamente, debes organizar los archivos en la carpeta `data` de la siguiente manera:

### 1. Carga de Tickets (Interacciones)
Los archivos de interacciones deben ser de formato Excel (`.xlsx`) y seguir el patrón de nombre `Interacciones_Ticket_*.xlsx`.

- **Ubicación:** `data/`
- **Ejemplo:** `data/Interacciones_Ticket_1.xlsx`

### 2. Carga de Root (Solicitudes de Servicio)
Los archivos Root son de formato CSV y deben estar organizados por año en subcarpetas.

- **Ubicación:** `data/root/[AÑO]/`
- **Patrón de nombre:** `Solicitud_de_servicio_full_[AÑO]_[NUMERO].csv`
- **Ejemplo:** `data/root/2017/Solicitud_de_servicio_full_2017_1.csv`

---

## Requisitos Previos

1.  Tener instalado [Node.js](https://nodejs.org/).
2.  Instalar las dependencias del proyecto:
    ```bash
    npm install
    ```

## Cómo ejecutar la carga

El proyecto cuenta con dos comandos principales configurados en `package.json`:

### Cargar Tickets / Interacciones
Este comando procesa los archivos Excel en la carpeta `data/` y los sube a las tablas correspondientes (`NIUBIZ_CRM_TICKET_NB_INTERACCION_X`).
```bash
npm run cargar-tickets
```

### Cargar Root / Solicitudes de Servicio
Este comando te preguntará qué año deseas cargar (o "todos") y procesará los archivos CSV en `data/root/[AÑO]/`.
```bash
npm run cargar-root
```

---

## Notas Técnicas

- **Conexión:** Los scripts están configurados para conectarse a HANA Cloud con el parámetro `sslValidateCertificate: false` para facilitar la conexión desde entornos locales sin necesidad de instalar certificados adicionales.
- **Evitar Duplicados:** Los scripts verifican el `OBJECTID` antes de insertar para no duplicar registros que ya existan en la base de datos HANA.
- **Reportes:** Al finalizar cada carga, se genera un archivo JSON en la carpeta `scripts/` (ej. `reporte-carga-root.json`) con el resumen de lo procesado.

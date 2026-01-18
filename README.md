
#📊 Proyecto ETL - Sistema de Ventas Cafetost con Power BI
\n

##📋 Descripción General
Este proyecto implementa un proceso ETL (Extract, Transform, Load) completo para analizar datos de ventas de productos y servicios de una empresa. El sistema procesa datos desde archivos CSV brutos hasta dashboards interactivos en Power BI, siguiendo la arquitectura de medallón (bronce, plata, oro) y culminando en visualizaciones empresariales.

##🏗️ Arquitectura del Proyecto
Flujo de Datos Completo

┌───────────────────────────────┐
│   Archivos CSV (RAW)          │
│             ↓                 │
│   Azure Data Lake Storage     │
│         (Bronze)              │
│             ↓                 │
│   Databricks ETL Processing   │
│          (Silver)             │
│             ↓                 │
│   Tablas Gold (Analíticas)    │
│             ↓                 │
│   Power BI Dashboards         │
│             ↓                 │
│   Decisiones Empresariales    │
└───────────────────────────────┘

Capas de Datos Medallón

RAW → BRONZE → SILVER → GOLD → POWER BI

RAW: Archivos CSV originales en Azure Data Lake Storage

BRONZE: Datos crudos ingeridos con metadatos básicos

SILVER: Datos transformados y enriquecidos

GOLD: Datos agregados listos para análisis

POWER BI: Dashboards y reportes interactivos

##Tecnologías Utilizadas
Azure Databricks: Procesamiento distribuido con PySpark

Azure Data Lake Storage Gen2: Almacenamiento de archivos

Unity Catalog: Gobernanza de datos en Databricks

Delta Tables: Tablas transaccionales para análisis

Power BI: Visualización y análisis empresarial

Power BI Gateway: Conexión a los datos

##📁 Estructura de Archivos
###1. preparacion_ambiente.sql

Propósito: Configuración inicial del entorno de Databricks

###Funcionalidades:

✅ Creación del catálogo proyecto_final

✅ Creación de esquemas (raw, bronze, silver, gold)

✅ Definición de external locations para cada capa

✅ Configuración de widgets para parámetros dinámicos

###Ubicaciones externas configuradas:

exlt-raw1: abfss://raw@cursosmartdesarrollo.dfs.core.windows.net/

exlt-bronze1: abfss://bronze@cursosmartdesarrollo.dfs.core.windows.net/

exlt-silver1: abfss://silver@cursosmartdesarrollo.dfs.core.windows.net/

exlt-gold: abfss://gold@cursosmartdesarrollo.dfs.core.windows.net/

###2. ingesta_datos.py

Propósito: Ingestión de datos desde archivos CSV a la capa Bronze

###Fuentes de datos:

tbl_clientes.csv → Información de clientes

tbl_tipo_cliente.csv → Categorías de clientes

tbl_servicios.csv → Catálogo de servicios

tbl_servicios_categoria.csv → Categorías de servicios

tbl_ventas_cafetost_fechas.csv → Ventas de productos

tbl_vendedor.csv → Información de vendedores

tbl_ventas_servicios_fechas.csv → Ventas de servicios

###Procesos realizados:

✅ Lectura de CSV con schemas definidos

✅ Adición de timestamp de ingesta (fecha_ingestion)

✅ Conversión de formatos de fecha

✅ Persistencia en:

Tablas Delta en Databricks Unity Catalog

Archivos Parquet en Azure Data Lake (capa Bronze)

###3. transformador.py

Propósito: Transformación de datos de Bronze a Silver

Transformaciones principales:

###Ventas de Productos:
* Cálculo de la moda del vendedor por cliente

* Agregación por cliente:

* Última fecha de compra

* Total de compras

* Promedio de compra

* Número de compras

* Enriquecimiento con nombres de clientes, tipos y vendedores

###Ventas de Servicios:
* Agregación por servicio y fecha:

* Cantidad total vendida

* Total de venta

* Enriquecimiento con categorías y detalles de servicios

Persistencia en:

Tablas Delta en esquema Silver

Archivos Parquet en Azure Data Lake (capa Silver)

###4. carga_datos_listos.py
Propósito: Carga de datos transformados a la capa Gold

##Procesos:

✅ Lectura desde tablas Silver

✅ Persistencia en:

Tablas Delta en esquema Gold

Archivos Parquet en Azure Data Lake (capa Gold)

##📊 Integración con Power BI

###Conexión de Datos
Power BI se conecta a las tablas Gold de Databricks mediante:

Import Mode: Datos cargados periódicamente

Power BI Gateway: Para actualizaciones programadas

#📈 Dashboards Principales
##1. Dashboard de Ventas de Productos
Métricas clave: 132 ventas totales, ticket promedio $11,120.11

Análisis temporal: Tendencias de Marzo a Noviembre

Segmentación: Por tipo de cliente (Persona/Empresa) y vendedor

Visualizaciones: Gráficos de tendencia, distribución y KPIs

##2. Dashboard de Ventas de Servicios
Métricas clave: 186 ventas, $1.73M en ingresos

Servicios analizados: Cafetería, Tueste, Marca de Café, Cata

Comportamiento mensual: Patrones de venta por servicio

Visualizaciones: Gráficos combinados y análisis comparativo



-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Creación del catalago y esquemas necesarios

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.removeAll() #remueve todos los widgets, para este notebook el lenguaje de default es SQL por eso se usa el magic command %python 

-- COMMAND ----------

create widget text storageName default "cursosmartdesarrollo"; --crea un widget nuevo para poder definir el contenedor de Azure con el que se va a trabajar

-- COMMAND ----------

DROP CATALOG IF EXISTS proyecto_final CASCADE; -- elimina el catalogo 'proyecto_final' en caso de exista, puede ser que no necesite esto en un caso real /P/-> ¿Por qué eliminaría un catalagó para la preparación de un ambiente?

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS proyecto_final; -- crea el catalogo 'proyecto_final' en caso de no existir, y en caso de existir no hace nada lo que evita conflictos con la ejecución del código

-- COMMAND ----------

--crea los esquemas necesarios para el proceso ETL
CREATE SCHEMA IF NOT EXISTS proyecto_final.raw;
CREATE SCHEMA IF NOT EXISTS proyecto_final.bronze;
CREATE SCHEMA IF NOT EXISTS proyecto_final.silver;
CREATE SCHEMA IF NOT EXISTS proyecto_final.gold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #Creación de los external locations

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Es necesario tener las external locations para poder usar lo que haya en el contenedor

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-raw1`
URL 'abfss://raw@${storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL credential)
COMMENT 'Ubicación externa para las tablas raw del Data Lake';

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-bronze1`
URL 'abfss://bronze@${storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL credential)
COMMENT 'Ubicación externa para las tablas bronze del Data Lake';

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-silver1`
URL 'abfss://silver@${storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL credential)
COMMENT 'Ubicación externa para las tablas silver del Data Lake';

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS `exlt-gold`
URL 'abfss://gold@${storageName}.dfs.core.windows.net/'
WITH (STORAGE CREDENTIAL credential)
COMMENT 'Ubicación externa para las tablas gold del Data Lake';

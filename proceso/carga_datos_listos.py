# Databricks notebook source
# MAGIC %md
# MAGIC #Liberias

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC #Widgets

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("catalogo", "proyecto_final")
dbutils.widgets.text("esquema_source", "silver")
dbutils.widgets.text("esquema_sink", "gold")

# COMMAND ----------

catalogo = dbutils.widgets.get("catalogo")
esquema_source = dbutils.widgets.get("esquema_source")
esquema_sink = dbutils.widgets.get("esquema_sink")

# COMMAND ----------

# MAGIC %md
# MAGIC #Definición de rutas

# COMMAND ----------

#Ruta final
ruta_final = f"abfss://{esquema_sink}@cursosmartdesarrollo.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %md
# MAGIC #Lectura desde el catálogo de Databricks

# COMMAND ----------

df_resumen_ventas_productos = spark.table(f"{catalogo}.{esquema_source}.tbl_resumen_ventas_productos")
df_resumen_ventas_servicios = spark.table(f"{catalogo}.{esquema_source}.tbl_resumen_ventas_servicios")

# COMMAND ----------

#verificar que se haya cargado bien
#df_resumen_ventas_productos.display()
#df_resumen_ventas_servicios.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Guardar en tabla delta dentro de Databricks

# COMMAND ----------

#Resumen de las ventas de los productos
df_resumen_ventas_productos.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_sink}.tbl_resumen_ventas_productos")

#Resumen de las ventas de los servicios
df_resumen_ventas_servicios.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_sink}.tbl_resumen_ventas_servicios")

# COMMAND ----------

# MAGIC %md
# MAGIC #Guardar en la capa gold en Data Lake de Azure

# COMMAND ----------

#Resumen de las ventas de los productos
df_resumen_ventas_productos.write.mode("overwrite").parquet(f"{ruta_final}/tbl_resumen_ventas_productos.csv")

#Resumen de las ventas de los servicios
df_resumen_ventas_servicios.write.mode("overwrite").parquet(f"{ruta_final}/tbl_resumen_ventas_servicios.csv")

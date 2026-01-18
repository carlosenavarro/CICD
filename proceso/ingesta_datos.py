# Databricks notebook source
# MAGIC %md
# MAGIC #Librerias

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC #Widgets

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("container", "raw")
dbutils.widgets.text("catalogo", "proyecto_final")
dbutils.widgets.text("esquema", "bronze")

# COMMAND ----------

container = dbutils.widgets.get("container")
catalogo = dbutils.widgets.get("catalogo")
esquema = dbutils.widgets.get("esquema")

# COMMAND ----------

# MAGIC %md
# MAGIC #Definición de rutas

# COMMAND ----------

#Rutas de los archivos
ruta_base = f"abfss://{container}@cursosmartdesarrollo.dfs.core.windows.net" #f"abfss://(container)@(datalake_Azure).dfs.core.windows.net/tbl_clientes.csv
#abfss porque es un datalake en azure storage, hay que buscar cuales serían los estándar para otras fuentes

ruta_clientes = f"{ruta_base}/tbl_clientes.csv" 
ruta_tipo_cliente = f"{ruta_base}/tbl_tipo_cliente.csv"
ruta_servicios = f"{ruta_base}/tbl_servicios.csv"
ruta_cartegoria_servicios = f"{ruta_base}/tbl_servicios_categoria.csv"
ruta_ventas_productos = f"{ruta_base}/tbl_ventas_cafetost_fechas.csv"
ruta_vendedores = f"{ruta_base}/tbl_vendedor.csv"
ruta_ventas_servicios = f"{ruta_base}/tbl_ventas_servicios_fechas.csv"

# COMMAND ----------

#Ruta final
ruta_final = f"abfss://{esquema}@cursosmartdesarrollo.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %md
# MAGIC #Definición de los tipos de columna

# COMMAND ----------

#clientes
clientes_schema = StructType(fields=[StructField("id_cliente", IntegerType(), False),
                                     StructField("nombre_cliente", StringType(), False),
                                     StructField("id_tipo_cliente", IntegerType(), False)
])

#tipo de cliente
tipo_cliente_schema = StructType(fields=[StructField("id_tipo_cliente", IntegerType(), False),
                                     StructField("tipo_cliente", StringType(), False)
])

#servicios
servicios_schema = StructType(fields=[StructField("id_servicio", IntegerType(), False),
                                     StructField("id_servicios_categoria", IntegerType(), False),
                                     StructField("servicios_detalle", StringType(), False)
                                     
])

#categoria de los servicios
cartegoria_servicios_schema = StructType(fields=[StructField("id_servicios_categoria", IntegerType(), False),
                                     StructField("categoria", StringType(), False),
                                     StructField("subcategoria", StringType(), False),
                                     StructField("tipo", StringType(), False)
])

#ventas de los productos 
ventas_productos_schema = StructType(fields=[StructField("id_vct", IntegerType(), False),
                                     StructField("id_cliente", IntegerType(), False),
                                     StructField("fecha_venta", StringType(), False),
                                     StructField("total_venta", FloatType(), False),
                                     StructField("id_vendedor", IntegerType(), False)
]) 

#vendedores
vendedores_schema = StructType(fields=[StructField("id_vendedor", IntegerType(), False),
                                     StructField("nombre_vendedor", StringType(), False)
]) 

#ventas de los servicios
ventas_servicios_schema = StructType(fields=[StructField("id_vct", IntegerType(), False),
                                     StructField("id_producto", IntegerType(), False),
                                     StructField("cantidad", IntegerType(), False),
                                     StructField("total_venta", FloatType(), False),  
                                     StructField("precio_venta_total", FloatType(), False),
                                     StructField("fecha", StringType(), False)
]) 

#por alguna razón no sirve, no sirve definir las fechas desde este paso

# COMMAND ----------

# MAGIC %md
# MAGIC #Carga de los datos

# COMMAND ----------

#clientes
df_clientes = spark.read.option('header', True)\
                        .schema(clientes_schema)\
                        .option('delimiter', ';')\
                        .csv(ruta_clientes)

#tipo de cliente
df_tipo_cliente = spark.read.option('header', True)\
                        .schema(tipo_cliente_schema)\
                        .option('delimiter', ';')\
                        .csv(ruta_tipo_cliente)

#servicios
df_servicios = spark.read.option('header', True)\
                        .schema(servicios_schema)\
                        .option('delimiter', ';')\
                        .csv(ruta_servicios)

#categoria de los servicios 
df_categoria_servicios = spark.read.option('header', True)\
                        .schema(cartegoria_servicios_schema)\
                        .option('delimiter', ';')\
                        .csv(ruta_cartegoria_servicios)

#ventas de los productos
df_ventas_productos = spark.read.option('header', True)\
                        .schema(ventas_productos_schema)\
                        .option('delimiter', ',')\
                        .csv(ruta_ventas_productos)

#vendedores
df_vendedores = spark.read.option('header', True)\
                        .schema(vendedores_schema)\
                        .option('delimiter', ';')\
                        .csv(ruta_vendedores)

#ventas de los servicios 
df_ventas_servicios = spark.read.option('header', True)\
                        .schema(ventas_servicios_schema)\
                        .option('delimiter', ',')\
                        .csv(ruta_ventas_servicios)

# COMMAND ----------

#Agregar una fecha de ingestión (última modificación)
df_clientes = df_clientes.withColumn("fecha_ingestion", current_timestamp())
df_tipo_cliente = df_tipo_cliente.withColumn("fecha_ingestion", current_timestamp())
df_servicios = df_servicios.withColumn("fecha_ingestion", current_timestamp())
df_categoria_servicios = df_categoria_servicios.withColumn("fecha_ingestion", current_timestamp())
df_ventas_productos = df_ventas_productos.withColumn("fecha_ingestion", current_timestamp())
df_vendedores = df_vendedores.withColumn("fecha_ingestion", current_timestamp())
df_ventas_servicios = df_ventas_servicios.withColumn("fecha_ingestion", current_timestamp())


# COMMAND ----------

#Ajustar las fechas al formato correcto
df_ventas_productos = df_ventas_productos.withColumn(
    "fecha_venta", 
    to_date(col("fecha_venta"), "yyyy-MM-dd")  # Ajusta el formato
)

df_ventas_servicios = df_ventas_servicios.withColumn(
    "fecha", 
    to_date(col("FECHA"), "yyyy-MM-dd")  # Ajusta el formato
)

# COMMAND ----------

#verificar que todo se haya cargado correctamente
#df_clientes.display()
#df_tipo_cliente.display()
#df_servicios.display()
#df_categoria_servicios.display()
#df_vendedores.display()
#df_ventas_productos.display()
#df_ventas_servicios.display()


# COMMAND ----------

# MAGIC %md
# MAGIC #Guardar en tabla delta dentro de Databricks

# COMMAND ----------

#clientes
df_clientes.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema}.tbl_clientes")

#tipo de cliente
df_tipo_cliente.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema}.tbl_tipo_cliente")

#servicios
df_servicios.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema}.tbl_servicios")

#categoria de los servicios
df_categoria_servicios.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema}.tbl_servicios_categoria")

#ventas de los productos
df_ventas_productos.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema}.tbl_ventas_cafetost")

#vendedores
df_vendedores.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema}.tbl_vendedor")

#ventas de los servicios
df_ventas_servicios.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema}.tbl_ventas_servicios")


# COMMAND ----------

# MAGIC %md
# MAGIC #Guardar en la capa bronze en Data Lake de Azure

# COMMAND ----------

df_clientes.write.mode("overwrite").parquet(f"{ruta_final}/clientes.csv")
df_tipo_cliente.write.mode("overwrite").parquet(f"{ruta_final}/tipo_cliente.csv")
df_servicios.write.mode("overwrite").parquet(f"{ruta_final}/servicios.csv")
df_categoria_servicios.write.mode("overwrite").parquet(f"{ruta_final}/categoria_servicios.csv")
df_ventas_productos.write.mode("overwrite").parquet(f"{ruta_final}/ventas_productos.csv")
df_vendedores.write.mode("overwrite").parquet(f"{ruta_final}/vendedores.csv")
df_ventas_servicios.write.mode("overwrite").parquet(f"{ruta_final}/ventas_servicios.csv")

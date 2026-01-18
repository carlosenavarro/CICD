# Databricks notebook source
# MAGIC %md
# MAGIC #Liberias

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC #Widgets

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.text("catalogo", "proyecto_final")
dbutils.widgets.text("esquema_source", "bronze")
dbutils.widgets.text("esquema_sink", "silver")

# COMMAND ----------

catalogo = dbutils.widgets.get("catalogo")
esquema_origen = dbutils.widgets.get("esquema_source")
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

df_clientes = spark.table(f"{catalogo}.{esquema_origen}.tbl_clientes")
df_tipo_cliente = spark.table(f"{catalogo}.{esquema_origen}.tbl_tipo_cliente")
df_servicios = spark.table(f"{catalogo}.{esquema_origen}.tbl_servicios")
df_servicios_categoria = spark.table(f"{catalogo}.{esquema_origen}.tbl_servicios_categoria")
df_ventas_productos = spark.table(f"{catalogo}.{esquema_origen}.tbl_ventas_cafetost")
df_vendedor = spark.table(f"{catalogo}.{esquema_origen}.tbl_vendedor")
df_ventas_servicios = spark.table(f"{catalogo}.{esquema_origen}.tbl_ventas_servicios")

# COMMAND ----------

# MAGIC %md
# MAGIC #Transfomación de los datos

# COMMAND ----------

#Transformación de df_ventas_productos, df_vendedor, df_tipo_cliente y df_clientes
df_moda_vendedor_productos = (df_ventas_productos
    .groupBy("id_cliente", "id_vendedor")
    .agg(count("*").alias("freq"))
    .withColumn("rn", row_number().over(
        Window.partitionBy("id_cliente").orderBy(desc("freq"), desc("id_vendedor"))
    ))
    .filter(col("rn") == 1)
    .select("id_cliente", col("id_vendedor").alias("moda_vendedor"))
)

df_ventas_productos_agregados = (df_ventas_productos
    .groupBy("id_cliente")
    .agg(
        max("fecha_venta").alias("ultima_fecha_compra"),
        sum("total_venta").alias("total_compras"),
        round(avg("total_venta"),2).alias("promedio_compra"),
        count("*").alias("numero_compras")
    )
    .join(df_moda_vendedor_productos, "id_cliente", "left")
    .orderBy("id_cliente")
)

df_ventas_productos_agregados_nombres = (df_ventas_productos_agregados
    .join(df_clientes, "id_cliente", "left")
    .join(df_tipo_cliente, "id_tipo_cliente", "left")
    .join(df_vendedor, df_ventas_productos_agregados.moda_vendedor == df_vendedor.id_vendedor, "left")  
    .select(
        "id_cliente", 
        "nombre_cliente", 
        "ultima_fecha_compra",
        "total_compras",
        "promedio_compra",
        "numero_compras",
        "tipo_cliente",
        col("moda_vendedor").alias("id_vendedor_moda"),  
        col("nombre_vendedor").alias("nombre_vendedor_moda") 
    )
    .orderBy("id_cliente"))

# COMMAND ----------

#Transformación de df_ventas_servicios, df_servicios y df_servicios_categoria
df_ventas_servicios_fecha = (df_ventas_servicios
    .groupBy("id_producto", "FECHA")
    .agg(
        sum("cantidad").alias("cantidad_total"),
        sum("precio_venta_total").alias("total_venta")
    )
    .withColumnRenamed("id_producto", "id_servicio")
    .withColumnRenamed("FECHA", "fecha")
    .orderBy("fecha", "id_producto")
)

df_ventas_servicios_fecha_detalles = (df_ventas_servicios_fecha
    .join(df_servicios, "id_servicio", "left")
    .join(df_servicios_categoria, "id_servicios_categoria", "left")
    .select('id_servicio',
            'id_servicios_categoria',
            'categoria',
            'subcategoria',
            'servicios_detalle',
            'tipo',
            'cantidad_total',
            'total_venta',
            'fecha'))

# COMMAND ----------

#verificar que todo este bien transformado
#df_ventas_productos_agregados_nombres.display()
#df_ventas_servicios_fecha_detalles.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #Guardar en tabla delta dentro de Databricks

# COMMAND ----------

#Resumen de las ventas de los productos
df_ventas_productos_agregados_nombres.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_sink}.tbl_resumen_ventas_productos")

#Resumen de las ventas de los servicios
df_ventas_servicios_fecha_detalles.write.mode("overwrite").saveAsTable(f"{catalogo}.{esquema_sink}.tbl_resumen_ventas_servicios")


# COMMAND ----------

# MAGIC %md
# MAGIC #Guardar en la capa silver en Data Lake de Azure

# COMMAND ----------

#Resumen de las ventas de los productos
df_ventas_productos_agregados_nombres.write.mode("overwrite").parquet(f"{ruta_final}/tbl_resumen_ventas_productos.csv")

#Resumen de las ventas de los servicios
df_ventas_servicios_fecha_detalles.write.mode("overwrite").parquet(f"{ruta_final}/tbl_resumen_ventas_servicios.csv")

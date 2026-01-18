# Databricks notebook source
# MAGIC %md
# MAGIC #Dar permisos individuales

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT ALL PRIVILEGES ON CATALOG proyecto_final TO `carlos_datasmart@outlook.com`;
# MAGIC GRANT CREATE, USE SCHEMA ON SCHEMA  proyecto_final.bronze to `carlos_datasmart@outlook.com`;
# MAGIC GRANT CREATE, USE SCHEMA ON SCHEMA  proyecto_final.silver to `carlos_datasmart@outlook.com`;
# MAGIC GRANT CREATE, USE SCHEMA ON SCHEMA  proyecto_final.gold to `carlos_datasmart@outlook.com`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `exlt-raw1` to `carlos_datasmart@outlook.com`;
# MAGIC GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `exlt-bronze1` to `carlos_datasmart@outlook.com`;
# MAGIC GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `exlt-silver1` to `carlos_datasmart@outlook.com`;
# MAGIC GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `exlt-gold` to `carlos_datasmart@outlook.com`;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Dar permisos grupales

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT ALL PRIVILEGES ON CATALOG proyecto_final TO `Ingenieros`;
# MAGIC GRANT CREATE, USE SCHEMA ON SCHEMA  proyecto_final.bronze to `Ingenieros`;
# MAGIC GRANT CREATE, USE SCHEMA ON SCHEMA  proyecto_final.silver to `Ingenieros`;
# MAGIC GRANT CREATE, USE SCHEMA ON SCHEMA  proyecto_final.gold to `Ingenieros`;

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `exlt-raw1` to `Ingenieros`;
# MAGIC GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `exlt-bronze1` to `Ingenieros`;
# MAGIC GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `exlt-silver1` to `Ingenieros`;
# MAGIC GRANT READ FILES, WRITE FILES ON EXTERNAL LOCATION `exlt-gold` to `Ingenieros`;

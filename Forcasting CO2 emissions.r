# Databricks notebook source
# MAGIC %md
# MAGIC # Forcasting CO2 emissions

# COMMAND ----------

# MAGIC %md
# MAGIC #### Libraries and set environemnt

# COMMAND ----------

library(tidyverse)
library(sparklyr)
sc <- spark_connect(method = "databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import data

# COMMAND ----------

tbl_change_db(sc, "")
spark_df <- spark_read_table(
  sc = sc,
  name = ""
)

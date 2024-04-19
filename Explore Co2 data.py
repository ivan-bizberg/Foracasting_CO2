# Databricks notebook source
# MAGIC %md
# MAGIC # Explore Co2 data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import libraries

# COMMAND ----------

import pandas as pd
import numpy as np

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import functions

# COMMAND ----------

def load_from_db(database_name, table_name, to_pandas=True):
    spark = SparkSession.builder.appName("hive_metastore").enableHiveSupport().getOrCreate()
    df = spark.table(f"{database_name}.{table_name}")
    if to_pandas:
        return df.toPandas()
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quickly check all data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Usage Data 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.GMS_US_MART.TXN_MRT_EHS_TANGO_MSR_ROLLUPS_GLBL
# MAGIC WHERE CITY_NM = "CiudaddeMexico"
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT BUILDING_NM, Aud_Ld_Dts, COUNT(Aud_Ld_Dts)
# MAGIC FROM GMS_US_MART.TXN_MRT_EHS_TANGO_MSR_ROLLUPS_GLBL
# MAGIC GROUP BY BUILDING_NM, Aud_Ld_Dts
# MAGIC ORDER BY BUILDING_NM, Aud_Ld_Dts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT CAST(R_MSR_VAL AS FLOAT), COUNT(CAST(R_MSR_VAL AS FLOAT))
# MAGIC FROM GMS_US_MART.TXN_MRT_EHS_TANGO_MSR_ROLLUPS_GLBL
# MAGIC WHERE CITY_NM = "CiudaddeMexico"
# MAGIC GROUP BY CITY_NM, R_MSR_VAL
# MAGIC ORDER BY CITY_NM ASC
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT RPRTNG_LVL, EHS_FUNC_DESC, 
# MAGIC EHS_BU_DESC, COUNT(RPRTNG_LVL) 
# MAGIC FROM GMS_US_MART.TXN_MRT_EHS_TANGO_MSR_ROLLUPS_GLBL
# MAGIC GROUP BY RPRTNG_LVL, EHS_FUNC_DESC, 
# MAGIC EHS_BU_DESC
# MAGIC ORDER BY RPRTNG_LVL

# COMMAND ----------

# MAGIC %md
# MAGIC ### Electricity

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM GMS_US_MART.MRT_EHS_TANGO_MSR_ROLLUPS_GLBL
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Natural Gas

# COMMAND ----------

# MAGIC %md
# MAGIC SELECT * FROM GMS_US_MART.TXN_CNSPN_MTRCS_GLBL

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC
# MAGIC SELECT Site_nm, Building_ID, COUNT(Site_nm) 
# MAGIC FROM GMS_US_MART.TXN_CNSPN_MTRCS_GLBL
# MAGIC GROUP BY Site_nm, Building_ID
# MAGIC ORDER BY Site_nm ASC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Steam Usage

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM GMS_US_MART.TXN_CNSPN_MTRCS_GLBL
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load data

# COMMAND ----------

"TXN_MRT_EHS_TANGO_MSR_ROLLUPS_GLBL".lower()

# COMMAND ----------

# df = load_from_db("gms_us_mart", "txn_mrt_ehs_tango_msr_rollups_glbl")

# COMMAND ----------

df_spark = spark.sql("SELECT *, CAST(R_MSR_VAL AS FLOAT) AS R_MSR_VAL_FLOAT FROM hive_metastore.gms_us_mart.txn_mrt_ehs_tango_msr_rollups_glbl")
df_spark = df_spark.drop("R_MSR_VAL")
#df_spark_c = df_spark.withColumn("R_MSR_VAL", col("R_MSR_VAL").cast("float"))
df_spark.display()


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC DESCRIBE hive_metastore.gms_us_mart.txn_mrt_ehs_tango_msr_rollups_glbl

# COMMAND ----------

df_spark.printSchema()

# COMMAND ----------

df_spark.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore data

# COMMAND ----------

df_MEX_spark = spark.sql("SELECT * FROM hive_metastore.gms_us_mart.txn_mrt_ehs_tango_msr_rollups_glbl WHERE CITY_NM = 'CiudaddeMexico'")
df_MEX = df_MEX_spark.toPandas()

# COMMAND ----------

# df_MEX.shape
# df_MEX.head(5)
# df_MEX.columns
df_MEX["R_MSR_VAL"] = pd.to_numeric(df_MEX["R_MSR_VAL"])
df_MEX.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualize data

# COMMAND ----------

import plotly.express as px

# COMMAND ----------

#df_MEX.display()

print(df_MEX["BUILDING_NM"].value_counts(dropna=False))

# COMMAND ----------


df = df_MEX.query("BUILDING_NM == 'ProlongacionVascodeQuiroga4800' & RPRTNG_LVL == 'BUSINESS_OPERATION'")
plot_df = df.groupby("FSCL_YR").agg({"R_MSR_VAL" : "mean"}).reset_index()


# COMMAND ----------

df.nunique()
plot_df.display()
# df.describe()
plot_df.toPandas()

# COMMAND ----------

px.bar(plot_df, x = "FSCL_YR", y = "R_MSR_VAL", color = "R_MSR_VAL", color_continuous_scale = "viridis",
       labels = {
           "FSCL_YR" : "Fiscal Year",
           "R_MSR_VAL" : "Energy"
       })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore multiple countries with loops

# COMMAND ----------

Cities = spark.sql("SELECT DISTINCT(CITY_NM) FROM hive_metastore.gms_us_mart.txn_mrt_ehs_tango_msr_rollups_glbl")
Cities.display()

# COMMAND ----------

v_city = Cities.toPandas()
list_city = v_city["CITY_NM"].tolist().copy()

# COMMAND ----------

Final_df = pd.DataFrame()
for i in list_city[0:10]:
    print(i)
    df_spark = spark.sql(f"SELECT * FROM hive_metastore.gms_us_mart.txn_mrt_ehs_tango_msr_rollups_glbl WHERE CITY_NM = '{i}'")
    df = df_spark.toPandas()
    df_agg = df.groupby("FSCL_YR").agg({"R_MSR_VAL" : "mean"}).reset_index().assign(City = f"{i}")
    Final_df = pd.concat([Final_df, df_agg])

print(Final_df)

# COMMAND ----------

px.bar(Final_df, x = "FSCL_YR", y = "R_MSR_VAL", color="City", barmode="group")

# COMMAND ----------

Final_df = pd.DataFrame()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM hive_metastore.gms_us_mart.txn_mrt_ehs_tango_msr_rollups_glbl WHERE CITY_NM = "Tyler"

# COMMAND ----------

Cities.display

# COMMAND ----------

list_city[0:2]

# COMMAND ----------

Cities = spark.sql("SELECT CITY_NM FROM hive_metastore.gms_us_mart.txn_mrt_ehs_tango_msr_rollups_glbl")
df_MEX = df_MEX_spark.toPandas()

df_spark = spark.sql("SELECT * FROM hive_metastore.gms_us_mart.txn_mrt_ehs_tango_msr_rollups_glbl WHERE CITY_NM = 'CiudaddeMexico'")
df_MEX = df_MEX_spark.toPandas()

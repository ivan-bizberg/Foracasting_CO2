# Databricks notebook source
# MAGIC %md
# MAGIC Test code chunks

# COMMAND ----------

extract_spot_empo()
    


# COMMAND ----------

from databricks import sql

# COMMAND ----------

connection = sql.connect(server_hostname = "onetakeda-usprd.cloud.databricks.com",
                        http_path = "sql/protocolv1/o/2186391591496286/1201-135729-4c9gjccf")
cursor = connection.cursor()

# COMMAND ----------

import numpy as np
import os
import pandas as pd
import pyodbc



###############################################################################
### enablon
def extract_enablon(cursor, msr):
    """
    Extracts usage data for various measurements/indicators from enablon data stored 
    in the GMS_US_MART.TXN_MRT_EHS_TANGO_MSR_ROLLUPS_GLBL table ind EDB.
    
    Args:
        cursor: The database cursor object.
        msr (str): The measurement to extract.
    
    Returns:
        pandas.DataFrame: A DataFrame containing the extracted data.
    """
    print(msr)
    dat_enablon = (cursor.execute("""
        select distinct
            MSR,
            SYSTM_SPCFIC_MSR,
            BUILDING_ID,
            CTRY_DESC,
            FSCL_MNTH_NO,
            FSCL_QRTR,
            FSCL_YR,
            R_MSR_VAL,
            R_MSR_UNT
        from GMS_US_MART.TXN_MRT_EHS_TANGO_MSR_ROLLUPS_GLBL
        where
        R_MSR_UNT = 'J'
        and MSR = '""" + msr + """'
        and RPRTNG_LVL = 'GEO'
        and EHS_FUNC_DESC is null
        and EHS_BU_DESC is null
        and FSCL_MNTH_NO is not null
        and FSCL_QRTR is not null
        and FSCL_YR is not null -- overall sum
        and BUILDING_ID is not null;""").fetchall())
    dat_enablon = pd.DataFrame(dat_enablon)
    dat_enablon.columns = [
        'MSR',
        'SYSTM_SPCFIC_MSR',
        'BUILDING_ID',
        'Cntry',
        'FSCL_MNTH_NO',
        'FSCL_QRTR',
        'FSCL_YR',
        'R_MSR_VAL',
        'R_MSR_UNT']
    print(dat_enablon)
    return dat_enablon


# COMMAND ----------

    dat_enablon = (cursor.execute("""
        select distinct
            MSR,
            SYSTM_SPCFIC_MSR,
            BUILDING_ID,
            CTRY_DESC,
            FSCL_MNTH_NO,
            FSCL_QRTR,
            FSCL_YR,
            R_MSR_VAL,
            R_MSR_UNT
        from GMS_US_MART.TXN_MRT_EHS_TANGO_MSR_ROLLUPS_GLBL
        where
        R_MSR_UNT = 'J'
        and MSR = '""" + msr + """'
        and RPRTNG_LVL = 'GEO'
        and EHS_FUNC_DESC is null
        and EHS_BU_DESC is null
        and FSCL_MNTH_NO is not null
        and FSCL_QRTR is not null
        and FSCL_YR is not null -- overall sum
        and BUILDING_ID is not null;""").fetchall())

# Databricks notebook source
# MAGIC %pip list
# MAGIC %pip show pandas

# COMMAND ----------

# MAGIC %pip install redis
# MAGIC

# COMMAND ----------

# MAGIC %pip install databricks

# COMMAND ----------

import numpy as np
import os
import pandas as pd
import pickle
import redis

# COMMAND ----------

# MAGIC %md
# MAGIC # extract.py

# COMMAND ----------

###############
### extract ###
###############

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



###############################################################################
### enablon: special case natural gas
def extract_natural_gas(cursor):
    """
    Extracts natural gas usage data from Enablon data stored in the gms_us_mart.txn_cnspn_mtrcs_glbl table in EDB.
    Natural gas is either reported in joules or in cubic meters. Cubic meters must be converted to joules prior to
    the modelling step. The conversion factor for m3 natural gas into joules depends on the country. Therefore, 
    include the Cntry column.

    Args:
        cursor: The database cursor object.
    
    Returns:
        pandas.DataFrame: A DataFrame containing the extracted data.
    
    Example:
        data = extract_natural_gas(cursor)
    """
    print('extract natural gas')
    dat = (cursor.execute("""
        select 
            FldrPth,
            Building_ID,
            Cntry,
            Rprtg_Prd_Key_2,
            Rfrnc_Key,	
            Cd_Key_2,
            Unvrsl_Val,	
            Unvrsl_Unt
        from gms_us_mart.txn_cnspn_mtrcs_glbl
        where Cd_Key_2 like '%Energy.2a%'
        ;""").fetchall())
    dat = pd.DataFrame(dat)
    dat.columns = [
        'FOLDERPATH',
        'BUILDING_ID',
        'Cntry',
        'Month',
        'MSR',
        'SYSTM_SPCFIC_MSR',
        'y',
        'R_MSR_UNT']
    return dat



###############################################################################
### enablon: special case steam
def extract_steam(cursor):
    """
    Extracts steam usage data from Enablon data stored in the gms_us_mart.txn_cnspn_mtrcs_glbl table in EDB.

    Args:
        cursor: The database cursor object.
    
    Returns:
        pandas.DataFrame: A DataFrame containing the extracted data.
    
    Example:
        data = extract_steam(cursor)
    """
    print('extract steam')
    dat = (cursor.execute("""
        select 
            FldrPth,
            Building_ID,
            Cntry,	
            Rprtg_Prd_Key_2,
            Rfrnc_Key,	
            Cd_Key_2,
            Unvrsl_Val,	
            Unvrsl_Unt
        from gms_us_mart.txn_cnspn_mtrcs_glbl
        where Cd_Key_2 like '%Energy.11a.mass%'
            or Cd_Key_2 like '%Energy.11a.nrg%'
        ;""").fetchall())
    dat = pd.DataFrame(dat)
    dat.columns = [
        'FOLDERPATH',
        'BUILDING_ID',
        'Cntry',
        'Month',	
        'MSR',
        'SYSTM_SPCFIC_MSR',
        'y',
        'R_MSR_UNT']
    return dat





###############################################################################
### tango/folderpath
def extract_tango(cursor):
    """
    Extracts building id and folderpath information from the tango data stored in the GMS_US_MART.TXN_CNSPN_MTRCS_GLBL table in EDB.
    
    Args:
        cursor: The database cursor object.
    
    Returns:
        pandas.DataFrame: A DataFrame containing the extracted data.
    """
    tango_fp = (cursor.execute("""
        select
            distinct
            Building_ID,
            FldrPth
        from
            GMS_US_MART.TXN_CNSPN_MTRCS_GLBL;""").fetchall())
    tango_fp = pd.DataFrame(tango_fp)
    tango_fp.columns = ['BUILDING_ID', 'FOLDERPATH']
    return tango_fp



###############################################################################
### flag
def extract_flag(cursor):
    """
    Extracts information on flagged (divested) sites from the GMS_US_MART.REF_MRT_EHS_TANGO_FOOTPRINT table in EDB.
    
    Args:
        cursor: The database cursor object.
    
    Returns:
        pandas.DataFrame: A DataFrame containing the extracted data.
    """
    flag = (cursor.execute("""
        select
            BUILDING_ID,
            BUILDING_NM,
            BUILDING_STAT_DESC,
            EHS_DATA_SHOW_FLG
        from GMS_US_MART.REF_MRT_EHS_TANGO_FOOTPRINT
        where EHS_DATA_SHOW_FLG = 'No' 
        """).fetchall())
    flag = pd.DataFrame(flag)
    flag.columns = [
        'BUILDING_ID', 
        'BUILDING_NM', 
        'BUILDING_STAT_DESC', 
        'EHS_DATA_SHOW_FLG']
    return flag


# NO are remove it is correct
###############################################################################
### leaks
def extract_leaks(cursor):
    """
    Extracts information on refrigerant leakages from the GMS_US_MART.TXN_MRT_EHS_TANGO_MSR_ROLLUPS_GLBL table in EDB.
    
    Args:
        cursor: The database cursor object.
    
    Returns:
        pandas.DataFrame: A DataFrame containing the extracted data.
    """
    leaks = (cursor.execute("""
        select distinct
            MSR,
            SYSTM_SPCFIC_MSR,
            BUILDING_ID,
            FSCL_MNTH_NO,
            FSCL_QRTR,
            FSCL_YR,
            R_MSR_VAL,
            R_MSR_UNT
        from GMS_US_MART.TXN_MRT_EHS_TANGO_MSR_ROLLUPS_GLBL
        where R_MSR_UNT = 'kg GHG'
        and MSR = 'Emission - Air Refrigerants'
        and RPRTNG_LVL = 'GEO'
        and EHS_FUNC_DESC is null
        and EHS_BU_DESC is null
        and FSCL_MNTH_NO is not null 
        and FSCL_QRTR is not null 
        and FSCL_YR is not null
        and BUILDING_ID is not null
        """).fetchall())
    leaks = pd.DataFrame(leaks)
    leaks.columns = [
        'MSR', 
        'SYSTM_SPCFIC_MSR', 
        'BUILDING_ID',
        'FSCL_MNTH_NO', 
        'FSCL_QRTR',
        'FSCL_YR',
        'R_MSR_VAL',
        'R_MSR_UNT']
    return leaks

#(and FSCL_MNTH_NO is not null  This one is to remove aggregated values)"""

def extract_fleet(cursor):
    """
    Extracts fleet data from Enablon data stored in the gms_us_mart.txn_cnspn_mtrcs_glbl table in EDB.

    Args:
        cursor: The database cursor object.
    
    Returns:
        dat (pd.DataFrame): A DataFrame containing the extracted data.
    """
    dat = (cursor.execute("""
        select 
            FldrPth,
            Building_ID,	
            Rprtg_Prd_Key_2,
            Rfrnc_Key,	
            Cd_Key_2,
            Unvrsl_Val,	
            Unvrsl_Unt
        from gms_us_mart.txn_cnspn_mtrcs_glbl
        where Cd_Key_2 like '%FLEET.Scp1.Cot.GHG.M%'
        and Cnspn_Typ == 'TET_GHG2'
        ;""").fetchall())
    dat = pd.DataFrame(dat)
    dat.columns = [
        'FOLDERPATH',
        'BUILDING_ID',
        'DATE',	
        'MSR',
        'SYSTM_SPCFIC_MSR',
        'R_MSR_VAL',
        'R_MSR_UNT']
    return dat



def extract_measures(cursor):
    """
    Extracts information on measures from the GMS_US_MART.TXN_MRT_EHS_TANGO_MSR_ROLLUPS_GLBL table in EDB.
    
    Args:
        cursor: The database cursor object.
    
    Returns:
        pandas.DataFrame: A DataFrame containing the extracted data.
    """
    msrs = (cursor.execute("""
        select distinct
            MSR,
            SYSTM_SPCFIC_MSR
        from  GMS_US_MART.TXN_MRT_EHS_TANGO_MSR_ROLLUPS_GLBL
        """).fetchall())
    msrs = pd.DataFrame(msrs)
    msrs.columns = ['MSR', 'EMSourceID']
    return msrs





###############################################################################
### Energy Conversion Factors
def extract_ecf(cursor):
    """
    Extracts energy conversion factors from the gms_us_mart.txn_cnspn_mtrcs_glbl table in EDB.
    Cd_Key_2 = 'Energy.EF.2.1.6'
    
    Args:
        cursor: The database cursor object.
    
    Returns:
        pandas.DataFrame: A DataFrame containing the extracted data.
    """
    ecf = (cursor.execute("""
        select 
            FldrPth,
            Rprtg_Prd_Key_2,
            Rfrnc_Key,
            Cd_Key_2,
            Unit,
            Nmbr_Val
        from gms_us_mart.txn_cnspn_mtrcs_glbl
        where Cd_Key_2 == 'Energy.EF.2.1.6'
        """).fetchall())
    ecf = pd.DataFrame(ecf)
    ecf.columns = [
        'FOLDERPATH',
        'Month',
        'Rfrnc_Key',
        'Cd_Key_2',
        'Unit',
        'Nmbr_Val']
    ecf = ecf.drop_duplicates()
    return ecf


# Rfrnc_Key, # Merged based on folder path

###############################################################################
### Steam Conversion Factors
def extract_scf(cursor):
    """
    Extracts energy conversion factors from the gms_us_mart.txn_cnspn_mtrcs_glbl table in EDB.
    Cd_Key_2 = Energy.EF.11.NRG or Energy.EF.11.MASS
    
    Args:
        cursor: The database cursor object.
    
    Returns:
        pandas.DataFrame: A DataFrame containing the extracted data.
    """
    scf = (cursor.execute("""
        select 
            FldrPth,
            Rprtg_Prd_Key_2,
            Rfrnc_Key,
            Cd_Key_2,
            Unit,
            Nmbr_Val
        from gms_us_mart.txn_cnspn_mtrcs_glbl
        where Cd_Key_2 == 'Energy.EF.11.NRG' or 
        Cd_Key_2 == 'Energy.EF.11.MASS'
        """).fetchall())
    scf = pd.DataFrame(scf)
    scf.columns = [
        'FOLDERPATH',
        'Month',
        'Rfrnc_Key',
        'Cd_Key_2',
        'Unit',
        'Nmbr_Val']
    return scf



###############################################################################
### SPOT
def extract_spot():
    """
    Extracts SPOT data from the SPOT database.
    
    Returns:
        pandas.DataFrame: DataFrame containing SPOT (Single Point of Truth) data - information GHG emission reduction projects.
    """
    print('extract spot')
    #spot = pd.read_sql_query("SELECT [SPOT ID],[Project Name],[Project Manager],[CAPS Project],[Environmental Portfolio],[Impact Realization Date],[Project Phase],[Project State],[Emissions Impact (tons CO2)]FROM [dbo].[Z_CAPS_Consolidated_Project_Listing_Carbon]", cnxn)
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER=usze2spotsql001rep.database.windows.net;DATABASE=TOPPMPROD;UID=SPOTEHS;PWD='+os.environ.get('SPOT_TOKEN'))
    spot = pd.read_sql_query("""
    SELECT [SPOT ID]
      ,a.[Project Name]
      ,a.[Project Manager]
      ,a.[CAPS Project]
      ,a.[Environmental Portfolio]
      ,a.[Impact Realization Date]
      ,a.[Project Phase]
      ,a.[Project State]
      ,a.[Emissions Impact (tons CO2)]
	  ,b.[ProblemID]
	  ,b.[ProblemUniqueID]
	  ,b.[EnergyImpact]
	  ,c.[ProjectID]
	  ,c.[EMSourceID]
	  ,c.[EMImpactTonsCO2Year]
	  ,c.[EMUnit]Ta
	  ,d.[EM Source Name]
      ,d.[TonsCO2]
      ,d.[Net Energy GJ]
    FROM [dbo].[Z_CAPS_Consolidated_Project_Listing_Carbon] as a INNER JOIN
        [dbo].[ProblemCapture] as b on b.ProblemID = a.[SPOT ID] INNER JOIN
        [dbo].[EMData] AS c ON c.ProjectID = b.ProblemUniqueID FULL JOIN
        [dbo].[FWM_CAPS-Emission Data (Projected Emission Abatement)] as d on d.[Project ID] = a.[SPOT ID] and d.[EM Source ID] = c.EMSourceID
    WHERE a.[Project Phase] IN ('Active', 'Completed')
        AND a.[Project State] IN ('Close', 'Track', 'Execute')
        AND b.EnergyImpact <> 0
        AND b.EnergyImpact IS NOT NULL
        AND c.EMImpactTonsCO2Year IS NOT NULL
        and d.TonsCO2 IS NOT NULL
    """, cnxn)
    cnxn.close()
    return spot

'''
SELECT TOP (1000) [SPOT ID]
      ,a.[Project Name]
      ,a.[Project Manager]
      ,a.[CAPS Project]
      ,a.[Environmental Portfolio]
      ,a.[Impact Realization Date]
      ,a.[Project Phase]
      ,a.[Project State]
      ,a.[Emissions Impact (tons CO2)]
	  ,b.[ProblemID]
	  ,b.[ProblemUniqueID]
	  ,c.[ProjectID]
	  ,c.[EMSourceID]
	  ,c.[EMImpactTonsCO2Year]
  FROM [dbo].[Z_CAPS_Consolidated_Project_Listing_Carbon] as a INNER JOIN
	[dbo].[ProblemCapture] as b on b.ProblemID = a.[SPOT ID] INNER JOIN
	[dbo].[EMData] AS c ON c.ProjectID = b.ProblemUniqueID
  WHERE (a.[Environmental Portfolio] IN ('Site-Los Angeles'))
    AND a.[CAPS Project] IN ('Yes')
	AND a.[Project Phase] IN ('Active', 'Completed')
	AND a.[Project State] IN ('Close', 'Track', 'Execute')
	AND b.EnergyImpact <> 0
	AND b.EnergyImpact IS NOT NULL
	AND c.EMImpactTonsCO2Year IS NOT NULL
'''


def extract_spot_empo():
    """
    Extracts SPOT data from the SPOT database. 
    
    Returns:
        pandas.DataFrame: DataFrame containing folderpath - required to match environmental portfolio owner with folderpath.
    """
    # query table [EMPortfolioOwner]
    print('extract empo')
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER=usze2spotsql001rep.database.windows.net;DATABASE=TOPPMPROD;UID=SPOTEHS;PWD='+os.environ.get('SPOT_TOKEN'))
    Spot_EMPortfolioOwner = pd.read_sql_query("""
        SELECT 
            [EMPortfolioOwnerUniqueID],
            [EMPortfolioOwnerGroup],
            [EMPortfolioOwnerID],
            [LocationReferenceID],
            [LocationReferenceName],
            [Comments]
        FROM [dbo].[EMPortfolioOwner]
    """, cnxn)
    cnxn.close()
    return Spot_EMPortfolioOwner



def extract_spot_sppo():
    """
    Extracts SPOT data from the SPOT database.
    
    Returns:
        pandas.DataFrame: DataFrame containing portoflio owner - required to match environmental portfolio owner with folderpath.
    """
    # query table [SPOTPortfolioOwner]
    print('extract sspo')
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER=usze2spotsql001rep.database.windows.net;DATABASE=TOPPMPROD;UID=SPOTEHS;PWD='+os.environ.get('SPOT_TOKEN'))
    Spot_SpotPortfolioOwner = pd.read_sql_query("""
        SELECT 
            [PortfolioOwnerID],
            [PortfolioID],
            [PortfolioOwner],
            [PortfolioGroup],
            [OpU],
            [Country],
            [Region],
            [PFID]  
        FROM [dbo].[SPOTPortfolioOwner]
    """, cnxn)
    cnxn.close()
    return Spot_SpotPortfolioOwner



###############################################################################
### SPOT
def extract_vppa():
    """
    Extracts VPPA data from the SPOT database.
    
    Returns:
        pandas.DataFrame:  DataFrame containing VPPA (Virtual Power Purchase Agreement) data.
    """
    print('extract vppa')
    cnxn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER=usze2spotsql001rep.database.windows.net;DATABASE=TOPPMPROD;UID=SPOTEHS;PWD='+os.environ.get('SPOT_TOKEN'))
    vppa = pd.read_sql_query("""
        SELECT
            a.ProblemID,
            a.ProblemUniqueID,
            a.ProjectDescription,
            a.IsCapsProject, 
            a.EmissionPortfolioID, 
            a.EmissionsImpactRealizationDate, 
            a.CalculatedEmissionsImpact, 
            a.EnergyImpact, 
            a.EnergyCostImpactPerYear, 
            a.NoCarbonImpact, 
            b.PortfolioOwner,
            b.IsEmissionPortfolio
        FROM            
            dbo.ProblemCapture AS a INNER JOIN
            dbo.SPOTPortfolioOwner AS b ON a.EmissionPortfolioID = b.PortfolioOwnerID
        WHERE (a.ProjectDescription = 'VPPA')
        """, cnxn)
    cnxn.close()
    return vppa


# COMMAND ----------

# MAGIC %sql
# MAGIC   SELECT [SPOT ID]
# MAGIC       ,a.[Project Name]
# MAGIC       ,a.[Project Manager]
# MAGIC       ,a.[CAPS Project]
# MAGIC       ,a.[Environmental Portfolio]
# MAGIC       ,a.[Impact Realization Date]
# MAGIC       ,a.[Project Phase]
# MAGIC       ,a.[Project State]
# MAGIC       ,a.[Emissions Impact (tons CO2)]
# MAGIC 	  ,b.[ProblemID]
# MAGIC 	  ,b.[ProblemUniqueID]
# MAGIC 	  ,b.[EnergyImpact]
# MAGIC 	  ,c.[ProjectID]
# MAGIC 	  ,c.[EMSourceID]
# MAGIC 	  ,c.[EMImpactTonsCO2Year]
# MAGIC 	  ,c.[EMUnit]
# MAGIC 	  ,d.[EM Source Name]
# MAGIC       ,d.[TonsCO2]
# MAGIC       ,d.[Net Energy GJ]
# MAGIC     FROM [dbo].[Z_CAPS_Consolidated_Project_Listing_Carbon] as a INNER JOIN
# MAGIC         [dbo].[ProblemCapture] as b on b.ProblemID = a.[SPOT ID] INNER JOIN
# MAGIC         [dbo].[EMData] AS c ON c.ProjectID = b.ProblemUniqueID FULL JOIN
# MAGIC         [dbo].[FWM_CAPS-Emission Data (Projected Emission Abatement)] as d on d.[Project ID] = a.[SPOT ID] and d.[EM Source ID] = c.EMSourceID
# MAGIC     WHERE a.[Project Phase] IN ('Active', 'Completed')
# MAGIC         AND a.[Project State] IN ('Close', 'Track', 'Execute')
# MAGIC         AND b.EnergyImpact <> 0
# MAGIC         AND b.EnergyImpact IS NOT NULL
# MAGIC         AND c.EMImpactTonsCO2Year IS NOT NULL
# MAGIC         and d.TonsCO2 IS NOT NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC         select distinct
# MAGIC             MSR,
# MAGIC             SYSTM_SPCFIC_MSR,
# MAGIC             BUILDING_ID,
# MAGIC             FSCL_MNTH_NO,
# MAGIC             FSCL_QRTR,
# MAGIC             FSCL_YR,
# MAGIC             R_MSR_VAL,
# MAGIC             R_MSR_UNT
# MAGIC         from GMS_US_MART.TXN_MRT_EHS_TANGO_MSR_ROLLUPS_GLBL
# MAGIC         where R_MSR_UNT = 'kg GHG'
# MAGIC         and MSR = 'Emission - Air Refrigerants'
# MAGIC         and RPRTNG_LVL = 'GEO'
# MAGIC         and EHS_FUNC_DESC is null
# MAGIC         and EHS_BU_DESC is null
# MAGIC         and FSCL_MNTH_NO is not null
# MAGIC         and FSCL_QRTR is not null
# MAGIC         and FSCL_YR is not null
# MAGIC         and BUILDING_ID is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC         select
# MAGIC             EHS_DATA_SHOW_FLG,
# MAGIC             COUNT (EHS_DATA_SHOW_FLG)
# MAGIC         from GMS_US_MART.REF_MRT_EHS_TANGO_FOOTPRINT
# MAGIC         group by EHS_DATA_SHOW_FLG
# MAGIC        

# COMMAND ----------

cnxn = pyodbc.connect('DRIVER={ODBC Driver 18 for SQL Server};SERVER=usze2spotsql001rep.database.windows.net;DATABASE=TOPPMPROD;UID=SPOTEHS;PWD='+os.environ.get('SPOT_TOKEN'))
    Spot_EMPortfolioOwner = pd.read_sql_query("""
        SELECT 
            [EMPortfolioOwnerUniqueID],
            [EMPortfolioOwnerGroup],
            [EMPortfolioOwnerID],
            [LocationReferenceID],
            [LocationReferenceName],
            [Comments]
        FROM [dbo].[EMPortfolioOwner]
    """, cnxn)
    cnxn.close()
    return Spot_EMPortfolioOwner

# COMMAND ----------

# MAGIC %md
# MAGIC # load.py

# COMMAND ----------

# MAGIC %pip show databricks
# MAGIC

# COMMAND ----------

############
### load ###
############

from extract import(
    extract_enablon,
    extract_natural_gas,
    extract_flag,
    extract_fleet,
    extract_leaks,
    extract_ecf,
    extract_measures,
    extract_scf,
    extract_spot,
    extract_spot_empo, 
    extract_spot_sppo,
    extract_steam,
    extract_tango,
    extract_vppa
)

from preprocess import(
    prepare_enablon,
    prepare_natural_gas,
    prepare_steam,
)

#from databricks import sql
import numpy as np
import os
import pandas as pd





###############################################################################
### load data from EDB
def get_edb():
    """
    Retrieves data from EDB (Enterprise Data Warehouse).
    
    Returns:
        tuple: A tuple containing the extracted data:
            - leaks (pandas.DataFrame): DataFrame containing refrigerant leakage data.
            - tango_fp (pandas.DataFrame): DataFrame containing folderpath and building id - the folderpath is required to match a portfolio owner with a building id.
            - flag (pandas.DataFrame): DataFrame containing data on flagged/divested sites.
            - msrs (pandas.DataFrame): DataFrame containing measure/indicator name and associated EMSourceID data - required to add the measure name to spot emission impact projects
            - ecf (pandas.DataFrame): DataFrame containing energy conversion factors.
    """
    #create connection and cursor
    print('open connection and establish cursor')
    connection = sql.connect(server_hostname = "onetakeda-usprd.cloud.databricks.com",
                        http_path = "sql/protocolv1/o/2186391591496286/1201-135729-4c9gjccf",
                        access_token = os.environ.get('ACCESS_TOKEN'))
    cursor = connection.cursor()
    cursor.execute("USE gms_us_mart;")
    print('start data extraction')
    print('leaks')
    leaks = extract_leaks(cursor)
    print('fleet')
    fleet = extract_fleet(cursor)
    print('tango')
    tango_fp = extract_tango(cursor)
    print('flag')
    flag = extract_flag(cursor)
    print('measures')
    msrs = extract_measures(cursor)
    print('energy conversion factors')
    ecf = extract_ecf(cursor)
    print('steam conversion factors')
    scf = extract_scf(cursor)
    print('end data extraction')
    # close cursor connection
    print('close connection')
    cursor.close()
    return leaks, fleet, tango_fp, flag, msrs, ecf, scf





###############################################################################
### load data from SPOT
def get_spot():
    """
    Retrieves data from SPOT (Single Point of Truth).
    
    Returns:
        tuple: A tuple containing the extracted data:
            - Spot_EMPortfolioOwner (pandas.DataFrame): DataFrame containing folderpath - required to match environmental portfolio owner with folderpath.
            - Spot_SpotPortfolioOwner (pandas.DataFrame): DataFrame containing portoflio owner - required to match environmental portfolio owner with folderpath.
            - spot (pandas.DataFrame): DataFrame containing SPOT (Single Point of Truth) data - information GHG emission reduction projects.
            - vppa (pandas.DataFrame): DataFrame containing VPPA (Virtual Power Purchase Agreement) data.
    """
    print('spot database')
    Spot_EMPortfolioOwner = extract_spot_empo() # contains folderpath
    #Spot_EMPortfolioOwner = pd.read_csv('./input_data/Spot_EMPortfolioOwner.csv') # contains folderpath
    Spot_SpotPortfolioOwner = extract_spot_sppo() # contains portfolio owner
    #Spot_SpotPortfolioOwner = pd.read_csv('./input_data/Spot_SpotPortfolioOwner.csv') # contains portfolio owner
    spot = extract_spot()
    #spot = pd.read_csv('./input_data/spot.csv')
    vppa = extract_vppa()
    # manual change of vppa impact realization data due to faulty data input in database
    vppa['EmissionsImpactRealizationDate'] = '2023-03-31'
    vppa['EmissionsImpactRealizationDate'] = pd.to_datetime(vppa['EmissionsImpactRealizationDate'])
    return Spot_EMPortfolioOwner, Spot_SpotPortfolioOwner, spot, vppa





###############################################################################
### load data from local files
def get_local_files():
    """
    Load data from local files.
    
    Returns:
        pd.DataFrame: The loaded data from the Energy_Conversion_Factors.csv file.
    """
    #vol = pd.read_csv('./data/ProductionVolumesRawData.csv', index_col=0, encoding='windows-1252')
    vol_past = pd.read_csv('./data/Volume_Past.csv') # historical future
    vol_future = pd.read_csv('./data/Volume_Future.csv') # next 3 years prediction of plasma (Improved predictions)
    # conversion factors
    cf = pd.read_csv('./data/Energy_Conversion_Factors.csv', encoding='windows-1252')
    # steam conversion factors
    #scf = pd.read_csv('./data/Steam_Conversion_Factors.csv')
    spot_lookup = pd.read_csv('./data/SPOT_LOOKUP_Table.csv')
    return cf, spot_lookup, vol_past, vol_future





###############################################################################
### extract enablon (energy usage) data: raw data used for ghg emission forecast model
# extract and transform (preprocess) measure-specific enablon data
def load_enablon(measure, tango_fp, spot_fp_po, flag, cf, ecf, scf):
    """
    Extracts and transforms (preprocesses) measure-specific Enablon data. This function is called
    iteratively for each measure/indicator. Natural Gas and Steam are special caes
    
    Args:
        measure (str): The specific measure for which Enablon data is extracted.
        tango_fp (pd.DataFrame): The tango data containing the folderpath for each building id.
        spot_fp_po (pd.DataFrame): The SPOT data used for adding the portfolio owner to Enablon data (folderpath and portfolio owner).
        flag (pd.DataFrame): The information on divested (flagged) sites - sites to be excluded.
        cf (pd.DataFrame): The conversion factors.
        ecf (pd.DataFrame): The electricity conversion factors.
        scf (pd.DataFrame): the steam conversion factors
    
    Returns:
        tuple: A tuple containing the transformed data and PortfolioOwner + FolderPath dataset.
            - df (data type): The transformed data.
            - po_fp (data type): The PortfolioOwner + FolderPath dataset.
    
    Raises:
        Exception: If any error occurs during the extraction or preprocessing.
    """
    print('start load_enablon')
    #create connection and cursor
    connection = sql.connect(server_hostname = "onetakeda-usprd.cloud.databricks.com",
                http_path = "sql/protocolv1/o/2186391591496286/1201-135729-4c9gjccf",
                access_token = os.environ.get('ACCESS_TOKEN'))
    cursor = connection.cursor()
    cursor.execute("USE gms_us_mart;")
    print('extract enablon indicator')
    # extract enablon data from EDB for the provided measure/indicator (dat_enablon)
    if measure == 'Natural Gas - Useage (Reported)':
        dat_enablon = extract_natural_gas(cursor)
    elif measure == 'Purchased Steam - Usage':
        dat_enablon = extract_steam(cursor)
    else:
        dat_enablon = extract_enablon(cursor, measure)
    # data preparation
    if measure == 'Natural Gas - Useage (Reported)':
        df = prepare_natural_gas(dat_enablon, flag, spot_fp_po) # folderpath already included in dataset
    elif measure == 'Purchased Steam - Usage':
        df = prepare_steam(dat_enablon, flag, spot_fp_po) # folderpath already included in dataset
    else:
        df = prepare_enablon(dat_enablon, flag, tango_fp, spot_fp_po)
    #close cursor
    cursor.close()
    print('end load_enablon')
    return df

# need to keep the Cntry level information

# remove blanks in scf
#df.loc[(df['SYSTM_SPCFIC_MSR']=='Energy.11a.mass') & (df['PortfolioOwner']=='Site-Linz')]['y'][:12].sum()
# only Linz, only reported biannually
# check if mass Kendall is only reported manually too or problem is the scf, scf are available monthly
# Kendall also reported binannually until 2020
#df.loc[df['SYSTM_SPCFIC_MSR']=='Energy.11a.mass']

#['FOLDERPATH', 'BUILDING_ID', 'Month', 'FSCL_DATE', 'MSR', 'SYSTM_SPCFIC_MSR', 'y', 'R_MSR_UNT', 'PortfolioOwner']

# electricity 
#['MSR', 'SYSTM_SPCFIC_MSR', 'BUILDING_ID', 'FSCL_MNTH_NO', 'FSCL_QRTR','FSCL_YR', 'y', 'R_MSR_UNT', 'FOLDERPATH', 'PortfolioOwner', 'Month','C_MNTH', 'C_YR'] 
# C_MNTH and C_YR can be dropped, also drop fscl stuff because later added again in date_conversion

# COMMAND ----------

# MAGIC %md
# MAGIC # Preprocess.py

# COMMAND ----------

##################
### preprocess ###
##################

from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import numpy as np
import os
import pandas as pd

from helper_functions import(
    date_conversion
)



###############################################################################
### preprocess spot: data on GHG emission reduction projects stored in SPOT
def prepare_spot(spot, spot_lookup):
    """
    Preprocess spot data on GHG emission reduction projects.
    
    Args:
        spot (pd.DataFrame): The ghg emission impact data of spot projects.
    Returns:
        pd.DataFrame: The preprocessed spot data with added columns and transformations.
    """
    # merge Enabon Source Name from spot_lookup with spot based on EM Source Name
    spot = pd.merge(spot, spot_lookup, on = 'EM Source Name', how = 'inner')
    # rename columns to align with other data sets
    spot = spot.rename(columns = {'EmissionsImpactRealizationDate': 'Impact Realization Date', 'Environmental Portfolio' : 'PortfolioOwner'})
    # data type
    spot['Impact Realization Date'] = pd.to_datetime(spot['Impact Realization Date'])
    # take the ceiling month of predicted impact aka spot project completion
    spot['Impact Month'] = spot['Impact Realization Date'] + pd.DateOffset(months=1) - pd.offsets.MonthBegin(1)
    # filter for spot projects in the future (past project are already reflected in actual data)
    now = pd.to_datetime(datetime.now().strftime("%Y-%m-%d"))
    # filter for spot impacts in the future
    spot = spot.loc[(spot['Impact Month'] > now)]
    spot = spot.sort_values('Impact Month')
    spot = spot[['SPOT ID', 'Impact Month', 'PortfolioOwner', 'EMImpactTonsCO2Year', 'EMUnit', 'EMSourceID', 'EM Source Name', 'Enablon Source Name']]\
            .drop_duplicates().sort_values('Impact Month')
    return spot




###############################################################################
### preprocess volume
def prepare_volume(vol_past, vol_future):
    # past volume
    vol_past = pd.melt(vol_past, id_vars=['Site', 'Product'], var_name='Date', value_name='Volume')
    vol_past['Volume'] = vol_past['Volume'].astype(float)
    vol_past['Volume_Sum'] = vol_past.groupby(['Site', 'Date'])['Volume'].transform('sum')
    vol_past = vol_past[['Site', 'Date', 'Volume_Sum']].drop_duplicates()
    vol_past['Date'] = pd.to_datetime(vol_past['Date'])
    vol_past['Date'] = vol_past['Date'].dt.strftime("%Y-%d-%m")
    # future volume
    vol_future = pd.melt(vol_future, id_vars=['Site', 'Product'], var_name='Date', value_name='Volume')
    vol_future['Volume'] = vol_future['Volume'].astype(float)
    vol_future['Volume_Sum'] = vol_future.groupby(['Site', 'Date'])['Volume'].transform('sum')
    vol_future = vol_future[['Site', 'Date', 'Volume_Sum']].drop_duplicates()
    vol_future['Date'] = pd.to_datetime(vol_future['Date'])
    vol_future['Date'] = vol_future['Date'].dt.strftime("%Y-%d-%m")
    # max past data is 01/08/2023 - use past data over future data
    vol_future = vol_future.loc[vol_future['Date']>='01/09/2023']
    vol = pd.concat([vol_past, vol_future]) 
    vol.columns = ['BUILDING_ID', 'Month', 'Volume']
    vol['Month'] = pd.to_datetime(vol['Month'])
    vol = vol.sort_values(['Month', 'BUILDING_ID'])
    return vol




###############################################################################
### preprocess refrigerant leakage data
def prepare_leaks(leaks, tango_fp, spot_fp_po):
    """
    Preprocess refrigerant leakage data. Refrigerant leakage data is handled separatly from
    other indicators. It is not modelled using a timeseries model. Instead, the value of the
    rolling average of the past three years is assumed for the future three years, per building. 
    Wrangle the leakage data in a way that the data frame can can be integrated into the CO2
    reporting feature in the dashboard.

    Args:
        leaks (pd.DataFrame): The refrigerant leakage data.
        tango_fp (pd.DataFrame): The database containing BUILDING_ID and FOLDERPATH information.
        spot_fp_po (pd.DataFrame): The spot data containing PortfolioOwner information.
    Returns:
        leaks (pd.DataFrame): The preprocessed refrigerant leakage data with added columns and transformations.
    """
    ### add PortfolioOwner column to refrigerant leaks data based on BUILDING_ID and FOLDERPATH
    # FOLDERPATH is added based on BUILDING_ID
    leaks = pd.merge(leaks, tango_fp, on='BUILDING_ID')
    # PortfolioOwner is added based on FOLDERPATH
    leaks = pd.merge(leaks, spot_fp_po, on='FOLDERPATH', how='left')
    # perform standard preprocessing tasks; derive calendar date from fiscal year and month
    leaks = leaks.assign(DATE = lambda x: pd.to_datetime(x['FSCL_YR'].astype(str) + '-' + x['FSCL_MNTH_NO'].astype(str))) \
            .assign(DATE = lambda x: x['DATE'] + pd.offsets.DateOffset(months=3)) \
            .astype({'R_MSR_VAL': 'int32'}) \
            .sort_values(['DATE', 'BUILDING_ID'])
    # calculate the average of the last three years worth of data per building id
    leaks.set_index('DATE', inplace=True)
    average_per_group = leaks.groupby('BUILDING_ID')['R_MSR_VAL'].apply(lambda x: x.loc[x.index >= x.index.max() - pd.DateOffset(years=3)].sum()/36)
    average_per_group = pd.DataFrame(average_per_group).reset_index()
    # add the average of the past three years as forecast to each building for the future three years
    leaks = leaks.reset_index()
    leaks['type'] = 'Actuals'
    max_date = leaks['DATE'].max()
    for i in average_per_group['BUILDING_ID'].unique():
        leak_forecast = average_per_group.loc[average_per_group['BUILDING_ID']==i, 'R_MSR_VAL'].item()
        new_row = leaks.loc[(leaks['BUILDING_ID']==i)].tail(1)
        new_row['DATE'] = max_date + pd.DateOffset(months=1)
        new_row['type'] = 'Predicted'
        leaks = leaks.append(new_row, ignore_index=True)
        for j in range(0,35):
            new_row = leaks.loc[(leaks['BUILDING_ID']==i)].tail(1)
            new_row['DATE'] = new_row['DATE'] + pd.DateOffset(months=1)
            new_row['R_MSR_VAL'] = leak_forecast
            leaks = leaks.append(new_row, ignore_index=True)
    # aggregate the leakages per portfolio owner
    leaks['y_ghg'] = leaks.groupby(['PortfolioOwner', 'DATE'])['R_MSR_VAL'].transform('sum')
    # data wrangling: select relevant columns and align column namimg conventions with other indicators
    leaks = leaks[['DATE', 'y_ghg', 'type', 'PortfolioOwner']].dropna().drop_duplicates()
    leaks['DATE'] = pd.to_datetime(leaks['DATE'])
    leaks = leaks.rename(columns={"DATE": "Impact Month"})
    leaks = date_conversion(leaks)
    # align dataframe with the shape of the df containing predictions of other indicators; 
    # leaks are added in the dashboard in scope 1
    leaks[[
        'y',
        'y_with_spot',
        'yhat_lower', 
        'yhat_upper', 
        'Emission Impact Sum', 
        'Emission Impact Accumulated', 
        'Energy Impact Sum', 
        'Energy Impact Accumulated' 
        ]] = None
    # energy conversion
    leaks['y_ghg'] = leaks['y_ghg']/1000 # convert kg GHG to tons GHG
    # spot impact not deducted/added, y_ghg_with_spot same as y_ghg
    leaks['y_ghg_with_spot'] = leaks['y_ghg']
    leaks = leaks[[
        'Impact Month', 
        'PortfolioOwner',
        'y', 
        'yhat_lower', 
        'yhat_upper', 
        'y_with_spot', 
        'y_ghg',
        'y_ghg_with_spot',
        'type', 
        'Emission Impact Sum', 
        'Emission Impact Accumulated',
        'Energy Impact Sum',
        'Energy Impact Accumulated',
        'C_MNTH', 
        'C_YEAR', 
        'C_QRTR',
        'F_MNTH', 
        'F_YEAR', 
        'F_QRTR']]
    return leaks



#leaks.loc[leaks['BUILDING_ID']=='AT-GRA-01'].tail(1).mean()


def prepare_fleet(fleet):
    """
    Preprocess fleet data. Fleet data is handled separatly from
    other indicators. It is not modelled using a timeseries model. Instead, the value of the
    rolling average of the past three years is assumed for the future three years, per building. 
    Wrangle the fleet data in a way that the data frame can can be integrated into the CO2
    reporting feature in the dashboard.

    Args:
        fleet (pd.DataFrame): The fleet data.
    Returns:
        fleet (pd.DataFrame): The preprocessed fleet data with added columns and transformations.
    """
    # column called type is added
    fleet['type'] = 'Actuals'
    # calculate the average of the last three years worth of data per building id
    fleet.set_index('DATE', inplace=True)
    average_per_group = fleet.groupby('BUILDING_ID')['R_MSR_VAL'].apply(lambda x: x.loc[x.index >= x.index.max() - pd.DateOffset(years=3)].sum()/36) # average over past 36 values/3 years
    average_per_group = pd.DataFrame(average_per_group).reset_index()
    # add the average of the past three years as forecast to each building for the future three years
    fleet = fleet.reset_index()
    max_date = fleet['DATE'].max()
    for i in average_per_group['BUILDING_ID'].unique():
        forecast = average_per_group.loc[average_per_group['BUILDING_ID']==i, 'R_MSR_VAL'].item() # select average
        new_row = fleet.loc[(fleet['BUILDING_ID']==i)].tail(1) # create a new row
        new_row['DATE'] = max_date + pd.DateOffset(months=1) # change month
        new_row['R_MSR_VAL'] = forecast # replace value by forecast
        new_row['type'] = 'Predicted'
        fleet = fleet.append(new_row, ignore_index=True)
        for j in range(0,35):
            new_row = fleet.loc[(fleet['BUILDING_ID']==i)].tail(1)
            new_row['DATE'] = new_row['DATE'] + pd.DateOffset(months=1)
            new_row['R_MSR_VAL'] = forecast
            fleet = fleet.append(new_row, ignore_index=True)
    # aggregate the fleet per building id and per date
    fleet['y_ghg'] = fleet.groupby(['BUILDING_ID', 'DATE'])['R_MSR_VAL'].transform('sum')
    # data wrangling
    # select relevant columns and align column naming conventions with other indicators
    # BUILDING ID required here, will be dropped later
    fleet = fleet[['DATE', 'y_ghg', 'type', 'BUILDING_ID']].dropna().drop_duplicates()
    # the PortfolioOwner column is needed for the dashboard; assume Fleet as PortfolioOwner
    fleet['PortfolioOwner'] = 'Fleet'
    # data type
    fleet['DATE'] = pd.to_datetime(fleet['DATE'])
    # rename columns to align naming converin with other indictors
    fleet = fleet.rename(columns={"DATE": "Impact Month"})
    # perform date conversion to align columns with other indicators from Enablon data
    fleet = date_conversion(fleet)
    # energy conversion
    fleet['y_ghg'] = fleet['y_ghg']/1000 # convert kg GHG to tons GHG
    fleet['y_ghg_with_spot'] = fleet['y_ghg'] # spot impact not deducted/added, y_ghg_with_spot same as y_ghg
    # align dataframe with the shape of the data containing predictions of other indicators; 
    # fleet data is added in the dashboard in scope 1
    fleet[[
        'y',
        'y_with_spot',
        'yhat_lower', 
        'yhat_upper', 
        'Emission Impact Sum', 
        'Emission Impact Accumulated', 
        'Energy Impact Sum', 
        'Energy Impact Accumulated' ]] = None
    fleet = fleet[[
        'Impact Month', 
        'PortfolioOwner',
        'y', 
        'yhat_lower', 
        'yhat_upper', 
        'y_with_spot', 
        'y_ghg',
        'y_ghg_with_spot',
        'type', 
        'Emission Impact Sum', 
        'Emission Impact Accumulated',
        'Energy Impact Sum',
        'Energy Impact Accumulated',
        'C_MNTH', 
        'C_YEAR', 
        'C_QRTR',
        'F_MNTH', 
        'F_YEAR', 
        'F_QRTR']]
    print('end fleet preparation')
    return fleet



###############################################################################
### preprocess energy conversion factors
def prepare_ecf(ecf):
    """
    Preprocess energy conversion factors.
    
    Args:
        ecf (pd.DataFrame): The energy conversion factors data frame.
    Returns:
        ecf (pd.DataFrame): The preprocessed energy conversion factors DataFrame.
    """
    ecf['Month'] = pd.to_datetime(ecf['Month'])
    ### special case izumisano: izumisano reported multiple conversion factors per month
    # filter for the highest conversion factor in izumisano per month
    df_filtered = ecf.loc[ecf['FOLDERPATH']=='Takeda > APAC > JPN > JPN.20 > 42105']
    # Group the DataFrame by month
    df_grouped = df_filtered.groupby('Month')
    # Define a function to perform the desired action for each group
    def filter_highest_value(group):
        group_sorted = group.sort_values('Nmbr_Val', ascending=False)
        return group_sorted.head(1)
    # Apply the function to each group
    ecf_izumisano = df_grouped.apply(filter_highest_value)
    # drop all reported izumisano values
    ecf = ecf.drop(ecf.loc[ecf['FOLDERPATH']=='Takeda > APAC > JPN > JPN.20 > 42105'].index)
    # replace with the highest reported value only
    ecf = ecf.append(ecf_izumisano)
    ### electricity conversion factors are required for future months to convert predictions into units of CO2
    # find the overall max date of electricity data and extend by three years (required for energy conversion)
    max_date = ecf['Month'].max() + pd.DateOffset(years=3) 
    ecf = ecf.reset_index(drop=True)
    # take the last/most recent available ecf value for each folderpath and extend it to the max_date (required for energy conversion)
    for i in ecf['FOLDERPATH'].unique():
        print(i)
        max_date_folderpath = ecf.loc[ecf['FOLDERPATH']==i]['Month'].max() # max date per folderpath
        last_ecf_val = ecf.loc[(ecf['FOLDERPATH']==i) & (ecf['Month']==max_date_folderpath), 'Nmbr_Val'].iloc[0] # last ecf value per folderpath
        while (max_date.year - max_date_folderpath.year) * 12 + (max_date.month - max_date_folderpath.month) > 0:
            new_row = ecf.loc[(ecf['FOLDERPATH']==i) & (ecf['Month']==max_date_folderpath) & (ecf['Nmbr_Val']==last_ecf_val)]
            new_row['Month'] = new_row['Month'] + pd.DateOffset(months=1)
            ecf = ecf.append(new_row, ignore_index=True)
            max_date_folderpath = ecf.loc[ecf['FOLDERPATH']==i, 'Month'].max()
    ecf = ecf.rename(columns={'Month': 'Impact Month'})
    return ecf




###############################################################################
### preprocess steam conversion factors
def prepare_scf(scf):
    """
    Preprocess steam conversion factors. Similar to prepare_ecf(), although with modifications.
    
    Args:
        scf (pd.DataFrame): The steam conversion factors data frame.
    Returns:
        scf (pd.DataFrame): The preprocessed steam conversion factors DataFrame.
    """
    # data wrangling:
    # data type
    scf['Month'] = pd.to_datetime(scf['Month'])
    # rename columns
    scf = scf.rename(columns={'Month':'Impact Month'})
    # drop NA
    scf.dropna(subset=['Nmbr_Val'], inplace=True)
    scf = scf[scf['Nmbr_Val'] != 0]
    # derive SYSTM_SPCFIC_MSR from Code_Key_2; required for merging with Enablon data later
    scf['SYSTM_SPCFIC_MSR'] = None
    scf.loc[scf['Cd_Key_2'] == 'Energy.EF.11.MASS', 'SYSTM_SPCFIC_MSR'] = 'Energy.11a.mass'
    scf.loc[scf['Cd_Key_2']=='Energy.EF.11.NRG', 'SYSTM_SPCFIC_MSR'] = 'Energy.11a.nrg'
    # steam conversion factors are required for future months to convert predictions into units of CO2
    # take the last/most recent available scf value for each folderpath and extend it by three years (required for energy conversion)
    scf = scf.sort_values('Impact Month')
    scf = scf.drop_duplicates()
    # max_date: three years ahead of current max date
    max_date = scf['Impact Month'].max() + pd.DateOffset(years=3)
    # perform the same action for both Cd_Key_2 ('Energy.EF.11.MASS', 'Energy.EF.11.NRG')
    # every folder path has its own steam conversion factor that needs to be extended by three years into the future
    for j in scf['Cd_Key_2'].unique():
        print(j)
        for i in scf['FOLDERPATH'].unique():
            try:
                print(i)
                max_date_folderpath = scf.loc[(scf['FOLDERPATH']==i) & (scf['Cd_Key_2']==j)]['Impact Month'].max() # max date per folderpath
                min_date_folderpath = scf.loc[(scf['FOLDERPATH']==i) & (scf['Cd_Key_2']==j)]['Impact Month'].min() # min date per folderpath
                last_scf_row = scf.loc[(scf['FOLDERPATH']==i) & (scf['Cd_Key_2']==j) & (scf['Impact Month']==max_date_folderpath)] # last scf row
                diff_months = (max_date.year - max_date_folderpath.year) * 12 + (max_date.month - max_date_folderpath.month) # difference in months
                scf = pd.concat([scf, pd.concat([last_scf_row]*diff_months)]) # replicate the last row by this number
                date_list = pd.date_range(min_date_folderpath, max_date,freq='MS') # create date range list
                scf.loc[(scf['FOLDERPATH']==i) & (scf['Cd_Key_2']==j), 'Impact Month'] = date_list
            except:
                continue
    return scf




###############################################################################
### run all preprocess functions
def preprocess(Spot_EMPortfolioOwner, Spot_SpotPortfolioOwner, spot, msrs, leaks, fleet, tango_fp, ecf, scf, vol_past, vol_future, spot_lookup):
    """
    Runs all preprocessing functions for the Enablon system.
    
    Args:
        - Spot_EMPortfolioOwner (pandas.DataFrame): DataFrame containing folderpath - required to match environmental portfolio owner with folderpath.
        - Spot_SpotPortfolioOwner (pandas.DataFrame): DataFrame containing portoflio owner - required to match environmental portfolio owner with folderpath.
        - spot (pandas.DataFrame): DataFrame containing SPOT (Single Point of Truth) data - information GHG emission reduction projects.
        - msrs (pandas.DataFrame): DataFrame containing measure/indicator name and associated EMSourceID data - required to add the measure name to spot emission impact projects
        - leaks (pandas.DataFrame): DataFrame containing refrigerant leakage data.
        - tango_fp (pandas.DataFrame): DataFrame containing folderpath and building id - the folderpath is required to match a portfolio owner with a building id.
        - ecf (pandas.DataFrame): DataFrame containing energy conversion factors.
    Returns:
        spot_fp_po (pd.DataFrame): The preprocessed spot data with additional information.
        spot (pd.DataFrame): The preprocessed spot data.
        leaks (pd.DataFrame): The preprocessed leaks data.
        ecf (pd.DataFrame): The preprocessed electricity conversion factors.
    """
    print('start data preprocessing')
    # rename column from EMPortfolioOwnerID to PortfolioOwnerID
    Spot_EMPortfolioOwner = Spot_EMPortfolioOwner.rename(columns={"EMPortfolioOwnerID": "PortfolioOwnerID"})
    # add FOLDERPATH (EMPortfolioOwnerGroup) on PortfolioOwnerID: # spot_fp_po contains the environmental portfolio owner with the associated folderpaths
    spot_fp_po = pd.merge(Spot_EMPortfolioOwner, Spot_SpotPortfolioOwner, on='PortfolioOwnerID')
    # rename EMPortfolioOwnerGroup to FOLDERPATH
    spot_fp_po = spot_fp_po.rename(columns={'EMPortfolioOwnerGroup': 'FOLDERPATH'})
    # the folderpath for Site-Vashi does not match with the tango_fp. Manually change folderpath
    spot_fp_po.loc[spot_fp_po['FOLDERPATH']=='Takeda > APAC > IND > Temp.Vash','FOLDERPATH'] = 'Takeda > APAC > IND > 43101'
    # make sure there are no duplicates for Vashi in case the folderpath is corrected in the raw data in the future
    spot_fp_po = spot_fp_po.drop_duplicates()
    spot_fp_po = spot_fp_po[['FOLDERPATH', 'PortfolioOwner']]
    print('prepare spot')
    spot = prepare_spot(spot, spot_lookup)
    print('prepare volume')
    vol = prepare_volume(vol_past, vol_future)
    print('prepare leaks')
    leaks = prepare_leaks(leaks, tango_fp, spot_fp_po)
    print('prepare fleet')
    fleet = prepare_fleet(fleet)
    print('prepare ecf')
    ecf = prepare_ecf(ecf)
    print('prepare scf')
    scf = prepare_scf(scf)
    print('end data preprocessing')
    return spot_fp_po, spot, leaks, fleet, ecf, scf, vol





###############################################################################
### preprocess enablon
def prepare_enablon(dat_enablon, flag, tango_fp, spot_fp_po):
    """
    Preprocesses usage data from the Enablon system. This function serves all indicators
    except natural gas and steam (these are two special cases and are preprocessed
    by separate functions).
    The building id column must be associated with a folderpath. The folderpath information
    is stored on the tango table. In a second step, the folderpath is matched with a portfolio
    owner. The information on the portfolio owner is obtained from the SPOT database.
    In addition, remove the divested sites and derive the calendar data from fiscal year information.
    
    Args:
        dat (pd.DataFrame): The input data containing the raw data.
        flag (pd.DataFrame): The flagged (divested) buildings data.
        spot_fp_po (pd.DataFrame): The data containing portfolio owner information.
    
    Returns:
        df (pd.DataFrame): The preprocessed data for the Enablon (energy usage) data.
    """
    print('preprocess enablon indicator')
    ### merge Enablon (energy usage data) and Tango (tango folderpath - tango_fp)
    # add FOLDERPATH based on BUILDING_ID
    tango_fp['BUILDING_ID'] = tango_fp['BUILDING_ID'].str.upper()
    dat_enablon = pd.merge(dat_enablon, tango_fp, on='BUILDING_ID', how='left') # small increase in data set TBC, multiple folderpaths per building?
    ### merge Enablon/Tango (energy usage data/folderpath) and SPOT (PortfolioOwner) creating dat_enablon_po
    # add PortfolioOwner based on FOLDERPATH
    dat_enablon_po = pd.merge(dat_enablon, spot_fp_po, on='FOLDERPATH', how='left')
    # filter and sort data - big loss of data (data often does not have a portfolio owner associated with folderpath)
    dat = dat_enablon_po[dat_enablon_po['PortfolioOwner'].notna()]
    # standard preprocess changes (mainly add the calendar date based on fiscal month and year)
    dat = dat.assign(DATE = lambda x: pd.to_datetime(x['FSCL_YR'].astype(str) + '-' + x['FSCL_MNTH_NO'].astype(str))) \
        .assign(DATE = lambda x: x['DATE'] + pd.offsets.DateOffset(months=3)) \
        .rename(columns={'R_MSR_VAL': 'y', 'DATE': 'Month'}) \
        .astype({'y': 'float'}) \
        .sort_values(['Month', 'BUILDING_ID'])
    # remove flagged (divested) buildings from data
    dat = dat[~dat['BUILDING_ID'].isin(flag['BUILDING_ID'])]
    # add production volumes based on Month and PortfolioOwner
    #df = df.merge(vol, on=['Month', 'PortfolioOwner'], how='left')
    # select relevant columns
    dat = dat[['FOLDERPATH', 'BUILDING_ID', 'Cntry', 'Month', 'MSR', 'SYSTM_SPCFIC_MSR', 'y', 'R_MSR_UNT', 'PortfolioOwner']]
    return dat





###############################################################################
### preprocess enablon: special case natural gas
def prepare_natural_gas(dat, flag, spot_fp_po):
    """
    Preprocesses natural gas data from the Enablon system. Natural gas usage data
    is stored on a building level. Buildings are associated with a folderpath. 
    Here, the folderpath is matched with a portfolio owner. See prepare_enablon
    for detailed information.
    In addition, remove the divested sites from the data and convert unit of 
    measurement of joules.
    
    Args:
    - dat_enablon (pd.DataFrame): The input data containing the natural gas data.
    - flag (pd.DataFrame): The flagged (divested) buildings data.
    - spot_fp_po (pd.DataFrame): The data containing portfolio owner information.
    
    Returns:
    - dat (pd.DataFrame): The preprocessed natural gas data for the Enablon system.
    """
    print('start prepare natural gas')
    dat['Month'] = pd.to_datetime(dat['Month'])
    # conversion from cubic meters to joules
    dat.loc[(dat['Cntry'].isin(['Canada', 'United States', 'Biolife US'])) & (dat['SYSTM_SPCFIC_MSR']=='Energy.2a.vol'), 'y'] = dat.loc[(dat['Cntry'].isin(['Canada', 'United States', 'Biolife US'])) & (dat['SYSTM_SPCFIC_MSR']=='Energy.2a.vol'), 'y'].astype(float) * 38116087.31
    dat.loc[~(dat['Cntry'].isin(['Canada', 'United States', 'Biolife US'])) & (dat['SYSTM_SPCFIC_MSR']=='Energy.2a.vol'), 'y'] = dat.loc[~(dat['Cntry'].isin(['Canada', 'United States', 'Biolife US'])) & (dat['SYSTM_SPCFIC_MSR']=='Energy.2a.vol'), 'y'].astype(float) * 34390174.57
    ### merge Enablon/Tango (energy usage data/folderpath) and SPOT (PortfolioOwner)
    # add PortfolioOwner based on FOLDERPATH
    dat = pd.merge(dat, spot_fp_po, on='FOLDERPATH', how='left')
    # remove flagged buildings from data
    dat = dat[~dat['BUILDING_ID'].isin(flag['BUILDING_ID'])]
    # filter and sort data (data often does not have a portfolio owner associated with folderpath)
    dat = dat.dropna(how='all', axis=1).query("PortfolioOwner.notna()", engine="python").sort_values(['Month', 'BUILDING_ID'])
    print('end prepare natural gas')
    return dat





###############################################################################
### preprocess enablon: special case natural gas
def prepare_steam(dat, flag, spot_fp_po):
    """
    Preprocesses steam data from the Enablon system. Steam usage data
    is stored on a building level. Buildings are associated with a folderpath. 
    Here, the folderpath is matched with a portfolio owner. See prepare_enablon
    for detailed information.
    In addition, remove the divested sites from the data and convert unit of 
    measurement to joules. 
    
    Args:
    - dat_enablon (pd.DataFrame): The input data containing the natural gas data.
    - flag (pd.DataFrame): The flagged (divested) buildings data.
    - spot_fp_po (pd.DataFrame): The data containing portfolio owner information.
    
    Returns:
    - dat (pd.DataFrame): The preprocessed steam for the Enablon system.
    """
    print('start prepare steam')
    dat['Month'] = pd.to_datetime(dat['Month'])
    # conversion from original unit to joules
    dat.loc[dat['SYSTM_SPCFIC_MSR']=='Energy.11a.mass','y'] = dat.loc[dat['SYSTM_SPCFIC_MSR']=='Energy.11a.mass','y'].astype(float)*2326006.377
    # some sites consistently drop zeros () remove these rows for cleaner data set
    dat = dat.drop(dat.loc[dat['y']==0].index)
    ### merge Enablon/Tango (energy usage data/folderpath) and SPOT (PortfolioOwner)
    # add PortfolioOwner based on FOLDERPATH
    dat = pd.merge(dat, spot_fp_po, on='FOLDERPATH', how='left')
    # remove flagged buildings from data
    dat = dat[~dat['BUILDING_ID'].isin(flag['BUILDING_ID'])]
    # filter and sort data (data often does not have a portfolio owner associated with folderpath)
    dat = dat.dropna(how='all', axis=1).query("PortfolioOwner.notna()", engine="python").sort_values(['Month', 'BUILDING_ID'])
    print('end prepare steam')
    return dat


# COMMAND ----------

extract_spot_empo()

# COMMAND ----------

# MAGIC %md
# MAGIC # model.py

# COMMAND ----------

#####################
### Model prophet ###
#####################

from dateutil.relativedelta import relativedelta
import numpy as np
import os
import pandas as pd
from prophet import Prophet
from prophet.utilities import regressor_coefficients
from prophet.diagnostics import cross_validation
from prophet.diagnostics import performance_metrics
from prophet.plot import plot_cross_validation_metric
from prophet.plot import plot_plotly, plot_components_plotly
from prophet.serialize import model_to_json, model_from_json
from scipy import stats
from scipy.signal import detrend
from sklearn import preprocessing
from sklearn.metrics import mean_squared_error




###############################################################################
### Prophet models and forecast
def get_prophet(df, vol):
    """
    Generates prophet models and forecasts for each unique PortfolioOwner in the given DataFrame.
    Models are fitted per indicator and per portfolio owner
    The input data contains all portfolio owners for one indicator
    For example: complete data set for natural gas indicator: 
    contains all portfolio owners -> loop through the portfolio owners
    
    Args:
        df (pd.DataFrame): The input data containing the time series data.
    
    Returns:
        prophet_models (dict): A dictionary containing Prophet models for each unique PortfolioOwner.
        prophet_fcst (dict): A dictionary containing Prophet forecasts for each unique PortfolioOwner (monthly).
    """
    prophet_models = dict()
    prophet_fcst = dict()
    prophet_reg_coeff = dict()
    plasma_buildings = ['AT-VIE-TEMP-01', 'IT-PIS-01', 'IT-RIE-01', 'BE-LES-TEMP-01', 'US-LOA-01', 'US-COV-02', 'US-ROL-01']
    #for i in plasma_buildings:
    for i in df['BUILDING_ID'].unique():
        print(i)
        df_prophet = df.copy()
        #df_prophet = df_prophet.reset_index()
        df_prophet = df_prophet.loc[df_prophet['BUILDING_ID']==i]
        if i in plasma_buildings:
            print('include volume')
            # add regressor
            df_prophet = df_prophet.merge(vol, on=['Month', 'BUILDING_ID'], how='left').sort_values('Month').dropna()
            df_prophet = df_prophet[['Month', 'y', 'Volume']]
            df_prophet.columns = ['ds', 'y', 'vol']
        else:
            df_prophet = df_prophet[['Month', 'y']]
            df_prophet.columns = ['ds', 'y']
            print(df_prophet)
        try:
            print('model')
            m = Prophet(seasonality_mode='multiplicative', mcmc_samples=300)
            if i in plasma_buildings:
                try:
                    # try to model with volume
                    print('add regressor')
                    m.add_regressor('vol', mode='additive')
                    print('model volume')
                    m.fit(df_prophet)
                    future = m.make_future_dataframe(periods=36, freq='MS') # need for future volume data
                    future['BUILDING_ID'] = i
                    print(future)
                    future = future.rename(columns={'ds':'Month'})
                    future = pd.merge(future, vol, on=['Month', 'BUILDING_ID'], how='left')
                    future = future[['Month', 'Volume']]
                    future.columns = ['ds', 'vol']
                    print(future)
                    print('predict')
                    fcst = m.predict(future)
                    reg_coef = regressor_coefficients(m)
                    print(reg_coef)
                    print('volume prediction completed')
                except:
                    print('except')
                    # try to model without volume
                    df_prophet = df.copy()
                    df_prophet = df_prophet.loc[df_prophet['BUILDING_ID']==i]
                    df_prophet = df_prophet[['Month', 'y']]
                    df_prophet.columns = ['ds', 'y']
                    m = Prophet(seasonality_mode='multiplicative', mcmc_samples=300)
                    m.fit(df_prophet)
                    future = m.make_future_dataframe(periods=36, freq='MS') # no need for future data
                    fcst = m.predict(future)
                    reg_coef = None
            else:
                m.fit(df_prophet)
                future = m.make_future_dataframe(periods=36, freq='MS') # no need for future data
                fcst = m.predict(future)
                reg_coef = None
        except:
            print('modeling failed')
            m = None
            fcst = None
            reg_coef = None
        prophet_models[i] = m
        prophet_fcst[i] = fcst
        prophet_reg_coeff[i] = reg_coef
    return prophet_models, prophet_fcst, prophet_reg_coeff


#fig = m.plot(fcst)
#fig = m.plot_components(fcst)


###############################################################################
### Cross-validation and performance metrics
def get_cv(df, prophet_models):
    """
    Perform cross-validation and compute performance metrics for a set of Prophet models.
    See the prophet documentation for more information on timeseries cross-validation.
    Manually define first and last cuffoff, set frequency to 36 months.
    
    Args:
        df (pandas.DataFrame): DataFrame containing the data for the models.
        prophet_models (dict): Dictionary of Prophet models.
    
    Returns:
        tuple: A tuple containing two dictionaries:
            - cv_dict (dict): Dictionary containing the cross-validation results for each model.
            - pm_dict (dict): Dictionary containing the performance metrics for each model.
    """
    cv_dict = dict()
    pm_dict = dict()
    for i in prophet_models.keys():
        print(i)
        df_prophet = df.copy()
        df_prophet = df_prophet.reset_index()
        df_prophet = df_prophet.loc[df_prophet['BUILDING_ID']==i, ['Month', 'y']]
        df_prophet.columns = ['ds', 'y']
        print(df_prophet)
        try:
            cutoffs = pd.date_range(start=min(df_prophet['ds'])+relativedelta(years=2), end=max(df_prophet['ds'])-relativedelta(years=1), freq='36MS')
            # perform cross-validation
            df_cv = cross_validation(model=prophet_models[i], horizon='360 days', cutoffs=cutoffs)
            # compute performance metrics
            df_p = performance_metrics(df_cv)
        except:
            df_cv = None
            df_p = None
        cv_dict[i] = df_cv
        pm_dict[i] = df_p
    return cv_dict, pm_dict





def get_prophet_residuals(prophet_fcst, df):
    """
    Calculate the residuals for Prophet forecasts.
    
    Args:
        prophet_fcst (dict): Dictionary containing the Prophet forecasts.
        df (pandas.DataFrame): DataFrame containing the actual values.
    
    Returns:
        dict: Dictionary containing the residuals for each forecast.
    """
    prophet_residuals = dict()
    for i in prophet_fcst.keys():
        try:
            yhat = prophet_fcst[i][['ds','yhat']]
            y = df.loc[df['BUILDING_ID']==i, ['y', 'Month']]
            y.columns = ['y', 'ds']
            y['ds'] = pd.to_datetime(y['ds'])
            resid = pd.merge(yhat, y, how='left', on='ds')
            resid['residual'] = resid['yhat'].astype(float)-resid['y'].astype(float)
            prophet_residuals[i] = resid
        except:
            prophet_residuals[i] = None
    return prophet_residuals




def MAPE(Y_actual,Y_Predicted):
    """
    Calculate the Mean Absolute Percentage Error (MAPE) between actual and predicted values.
    
    Args:
        Y_actual (array-like): The actual values.
        Y_Predicted (array-like): The predicted values.
    
    Returns:
        float: The MAPE value.
    """
    mape = np.mean(np.abs((Y_actual - Y_Predicted)/Y_actual))
    return mape





def get_metrics(pm_dict, prophet_residuals):
    """
    Calculate various metrics based on the performance metrics and residuals obtained from Prophet forecasts.
    What is the difference between mape_scores and mape_prophet?
    mape_scores contains the mape of the all models fit during the cross-validation step for each indicator
    mape_prophet contains the manually calculated residuals (differences between actual historic values and predictions made by the model)
    mape_prophet is calculated using the model which is fit during the model training step
    
    Args:
        pm_dict (dict): Dictionary containing the cross-validation performance metrics for each model.
        prophet_residuals (dict): Dictionary containing the residuals for each forecast.
    
    Returns:
        tuple: A tuple containing four pandas DataFrames:
            - mape_scores (pandas.DataFrame): DataFrame containing the MAPE scores for each model.
            - rmse_scores (pandas.DataFrame): DataFrame containing the RMSE scores for each model.
            - rmse_prophet (pandas.DataFrame): DataFrame containing the RMSE scores for each forecast.
            - mape_prophet (pandas.DataFrame): DataFrame containing the MAPE scores for each forecast.
    """
    mape_scores = dict()
    rmse_scores = dict()
    mape_prophet= dict()
    rmse_prophet = dict()
    for i in pm_dict.keys():
        try:
            mape_scores[i] = np.mean(pm_dict[i]['mape'])
            rmse_scores[i] = np.mean(pm_dict[i]['rmse'])
        except:
            mape_scores[i] = None
            rmse_scores[i] = None
    for i in prophet_residuals.keys():
        try:
            prophet_residuals[i] = prophet_residuals[i].dropna()
            y_actual = prophet_residuals[i]['y'].astype(float)
            y_predicted = prophet_residuals[i]['yhat'].astype(float)
            rmse_prophet[i] = mean_squared_error(y_actual, y_predicted, squared=False)
            mape_prophet[i] = MAPE(y_actual, y_predicted)
        except:
            rmse_prophet[i] = None
            mape_prophet[i] = None
    mape_scores = pd.DataFrame(mape_scores.items(), columns=['BUILDING_ID', 'MAPE'])
    rmse_scores = pd.DataFrame(rmse_scores.items(), columns=['BUILDING_ID', 'RMSE'])
    mape_prophet = pd.DataFrame(mape_prophet.items(), columns=['BUILDING_ID', 'MAPE'])
    rmse_prophet = pd.DataFrame(rmse_prophet.items(), columns=['BUILDING_ID', 'RMSE'])
    return mape_scores, rmse_scores, rmse_prophet, mape_prophet



# COMMAND ----------

# MAGIC %md
# MAGIC # postprocess.py

# COMMAND ----------

###################
### postprocess ###
###################

from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import numpy as np
import os
import pandas as pd


# convert prediction dictionary into dataframe
def get_prd_dataframe(prd_dic):
    """
    Convert prediction dictionary into a DataFrame.
    
    Args:
        prd_dic (dict): The prediction dictionary.
    
    Returns:
        DataFrame: The converted prediction DataFrame.
    """
    prd_dic = {k: v for k, v in prd_dic.items() if v is not None}
    try:
        for i in prd_dic.keys():
            print(i)
            prd_dic[i]['BUILDING_ID'] = i
            prd_dic[i] = prd_dic[i][['ds', 'yhat' ,'yhat_lower', 'yhat_upper', 'BUILDING_ID']]
        prd_df = pd.concat(prd_dic.values(), ignore_index=False)
        prd_df = prd_df.rename(columns={'ds':'Impact Month'})
        prd_df['Impact Month'] = pd.to_datetime(prd_df['Impact Month'])
        print('end get_prd_dataframe')
    except:
        prd_df = None
    return prd_df



# create a dataframe for the global forecast: replace predictions in the past in prd_df with the actuals from df
def get_app_dataframe(df, prd_df):
    """
    Create a DataFrame for the global forecast by replacing predictions in the past in prd_df with the actuals from df.
    
    Args:
        df (DataFrame): The DataFrame containing actuals.
        prd_df (DataFrame): The DataFrame containing predictions.
    
    Returns:
        DataFrame: The DataFrame for the global forecast.
    """
    try:
        # extract the actuals from df
        df_hist = df[['Month', 'y', 'BUILDING_ID']]
        df_hist = df_hist.set_index('Month')
        df_hist.index.name = 'Impact Month'
        df_hist['type'] = 'Actuals'
        df_hist[['yhat_lower', 'yhat_upper']] = np.nan
        df_hist = df_hist[['y', 'yhat_lower', 'yhat_upper', 'BUILDING_ID', 'type']]
        print('df_hist')
        print(df_hist)
        # prepare predictions to merge with actuals
        df_prd = prd_df.copy()
        df_prd['type'] = 'Predicted'
        df_prd = df_prd.rename(columns={'yhat':'y'})
        df_prd = df_prd[['Impact Month', 'y', 'yhat_lower', 'yhat_upper','BUILDING_ID', 'type']]
        df_prd = df_prd.set_index('Impact Month')
        print('df_prd')
        print(df_prd)
        # replace predictions with actuals: df_hist contains all actuals
        temp_df = pd.DataFrame()
        for i in df_prd['BUILDING_ID'].unique():
            temp_hist = df_hist.loc[df_hist['BUILDING_ID']==i]
            print('temp_hist')
            print(temp_hist)
            temp_prd = df_prd.loc[df_prd['BUILDING_ID']==i]
            print('temp_prd')
            print(temp_prd)
            temp_df = temp_df.append(temp_prd[~temp_prd.index.isin(temp_hist.index)])
        df_app = pd.concat([df_hist, temp_df])
        df_app = df_app.reset_index()
    except:
        df_app = None
    return df_app





def add_columns(dat, df):
    """
    Add columns  'SYSTM_SPCFIC_MSR', 'Cntry', 'PortfolioOwner' to the dataset containing
    historical data and predictions. These columns are needed to convert the values from 
    joule to ghg emissions. The information on the portfolioowner is needed to aggregate
    the predicitons for individual buildings on a portfolioowner level.

    Args:
    - dat (pd.DataFrame): data set containing historical values and predictions
    - df (pd.DataFrame): load_enablon output
    
    Returns:
    - dat (pd.DataFrame): 
    """
    try:
        dat_add = df.copy()
        dat_add = dat_add[['BUILDING_ID', 'SYSTM_SPCFIC_MSR', 'Month', 'Cntry', 'PortfolioOwner', 'FOLDERPATH']] # df[df['BUILDING_ID']=='IT-RIE-01'], rieti return two entries for natural gas, vol and nrg
        dat_add.columns = ['BUILDING_ID', 'SYSTM_SPCFIC_MSR', 'Impact Month', 'Cntry','PortfolioOwner', 'FOLDERPATH']
        #dat_add[dat_add['PortfolioOwner']=='Site-Rieti'].tail(1)
        #dat_add['SYSTM_SPCFIC_MSR'].unique()
        #dat_add[(dat_add['PortfolioOwner']=='Site-Rieti') & (dat_add['SYSTM_SPCFIC_MSR']=='Energy.2a.nrg')]
        # need to expand the dat_add till the end of the predictions (at least three years)
        for i in dat_add['BUILDING_ID'].unique():
            print(i)
            for j in range(0, 40):
                new_row = dat_add[dat_add['BUILDING_ID']==i].tail(1)
                new_row['Impact Month'] = new_row['Impact Month'] + pd.DateOffset(months=1)
                dat_add = dat_add.append(new_row, ignore_index=True)
        #df.index.name = None
        #df['Impact Month'] = pd.to_datetime(df['Impact Month']) # cannot use impact month for merging because df has not date in the future, need to extend df to have 
        dat['Impact Month'] = pd.to_datetime(dat['Impact Month']) # causes error because integers and dates mixed
        # add portfolio owner and and country
        # exception natural gas: country-dependent cf (joules back to kg)
        # the country information is available in the steam extract from enablon (df)
        dat = pd.merge(dat, dat_add, on=['BUILDING_ID', 'Impact Month'], how='left') # rieti creates duplicates, also global biolife us
    except:
        print('post-processing failed in add_columns()')
        dat = None
    return dat





###############################################################################
### postprocess enablon

# energy needs to be  converted at site level, before the portfolio level
# at portfolio level multiple sites with potentially different natural gas reporting units are aggregated
# requirements are measure, cf, ecf, po_fp - all loaded in etl function
# apply energy conversion in load_enablon function

def get_energy_conversion(df, measure, cf, ecf, scf):
    """
    Apply energy conversion to the given dataframe (from Joules to GHG). There are four different cases:
    Natural gas conversion factors depend on whether gas is reported in volumetric (m3) or energetic (J) units
    The volumetric natural gas conversion factor also depends on the geographic location (CAN/US or ROW)
    Steam conversion factors depend on whether steam is reported in mass (kg) or energy units (J)
    Electricity conversion factors depend on the energy grid (geographic location); defined by folderpaths
    Convert all other indicators simply with the pyhsical/fixed conversion factor
    
    Args:
        df (pd.DataFrame): The dataframe containing usage data.
        measure (str): The measure/indicator for which unit conversion to CO2 needs to be applied.
        cf (pd.DataFrame): The data containing conversion factors (flat file upload).
        ecf (pd.DataFrame): The data containing electricity conversion factors (EDB).
        scf (pd.DataFrame): The data containing steam conversion factors (EDB).
    Returns:
        df (pd.DataFrame): The dataframe with energy conversion applied.
    """
    try:
        # create a new columne in data frame for emissions (kg GHG)
        df['y_ghg'] = None 
        # convert Natural Gas - Usage: the conversion factors depend on unit of measurement (volumetric or energetic)
        if measure == 'Natural Gas - Useage (Reported)':
            # convert joules to gigajoules for Energy.2a.nrg
            df.loc[df['SYSTM_SPCFIC_MSR']=='Energy.2a.nrg', 'y_ghg'] = df.loc[df['SYSTM_SPCFIC_MSR']=='Energy.2a.nrg', 'y']/1000000000
            # convert cubic meters to liters
            #df.loc[df['SYSTM_SPCFIC_MSR']=='Energy.2a.vol', 'y'] = df.loc[df['SYSTM_SPCFIC_MSR']=='Energy.2a.vol', 'y']*100
            # convert joules to cubic meteres (requires country information) and convert cubic meters to liters (*1000)
            df.loc[(df['Cntry'].isin(['Canada', 'United States', 'Biolife US'])) & (df['SYSTM_SPCFIC_MSR']=='Energy.2a.vol'), 'y_ghg'] = df.loc[(df['Cntry'].isin(['Canada', 'United States', 'Biolife US'])) & (df['SYSTM_SPCFIC_MSR']=='Energy.2a.vol'), 'y'].astype(float) * 1/38116087.31 * 1000
            df.loc[~(df['Cntry'].isin(['Canada', 'United States', 'Biolife US'])) & (df['SYSTM_SPCFIC_MSR']=='Energy.2a.vol'), 'y_ghg'] = df.loc[~(df['Cntry'].isin(['Canada', 'United States', 'Biolife US'])) & (df['SYSTM_SPCFIC_MSR']=='Energy.2a.vol'), 'y'].astype(float) * 1/34390174.57 * 1000
            # extract coefficients from cf data frame
            coeff_vol = cf.loc[(cf['Indicators']==measure)&(cf['Cd_Key_2']=='Energy.2a.vol')][['CF Final']].values[0].item()
            coeff_nrg = cf.loc[(cf['Indicators']==measure)&(cf['Cd_Key_2']=='Energy.2a.nrg')][['CF Final']].values[0].item()
            df.loc[df['SYSTM_SPCFIC_MSR']=='Energy.2a.vol', 'y_ghg'] = df.loc[df['SYSTM_SPCFIC_MSR']=='Energy.2a.vol', 'y_ghg'].astype(float)*coeff_vol # cf: kg GHG/L
            df.loc[df['SYSTM_SPCFIC_MSR']=='Energy.2a.nrg', 'y_ghg'] = df.loc[df['SYSTM_SPCFIC_MSR']=='Energy.2a.nrg', 'y_ghg'].astype(float)*coeff_nrg # cf: kg GHG/kWh
            print('end convert natural gas')
        # convert Purchased Steam - Usage: the conversion factors depend on unit of measurement (mass or energy) and location; defined by folderpath
        elif measure == 'Purchased Steam - Usage':
            # find Number Value based on FOLDERPATH, SYSTM_SPCFIC_MSR (mass or energy) and Month (steam cf change over time)
            df = pd.merge(df, scf, on = ['FOLDERPATH', 'SYSTM_SPCFIC_MSR', 'Impact Month'], how='left').drop_duplicates()
            # convert the energy units to kWh for Energy.11a.nrg
            df.loc[df['SYSTM_SPCFIC_MSR']=='Energy.11a.nrg','y_ghg'] = df.loc[df['SYSTM_SPCFIC_MSR']=='Energy.11a.nrg','y'].astype(float)/1000000000*277.778 
            # convert the energy unit from GJ back to kg for Energy.11a.mass
            df.loc[df['SYSTM_SPCFIC_MSR']=='Energy.11a.mass','y_ghg'] = df.loc[df['SYSTM_SPCFIC_MSR']=='Energy.11a.mass','y'].astype(float)*1/2326006.377
            df['y_ghg'] = df['y_ghg'].astype(float) * df['Nmbr_Val'].astype(float) # cf: kg GHG/kWh (Energy.11a.nrg) or kg GHG/kg (Energy.11a.mass)
            print('end convert steam')
        # convert Purchased Electricity - Usage: the conversion factors depend on time and geographic location; defined by date and folderpath
        elif measure == 'Purchased Electricity - Usage':
            # apply electricity conversion factor based on folderpath and month (conversion factor changes with time)
            df = pd.merge(df, ecf, on = ['FOLDERPATH', 'Impact Month']).drop_duplicates() # not date available : go up to three years past current date
            # convert J to GJ (*10^9) and GJ into kWh (*277.778), then multiply the energy usage with the conversion factor
            df['y_ghg'] = df['y']/1000000000 * 277.778 * df['Nmbr_Val'].astype(float)# cf: kg GHG/kwH
            print('end convert electricity')
        # convert all indicators which are not equal to Purchased - Electricity - Usage and not equal to Natural Gas - Useage (Reported) and not equal to Purchased Steam - Usage
        else:
            # find coefficient based on measure in cf (measure is identical with indicator)
            coeff = cf.loc[cf['Indicators']==measure][['CF Final']].values[0].item()
            print(coeff)
            # multiply with conversion factor
            df['y_ghg'] = df['y'].astype(float)/1000000000*coeff
            print('end convert standard indicator')
        # convert kg into tons of CO2
        df['y_ghg'] = df['y_ghg']/1000
        # convert joules into gigajoules
        df['y'] = df['y']/1000000000
        df['yhat_lower'] = df['yhat_lower']/1000000000
        df['yhat_upper'] = df['yhat_upper']/1000000000
        print('end energy conversion')
    except:
        print('energy conversion failed in get_energy_conversion()')
        df = None
    return df


# find coefficient based on measure in cf (measure is identical with indicator)
#coeff = cf.loc[cf['Indicators']==measure][['CF Final']].values[0].item()
#print(coeff)
#df['y'] = df['y'].astype(float)*coeff




def aggregate_on_portfolio_level(df):
    """
    Aggregate the emission data on a portfolio owner level. A portfolio owner can contain multiple buildings. 
    Especially, the portoflio owner Global BioLife US contains several buildings in various locations.
    Aggregation is performed per portfolio owner and per month.
    
    Args:
    - df (pd.DataFrame): The input data containing the emission data.
    
    Returns:
    - df (pd.DataFrame): The aggregated emission data.
    """
    try:
        # drop building_id column (it would create unwanted duplicates)
        df = df[[
            'Impact Month', 
            'PortfolioOwner',
            'SYSTM_SPCFIC_MSR', 
            'y', 
            'yhat_lower', 
            'yhat_upper', 
            'y_ghg', 
            'type']].drop_duplicates() #only removes duplicates from historicals. yhat upper and lower cause duplicates in prediction
        # aggregate emissions per portfolio owner and month (one portfolio owner can have multiple buildings)
        df['y'] = df['y'].astype(float)
        df['yhat_lower'] = df['yhat_lower'].astype(float)
        df['yhat_upper'] = df['yhat_upper'].astype(float)
        df['y_ghg'] = df['y_ghg'].astype(float)
        df['y'] = df.groupby(['PortfolioOwner', 'Impact Month'])['y'].transform('sum')
        df['yhat_lower'] = df.groupby(['PortfolioOwner', 'Impact Month'])['yhat_lower'].transform('sum')
        df['yhat_upper'] = df.groupby(['PortfolioOwner', 'Impact Month'])['yhat_upper'].transform('sum')
        df['y_ghg'] = df.groupby(['PortfolioOwner', 'Impact Month'])['y_ghg'].transform('sum')
        df = df.drop_duplicates()
    except:
        print('aggregate on portfolio level failed')
        df = None
    return df





# add the spot project impacts to the timeseries forecasts
def add_spot(dat, spot, measure):
    """
    Add the spot project impacts to the timeseries forecasts. 
    # SPOT project emission impact sum must be provided in units of tons CO2
    # Forcastes (y) must be provided in units of tons CO2
    
    Args:
        dat (DataFrame): The DataFrame for the global forecast.
        spot (DataFrame): The DataFrame containing spot project impacts.
        measure (str): The measure used to filter in spot.
    
    Returns:
        DataFrame: The DataFrame with spot project impacts added to the timeseries forecasts.
    """
    if dat is not None:
        try:
            # multiple projects could be completed within the same month for the same portfolio owner and same measure (Enablon Source Name): sum up the emission impacts
            spot['Emission Impact Sum'] = spot.groupby(['PortfolioOwner', 'Impact Month', 'Enablon Source Name'])['EMImpactTonsCO2Year']\
                .transform('sum')/12 # divide yearly value by 12 to obtain monthly impact
            # perform the same action for the energy column, in addition convert kWh to GJ to align unit from spot data with unit from enablon data
            spot['Energy Impact Sum'] = spot.groupby(['PortfolioOwner', 'Impact Month', 'Enablon Source Name'])['EMUnit']\
                .transform('sum')/12 # divide yearly value by 12 to obtain monthly impact
            # select relevant columns and drop potential duplicates
            spot = spot[['Impact Month', 'PortfolioOwner', 'Emission Impact Sum', 'Energy Impact Sum', 'EMSourceID', 'Enablon Source Name']]\
                .drop_duplicates()
            spot = spot.loc[spot['Enablon Source Name']==measure] # use the added Enablon Source Name instead of EM Source Name
            # add the SPOT data: based on impact realization date and PortfolioOwner
            dat_spot = pd.merge(dat, spot, how='left', on=['Impact Month', 'PortfolioOwner'])
            print('test three')
            # add spot emission impacts
            dat_spot['Emission Impact Sum'] = dat_spot['Emission Impact Sum'].fillna(0)
            dat_spot['Emission Impact Accumulated'] = dat_spot.groupby('PortfolioOwner')['Emission Impact Sum'].cumsum()
            # aggregate every month (should happen after predictions are merged with actuals)
            dat_spot['y_ghg_with_spot'] = dat_spot['y_ghg'] + dat_spot['Emission Impact Accumulated']
            print('test four')
            # add spot energy impacts
            dat_spot['Energy Impact Sum'] = dat_spot['Energy Impact Sum'].fillna(0)
            dat_spot['Energy Impact Accumulated'] = dat_spot.groupby('PortfolioOwner')['Energy Impact Sum'].cumsum()
            # aggregate every month (should happen after predictions are merged with actuals)
            dat_spot['y_with_spot'] = dat_spot['y'] + dat_spot['Energy Impact Accumulated']
            print('end add spot')
        except:
            dat_spot = dat
            dat_spot['Emission Impact Sum'] = 0
            dat_spot['Emission Impact Accumulated'] = 0
            dat_spot['y_with_spot'] = dat_spot['y']
            dat_spot['y_ghg_with_spot'] = dat_spot['y_ghg']
    else:
        print('spot not included')
        dat_spot = None
    return dat_spot





# add the vppa project impacts to the timeseries forecasts
def add_vppa(dat, vppa, usage_coeff):
    """
    Add the VPPA project impacts to the timeseries forecasts. The VPPA impacts are provided 
    as emissions (tons of GHG) or energy (kWh). Goal of this function is to find the overage.
    Compare the total use of energy to the vppa value. In case, there is an overhead, it is
    divided evenly into 12 months to replace the original predictions.
    
    Args:
        dat (DataFrame): the data containing historical values and predicitons
        vppa (DataFrame): The data containing VPPA project impacts
        usage_coeff (float): The usage coefficient
    
    Returns:
        DataFrame: The DataFrame with VPPA project impacts added to the timeseries forecasts
    """
    try:
        df_vppa = vppa.copy()
        # find the overage for each site
        overhead = []
        for i in range(0,vppa.shape[0]):
            portfolio_owner = vppa['PortfolioOwner'][i]
            # find starting date of vppa
            vppa_start = vppa['EmissionsImpactRealizationDate'][i]
            vppa_end = vppa_start + relativedelta(months=12)
            # find the total energy usage of the portfolio owner for the year starting from the vppa start year
            e_total = dat.loc[dat['PortfolioOwner']==portfolio_owner]
            e_total = e_total.loc[(e_total['Impact Month']>vppa_start) & (e_total['Impact Month']<=vppa_end)]
            e_total = e_total['y_ghg_with_spot'].sum()
            vppa_total = df_vppa['CalculatedEmissionsImpact'][i] * usage_coeff
            # energy difference between usage (actual & predict) and vppa total
            e_diff = (e_total + vppa_total)
            # split up per month
            overhead_monthly = e_diff/12
            overhead.append(overhead_monthly)
            # replace the prediction from beginning of vppa contract till the end of the contract
            dat.loc[(dat['Impact Month']>vppa_start) & (dat['Impact Month']<=vppa_end) & (dat['PortfolioOwner']==portfolio_owner), 'y_ghg_with_spot'] = overhead_monthly
            print('vppa included')
    except:
        print('vppa not included')
    return dat





# convert the negative predictions to zero
def negative_to_zero(dat):
    """
    Convert the negative predictions to zero.
    
    Args:
        dat (DataFrame): The DataFrame containing the predictions.
    
    Returns:
        DataFrame: The DataFrame with negative predictions converted to zero.
    """
    try:
        # convert all negative predictions to zero
        dat.loc[dat['y']<0, 'y'] = 0
        dat.loc[dat['y_with_spot']<0, 'y_with_spot'] = 0
        dat.loc[dat['y_ghg']<0, 'y_ghg'] = 0
        dat.loc[dat['y_ghg_with_spot']<0, 'y_ghg_with_spot'] = 0
    except:
        print('failed negative_to_zero()')
    return dat




def select_columns(dat):
    columns = [
        'Impact Month', 
        'PortfolioOwner',
        'y', 
        'yhat_lower', 
        'yhat_upper', 
        'y_with_spot', 
        'y_ghg',
        'y_ghg_with_spot',
        'type', 
        'Emission Impact Sum', 
        'Emission Impact Accumulated',
        'Energy Impact Sum',
        'Energy Impact Accumulated',
        'C_MNTH', 
        'C_YEAR',
        'C_QRTR',
        'F_MNTH', 
        'F_YEAR', 
        'F_QRTR']
    if dat is not None:
        dat = dat[columns]
    else:
        dat = pd.DataFrame(columns=columns)
    return dat

# COMMAND ----------

# MAGIC %md
# MAGIC # Store.py

# COMMAND ----------

#############
### store ###
#############

import numpy as np
import os
import pandas as pd
import pickle
import redis


def store_data(leaks, fleet, spot, vppa, flag, vol):    
    leaks_json = pickle.dumps(leaks)
    fleet_json = pickle.dumps(fleet)
    spot_json = pickle.dumps(spot)
    vppa_json = pickle.dumps(vppa)
    flag_json = pickle.dumps(flag)
    vol_json = pickle.dumps(vol)
    # establish redis
    redis_client = redis.StrictRedis.from_url(os.environ.get("REDIS_URL", "redis://127.0.0.1:6379"))
    # to redis
    redis_client.set('leaks', leaks_json)
    redis_client.set('fleet', fleet_json)
    redis_client.set('spot', spot_json)
    redis_client.set('vppa', vppa_json)
    redis_client.set('flag', flag_json)
    redis_client.set('vol', vol_json)
    # to csv
    #leaks.to_csv('./input_data/leaks.csv')
    #spot.to_csv('./input_data/spot.csv')
    #vppa.to_csv('./input_data/vppa.csv')
    #flag.to_csv('./input_data/flag.csv')
    return

# COMMAND ----------

# MAGIC %md
# MAGIC # run_pipeline.py

# COMMAND ----------

# MAGIC %pip install databricks-sql-connector
# MAGIC

# COMMAND ----------

from store import(
    store_data
)

# COMMAND ----------

import numpy as np
import os
import pandas as pd
import pickle
import redis


# import functions
from load import(
    get_local_files,
    get_spot
)

from preprocess import(
    preprocess,
    prepare_volume
)

from model import(
    get_prophet,
    get_cv,
    get_metrics,
    get_prophet_residuals
)

from postprocess import(
    aggregate_on_portfolio_level,
    get_energy_conversion,
    get_app_dataframe,
    get_prd_dataframe,
    add_columns,
    add_spot,
    add_vppa,
    negative_to_zero,
    select_columns
)

from store import(
    store_data
)

from helper_functions import(
    date_conversion,
    get_fiscal_month,
    get_quarter
)


# COMMAND ----------

# import packages
import numpy as np
import os
import pandas as pd
import pickle
import redis


# import functions
from load import(
    get_edb,
    load_enablon,
    get_local_files,
    get_spot
)

from preprocess import(
    preprocess,
    prepare_volume
)

from model import(
    get_prophet,
    get_cv,
    get_metrics,
    get_prophet_residuals
)

from postprocess import(
    aggregate_on_portfolio_level,
    get_energy_conversion,
    get_app_dataframe,
    get_prd_dataframe,
    add_columns,
    add_spot,
    add_vppa,
    negative_to_zero,
    select_columns
)

from store import(
    store_data
)

from helper_functions import(
    date_conversion,
    get_fiscal_month,
    get_quarter
)


###############################################################################
### extract transform load
def run_etl():
    """
    Runs the Extract Transform Load (ETL) process.
    
    Returns:
        tuple: A tuple containing the transformed data.
            - spot_fp_po (data frame): The transformed data related to folderpaths and portfolio owners in spot.
            - spot (data frame): The transformed data related to ghg emissions impact projects on spot.
            - leaks (data frame): The transformed data related to refrigerant leaks.
            - tango_fp (data frame): The transformed data related to folderpaths.
            - flag (data frame): The transformed data related to flagged/divested sites.
            - msrs (data frame): The transformed data related to measure codes.
            - vppa (data frame): The transformed data related to vppa.
            - cf (data frame): The transformed data related to conversion factors.
            - ecf (data frame): The transformed data related to electricity conversion factors.
    """
    # run extract functions (saved in extract.py and load.py)
    Spot_EMPortfolioOwner, Spot_SpotPortfolioOwner, spot, vppa = get_spot()
    leaks, fleet, tango_fp, flag, msrs, ecf, scf = get_edb()
    cf, spot_lookup, vol_past, vol_future = get_local_files()
    # run transform functions (saved in preprocess.py)
    spot_fp_po, spot, leaks, fleet, ecf, scf, vol = preprocess(Spot_EMPortfolioOwner, Spot_SpotPortfolioOwner, spot, msrs, leaks, fleet, tango_fp, ecf, scf, vol_past, vol_future, spot_lookup)
    return spot_fp_po, spot, leaks, fleet, tango_fp, flag, vppa, msrs, cf, ecf, scf, vol



###############################################################################
### run prediction
def run_prediction(scope, measure, spot_fp_po, spot, tango_fp, flag, vppa, cf, ecf, scf, vol):
    """
    Run the prediction per measure
    """
    
    print('start run prediction')
    df = load_enablon(measure, tango_fp, spot_fp_po, flag, cf, ecf, scf) # df.loc[(df['PortfolioOwner']=='Global-BioLife US') & (df['BUILDING_ID']=='US-AME-01')]
    po_bu = df[['BUILDING_ID','PortfolioOwner']].drop_duplicates()
    print('modeling')
    prophet_models, prophet_fcst, prophet_reg_coeff = get_prophet(df, vol)
    print('cross-validation')
    cv_dict, pm_dict = get_cv(df, prophet_models)
    print('post-processing')
    prophet_residuals = get_prophet_residuals(prophet_fcst, df)
    print('get metrics')
    mape_scores, rmse_scores, rmse_prophet, mape_prophet = get_metrics(pm_dict, prophet_residuals)
    prd_dic = prophet_fcst.copy()
    print(prd_dic)
    print('get prd df')
    prd_df = get_prd_dataframe(prd_dic)
    print(prd_df)
    print('get app df')
    df_app = get_app_dataframe(df, prd_df)
    # add portfolio owner, add Country too (required for conversion of natural gas)
    # add systm_spcf_msr for energy conversion, folderpath for energy and steam
    df_app = add_columns(df_app, df)
    df_app = get_energy_conversion(df_app, measure, cf, ecf, scf) # need building
    df_app = aggregate_on_portfolio_level(df_app)
    print('add spot')
    df_global = add_spot(df_app, spot, measure) 
    print('date conversion')
    df_global = date_conversion(df_global)
    print('add vppa')
    # take out vppa (team decided to ignore VPPA contracts)
    #if measure == 'Purchased Electricity - Usage':
    #    usage_coeff = 0.8
    #    df_global = add_vppa(df_global, vppa, usage_coeff)
    print('negative to zero')
    df_global = negative_to_zero(df_global)
    print('select columns')
    df_global = select_columns(df_global)
    
    # convert the list of dictionaries to a JSON serializable format
    print('pickle dumps')
    prophet_models_json = pickle.dumps(prophet_models)
    prophet_fcst_json = pickle.dumps(prophet_fcst)
    prophet_residuals_json = pickle.dumps(prophet_residuals)
    prophet_reg_coeff_json = pickle.dumps(prophet_reg_coeff)
    cv_dict_json = pickle.dumps(cv_dict)
    df_global_json = pickle.dumps(df_global)
    mape_scores_json = pickle.dumps(mape_scores)
    rmse_scores_json = pickle.dumps(rmse_scores)
    mape_prophet_json = pickle.dumps(mape_prophet)
    rmse_prophet_json = pickle.dumps(rmse_prophet)
    po_bu_json = pickle.dumps(po_bu)
    
    # set up redis client
    print('establish redis')
    redis_client = redis.Redis(host='localhost', port=6379, db=0)
    redis_client =   redis.StrictRedis.from_url(os.environ.get("REDIS_URL", "redis://127.0.0.1:6379"))
    
    # save model to redis
    print('redis set')
    redis_client.set('prophet_models' + measure, prophet_models_json)
    redis_client.set('prophet_fcst' + measure, prophet_fcst_json)
    redis_client.set('prophet_residuals' + measure, prophet_residuals_json)
    redis_client.set('prophet_reg_coeff' + measure, prophet_reg_coeff_json)
    redis_client.set('cv_dict' + measure, cv_dict_json)
    redis_client.set('df_global' + measure, df_global_json) 
    redis_client.set('mape_scores' + measure, mape_scores_json)
    redis_client.set('rmse_scores' + measure, rmse_scores_json)
    redis_client.set('rmse_prophet' + measure, rmse_prophet_json)
    redis_client.set('mape_prophet' + measure, mape_prophet_json)
    redis_client.set('po_bu' + measure, po_bu_json)
    print('end run prediction')
    return

# COMMAND ----------

# MAGIC %md
# MAGIC ## helper_functions.py

# COMMAND ----------

########################
### helper functions ###
########################

from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
import numpy as np
import os
import pandas as pd
import pickle


# get the yearly quarter
def get_quarter(x):
    if x in [1,2,3]:
        return 1
    elif x in [4,5,6]:
        return 2
    elif x in [7,8,9]:
        return 3
    elif x in [10,11,12]:
        return 4


def get_fiscal_month(x):
    if x in [1,2,3]:
        return x + 9
    else:
        return x - 3


def date_conversion(dat):
    try:
        dat['C_MNTH'] = dat['Impact Month'].dt.month
        dat['C_YEAR'] = dat['Impact Month'].dt.year
        dat['C_QRTR'] = dat['Impact Month'].dt.month.apply(get_quarter)
        dat['F_MNTH'] = dat['Impact Month'].dt.month.apply(get_fiscal_month)
        dat['F_YEAR'] = np.where((dat['Impact Month'].dt.month.isin([1,2,3])), dat['Impact Month'].dt.year - 1, dat['Impact Month'].dt.year)
        dat['F_QRTR'] = dat['F_MNTH'].apply(get_quarter)
    except:
        print('failed date_conversion()')
    return dat



###############################################################################
### save objects
###############################################################################
def save_objects(prophet_models, prophet_fcst, cv_dict, pm_dict):
    prophet_models_filtered = {k: v for k, v in prophet_models.items() if v is not None}
    for i in prophet_models_filtered.keys():
        with open('serialized_model'+i+'.json', 'w') as fout:
            fout.write(model_to_json(prophet_models_filtered[i]))
    with open('prophet_fcst.pkl', 'wb') as f:
        pickle.dump(prophet_fcst, f)
    with open('cross_validation.pkl', 'wb') as f:
        pickle.dump(cv_dict, f)
    with open('performance_metric.pkl', 'wb') as f:
        pickle.dump(pm_dict, f)
    return



###############################################################################
### load objects
###############################################################################
def load_objects(df):
    prophet_models = dict()
    for i in df['PortfolioOwner'].unique(): # ['Site-Hikari', 'Global-BioLife US']
        with open('serialized_model'+i+'.json', 'r') as fin:
            prophet_models[i] = model_from_json(fin.read())  # Load model
    with open('prophet_fcst.pkl', 'rb') as f:
        prophet_fcst = pickle.load(f)
    with open('cross_validation.pkl', 'rb') as f:
        cv_dict = pickle.load(f)
    with open('performance_metric.pkl', 'rb') as f:
        pm_dict = pickle.load(f)
    return prophet_models, prophet_fcst, cv_dict, pm_dict



# COMMAND ----------

import numpy as np
import os
import pandas as pd



connection = sql.connect(server_hostname = "onetakeda-usprd.cloud.databricks.com",
                        http_path = "sql/protocolv1/o/2186391591496286/1201-135729-4c9gjccf",
                        access_token = os.environ.get('ACCESS_TOKEN'))

# COMMAND ----------

# MAGIC %md
# MAGIC # Code

# COMMAND ----------

    Spot_EMPortfolioOwner, Spot_SpotPortfolioOwner, spot, vppa = get_spot()
    leaks, fleet, tango_fp, flag, msrs, ecf, scf = get_edb()
    cf, spot_lookup, vol_past, vol_future = get_local_files()
    # run transform functions (saved in preprocess.py)
    spot_fp_po, spot, leaks, fleet, ecf, scf, vol = preprocess(Spot_EMPortfolioOwner, Spot_SpotPortfolioOwner, spot, msrs, leaks, fleet, tango_fp, ecf, scf, vol_past, vol_future, spot_lookup)
    return spot_fp_po, spot, leaks, fleet, tango_fp, flag, vppa, msrs, cf, ecf, scf, vol


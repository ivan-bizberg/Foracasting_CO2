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
	  ,c.[EMUnit]
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

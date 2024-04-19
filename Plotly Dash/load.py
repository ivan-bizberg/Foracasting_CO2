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

from databricks import sql
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
    vol_past = pd.read_csv('./data/Volume_Past.csv')
    vol_future = pd.read_csv('./data/Volume_Future.csv')
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
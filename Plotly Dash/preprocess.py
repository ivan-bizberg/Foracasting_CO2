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

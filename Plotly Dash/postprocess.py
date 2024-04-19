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
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
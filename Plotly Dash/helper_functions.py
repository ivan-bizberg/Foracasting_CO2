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


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


# Import packages
from dash import Dash, html, dash_table, dcc, callback, Output, Input, State, CeleryManager
import dash_design_kit as ddk
import dash
from dash.exceptions import PreventUpdate
from databricks import sql
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import plotly
import plotly.express as px # build interactive graphs
from plotly.tools import mpl_to_plotly
import plotly.graph_objs as go
import plotly.graph_objects as go
import plotly.figure_factory as ff
import matplotlib.pylab as plt
from matplotlib.pylab import rcParams
import matplotlib.dates as mdates
from scipy.signal import detrend
from sklearn import preprocessing
from scipy import stats
from datetime import date 
from dateutil.relativedelta import relativedelta
from sklearn.metrics import mean_squared_error
from prophet import Prophet
from prophet.utilities import regressor_coefficients
from prophet.diagnostics import cross_validation
from prophet.diagnostics import performance_metrics
from prophet.plot import plot_cross_validation_metric
from prophet.plot import plot_plotly, plot_components_plotly
from prophet.serialize import model_to_json, model_from_json
import pickle 
import pyodbc
import json
from celery import Celery
from urllib.parse import urlparse
import redis
from definition_bu import business_unit
from definition_bu import MSR_ONE
from definition_bu import MSR_TWO
# load the business function definitions
MSR_ONE.extend(MSR_TWO)


###############################################################################
### Dash Application
###############################################################################

app = Dash(__name__)


# expose server variable for Procfile
server = app.server

# Defining the Redis instance and using different DB numbers for Workspaces than for the app connected to the workspace
app.config.suppress_callback_exceptions = True
redis_client = redis.StrictRedis.from_url(os.environ.get("REDIS_URL", "redis://127.0.0.1:6379"))
#redis_client = redis.Redis(host='localhost', port=6379, db=0)


### controls
controls = [
    html.Div(children=[

    html.Label('Scope'),
    dcc.Checklist(id='input_scope',
                options=['Scope 1', 'Scope 2'], 
                value=['Scope 1', 'Scope 2']),

    html.Br(),
    html.Label('Reporting Unit'),
    dcc.RadioItems(id='input_unit',
                   options=['Energy (GJ)', 'CO2 (t)'],
                   value='Energy (GJ)'),

    html.Br(),
    html.Label('Calendar'),
    dcc.RadioItems(id='input_calendar',
                   options=['Fiscal Year', 'Calendar Year'], 
                   value='Fiscal Year'),
    
    html.Br(),
    html.Label('Measures'),
    html.Button('Select/Deselect All', id='select-all-button', n_clicks=0),
    html.Div(id='input_measure-container'), #options are dependent on the selection of Scope 1 or 2 and on input indicator
    
    html.Br(),
    html.Label('Takeda BU, Function, Partner'),
    html.Button('Select/Deselect All', id='select-all-bu', n_clicks=0),
    html.Div(id='input_bu-container'),
    
    
    html.Br(),
    html.Label('Options per Business Unit'),
    html.Button('Select/Deselect All', id='select-all-gms', n_clicks=0),
    html.Div(id='input_gms-container'),

    html.Br(),
    html.Button('Submit', id='submit-val', n_clicks=1),

    dcc.Store(id='memory-output'),

    html.Br(),
    dcc.Loading(
        id="loading-1",
        type="default",
        fullscreen=True,
        children=html.Div(id="loading-output-1")
        )

    ], style={'padding': 10, 'flex': 1})
]




app.layout = ddk.App(children=[

    ddk.Header([
        ddk.Logo(app.get_asset_url("Takeda Logo PNG.png")),
        ddk.Title('GHG Emission Forecast')
    ]),
    dcc.Tabs([
        dcc.Tab(label='GHG Emissions Forecast', children=[
            ddk.Block(width=15, children=[ddk.ControlCard(controls)]),
            ddk.Block(width=85, children=[
                ddk.Row([
                    ddk.Card(width=25, children=[ddk.CardHeader(title='KPI 1: Year-end prediction'), html.Div(id='KPI_1')]),
                    #ddk.Card(width=25, children=[ddk.CardHeader(title='KPI 2: RMSE'), html.Div(id='KPI_2')]),
                    #ddk.Card(width=25, children=[ddk.CardHeader(title='KPI 3: MAPE'), html.Div(id='KPI_3')]),
                ]),
                ddk.Row([
                    ddk.Card(width=30, children=[
                        ddk.CardHeader(title='Baseline Comparison'), 
                        ddk.Graph(id='baseline_comparison')]),
                    ddk.Card(width=70, children=[
                        ddk.CardHeader(title='GHG Emission Forecast'), 
                        ddk.Graph(id='global_box'),
                        ddk.CardFooter([
                            html.Label('Choose Time Interval'),
                            dcc.RadioItems(id='input_box_range',
                                options=['Yearly', 'Quarterly', 'Monthly'], 
                                value='Yearly'),
                            dcc.RadioItems(id='box_radio',
                                options=['Total', 'Per PortfolioOwner'],
                                value='Total')
                        ])
                    ])
                ]),
            
                ddk.Row([
                    ddk.Card(width=100, children=[
                        ddk.CardHeader(title='Global (All sites) GHG Emission Forecast'), 
                        ddk.Graph(id='fig_global'),
                        dcc.RadioItems(id='radio_fig_three',
                            options=['Total', 'Per PortfolioOwner'],
                            value='Total')
                    ])
                ]),

                ddk.Row([
                    ddk.Card(width=100, children=[
                        ddk.CardHeader(title='Data Table'), 
                        ddk.DataTable(id='data_table', page_size = 25),
                        ddk.CardFooter('Emission predictions provided in tons of CO2'),
                        html.Button("Download CSV", id="btn_csv"),
                        dcc.Download(id="download-dataframe-csv")
                    ]),
                ]),
                ddk.Row([
                    ddk.Card(width=100, children=[
                        ddk.CardHeader(title='SPOT Table'), 
                        ddk.DataTable(id='spot_table', page_size = 25),
                        ddk.CardFooter('Emission impacts provided in tons of CO2'),
                        html.Button("Download CSV", id="btn_spot_csv"),
                        dcc.Download(id="download-spot-dataframe-csv")
                    ])
                ]),

                ddk.Row([
                    ddk.Card(width=100, children=[
                        ddk.CardHeader(title='VPPA Table'), 
                        ddk.DataTable(id='vppa_table', page_size = 25),
                        ddk.CardFooter('Emissions reductions from VPPA'),
                        html.Button("Download CSV", id="btn_vppa_csv"),
                        dcc.Download(id="download-vppa-dataframe-csv")
                    ]),
                ]),
                ddk.Row([
                    ddk.Card(width=100, children=[
                        ddk.CardHeader(title='Flag Table'), 
                        ddk.DataTable(id='flag_table', page_size = 25),
                        ddk.CardFooter('List of flagged (divested) sites'),
                        html.Button("Download CSV", id="btn_flag_csv"),
                        dcc.Download(id="download-flag-dataframe-csv")
                    ])
                ])
            ]) # end block
        ]), # end tab

        dcc.Tab(label='Model Analysis', children=[
            ddk.Row([
                ddk.ControlCard(
                    html.Div(children=[
                    html.Label('Model Diagnostics'),
                    dcc.Dropdown(id='input_diagnostics_msr',
                        options=MSR_ONE,
                        #value='Natural Gas - Useage (Reported)',
                        multi=False),
                    html.Div(id='input_po-container'),
                    dcc.Dropdown(id='input_metric',
                        options=['mse','rmse', 'mae' ,'mape'],
                        value='mape',
                        multi=False),
                    dcc.Link("Click here to visit prophet library documentation", href="https://facebook.github.io/prophet/"),
                    ], style={'padding': 10, 'flex': 1}))
            ]),
            ddk.Row([
                ddk.Card(width=30, children=[ddk.CardHeader(title='Model Performance'), ddk.DataTable(id='model_performance',page_size = 25)]),
                ddk.Card(width=30, children=[ddk.CardHeader(title='Volume Coefficient'), ddk.DataTable(id='volume_coefficient',page_size = 25)])
            ]),
            ddk.Row([
                ddk.Card(width=25, children=[ddk.CardHeader(title='Fitted vs. Actuals',), ddk.Graph(id='prophet_one')]),
                ddk.Card(width=25, children=[ddk.CardHeader(title='Overall trend and yearly trend analysis',), ddk.Graph(id='prophet_two')])
            ]),
            ddk.Row([
                ddk.Card(children=[ddk.CardHeader(title='Standardized Residuals over Time',), ddk.Graph(id='graph_diagnostics_one')]),
                ddk.Card(children=[ddk.CardHeader(title='Distribution Plot',), ddk.Graph(id='graph_diagnostics_two')]),
                ddk.Card(children=[ddk.CardHeader(title='Q-Q Plot',), ddk.Graph(id='graph_diagnostics_three')])
            ]),
            ddk.Row([
                ddk.Card(width=70, children=[ddk.CardHeader(title='Cross-Validation',), ddk.Graph(id='prophet_three')])
            ])
        ]) # end Tab
    ]) # end Tabs
]) #end app






#########################
### Dash Application
#########################

@app.callback(
    Output("input_measure-container", "children"),
    Input("input_scope", "value"),
    Input("input_unit", "value"),
    Input('select-all-button', 'n_clicks'))
def update_input_measure(scope, unit, n_clicks):
    options_one = [
        'Refrigerant Leaks',
        'Fleet',
        'Natural Gas - Useage (Estimated)',
        'Natural Gas - Useage (Reported)',
        'Propane - Usage',
        'LPG - Usage',
        'Diesel Stationary - Useage',
        'Diesel High Speed - Usage',
        'Heavy Oil #5 - Usage',
        'Gasoline Mobile - Usage',
        'Diesel Mobile - Usage'
    ]
    options_two = [
        'Purchased Electricity FY 16 - FY 20 (Estimated) - Usage', 
        'Purchased Electricity - Usage',
        'On-Site Generated Renewable Electricity - Usage',
        'ELECTRICITY from on-site FUEL CELL - Usage',
        'Purchased Electricity (Estimated) - Usage',
        'Purchased Steam - Usage',
        'District Heating - Usage',
        'District Cooling - Usage'
    ]
    if ('Scope 1' in scope and 'Scope 2' not in scope):
        print('Scope one')
        options = options_one
        value = options_one
    elif  ('Scope 2' in scope and 'Scope 1' not in scope):
        options = options_two
        value = options_two
    else:
        options_one.extend(options_two)
        options = options_one
        value = options_one
    # leaks are only reported in CO2 equivalents, not in GJ
    if ('Energy (GJ)' in unit and 'Refrigerant Leaks' in options):
        options.remove('Refrigerant Leaks')
    # fleet is only reported in CO2 equivalents, not in GJ
    if ('Energy (GJ)' in unit and 'Fleet' in options):
        options.remove('Fleet')
    if n_clicks % 2 == 0:
        return dcc.Checklist(id='input_measure',
        options = [option for option in options],
        value = value)
    else:
        return dcc.Checklist(id='input_measure',
        options = [option for option in options],
        value = [])





# container for business unit
@app.callback(
    Output("input_bu-container", "children"),
    Input('select-all-bu', 'n_clicks'))
def update_input_gms_unit(n_clicks):
    values = ['GMS', 'RnD', 'BioLife', 'GREFP', 'VBU', 'Fleet']
    if n_clicks % 2 == 0:
        options = dcc.Checklist(
            id='input_bu',
            options = values,
            value=values)
    else:
        options = dcc.Checklist(
            id='input_bu',
            options = values,
            value=[])
    return options





# container for gms unit
@app.callback(
    Output("input_gms-container", "children"),
    Input("input_bu", "value"),
    Input('select-all-gms', 'n_clicks'))
def update_input_gms_unit(bu, n_clicks):
    if 'GMS' in bu:
        values = ['Plasma', 'Biologics', 'Japan', 'Small Molecule']
        if n_clicks % 2 == 0:
            options = dcc.Checklist(
                id='input_gms',
                options = values,
                value=values)
        else:
            options = dcc.Checklist(
                id='input_gms',
                options = values,
                value=[])
    else:
        options = html.Div(id='input_gms')
    return options





def load_redis_objects(measure, bu, gms, unit, data_object):
    print('load redis objects')
    
    # extract data from selected measure
    df_measure = pd.DataFrame()
    
    # exception leaks
    if 'Refrigerant Leaks' in measure:
        measure.remove('Refrigerant Leaks')
        df_json = redis_client.get('leaks')
        df = pickle.loads(df_json)
        #df['Emission Impact Sum', 'Energy Impact Sum'] = 0
        df_measure = df_measure.append(df, ignore_index=True)
    
    # exception fleet
    if 'Fleet' in measure:
        measure.remove('Fleet')
        df_json = redis_client.get('fleet')
        df = pickle.loads(df_json)
        df[['Emission Impact Accumulated', 'Energy Impact Accumulated']] = 0
        df_measure = df_measure.append(df, ignore_index=True)
    
    for i in measure:
        try:
            df_json = redis_client.get(data_object+i)
            df = pickle.loads(df_json)
            df_measure = df_measure.append(df, ignore_index=True)
        except:
            continue
    #print(df_measure)
    #df_measure.to_csv('./df_measure.csv')
    # filter based on business unit input
    print('gms filter')
    if gms:
        bu.extend(gms)
        bu.remove('GMS')
    print('bu filter')
    if bu:
        list_bu = [business_unit.get(key) for key in bu]
        list_bu = [item for sublist in list_bu for item in sublist]
        df_global = df_measure.loc[df_measure['PortfolioOwner'].isin(list_bu)]
    else:
        df_global = df_measure
 
    # select unit: energy or emission
    print('unit filter')
    if unit == 'CO2 (t)':
        print('change unit')
        df_global['y'] = df_global['y_ghg']
        df_global['y_with_spot'] = df_global['y_ghg_with_spot']
    
    # select SPOT Impact colum (energy or emission)
    if unit == 'CO2 (t)': 
        spot_impact = 'Emission Impact Accumulated'
    else:
        spot_impact = 'Energy Impact Accumulated'
    
    # calculate the total for all selected measure (reduce data set to one entry per month per portfolio owner)
    # code to be simplied
    df_global['y'] = df_global.groupby(['Impact Month', 'PortfolioOwner', 'type'])['y'].transform(lambda x: x.sum())
    df_global['yhat_lower'] = df_global.groupby(['Impact Month', 'PortfolioOwner', 'type'])['yhat_lower'].transform(lambda x: x.sum())
    df_global['yhat_upper'] = df_global.groupby(['Impact Month', 'PortfolioOwner', 'type'])['yhat_upper'].transform(lambda x: x.sum())
    df_global['Energy Impact Accumulated'] = df_global.groupby(['Impact Month', 'PortfolioOwner', 'type'])['Energy Impact Accumulated'].transform(lambda x: x.sum())
    df_global['Emission Impact Accumulated'] = df_global.groupby(['Impact Month', 'PortfolioOwner', 'type'])['Emission Impact Accumulated'].transform(lambda x: x.sum())
    print('test')
    print(df_global)
    #df_global.to_csv('./app_selection_fleet.csv')
    #df_global = pd.read_csv('./app_selection.csv')
    # y_with_spot needs to be recalculated
    df_global['y_with_spot'] = df_global['y'].astype(float)+ df_global[spot_impact].astype(float)
    print('here')
    df_global.loc[df_global['y_with_spot'] < 0, 'y_with_spot'] = 0
    print('test two')
    # these columns need to be dropped to avoid duplicates
    df_global = df_global.drop(['Emission Impact Sum', 'Energy Impact Sum', 'y_ghg', 'y_ghg_with_spot'], axis=1)
    df_global = df_global.drop_duplicates()
    print(df_global)
    print('test three')
    # check data
    #df_global.to_csv('./output_data/app_selection.csv')
    print(df_global)
    return df_global





# create a store for the selected data to work with
@app.callback(
    Output("memory-output", "data"),
    Input("submit-val", "n_clicks"),
    State('input_measure', 'value'),
    State('input_bu', 'value'),
    State('input_gms', 'value'),
    State("input_unit", "value"))
def data_store(n_clicks, measure, bu, gms, unit):
    data = load_redis_objects(measure, bu, gms, unit, 'df_global')
    print(data)
    return data.to_dict('records')





# model analysis: select portfolio owner
@app.callback(
    Output("input_po-container", "children"),
    Input('input_diagnostics_msr', 'value'))
def update_input_po(measure):
    # take the selection from measure selection on tab two as measure input (avoid interaction with tab one)
    # load the buildings per measure (not buildings per portfolio owner)
    po_bu_json = redis_client.get('po_bu'+measure)
    po_bu = pickle.loads(po_bu_json)
    options = po_bu['BUILDING_ID'].unique()
    return dcc.Dropdown(
        id='input_diagnostics_po',
        options=options,
        multi=False)





# KPI 1: year-end prediction
@app.callback(
    Output("KPI_1", "children"),
    Input("submit-val", "n_clicks"),
    Input('memory-output', 'data'),
    State('input_calendar', 'value'))
def update_kpi_one(n_clicks, data, calendar):
    # read data
    dat = pd.DataFrame.from_dict(data)
    # calculate total per month
    dat['y_global_with_spot'] = dat.groupby(['Impact Month'])['y_with_spot'].transform(lambda x: x.sum())
    dat = dat[['Impact Month', 'y_global_with_spot', 'C_MNTH', 'C_YEAR', 'C_QRTR', 'F_MNTH', 'F_YEAR', 'F_QRTR']].drop_duplicates()

    # determine the current fiscal year
    if calendar == 'Fiscal Year':
        if datetime.now().month in [1,2,3]:
            val_year = datetime.now().year-1
        else:
            val_year = datetime.now().year
        val_year_end = round(sum(dat.loc[dat['F_YEAR']==val_year, 'y_global_with_spot']))
    else:
        val_year = datetime.now().year
        val_year_end = round(sum(dat.loc[dat['C_YEAR']==val_year, 'y_global_with_spot']))
    
    prd_year_end = "The total for the current year is predicted at: {}".format(val_year_end)
    
    return prd_year_end


'''
# KPI 2
@app.callback(
    Output("KPI_2", "children"),
    Input("submit-val", "n_clicks"),
    State('input_measure', 'value'),
    State('input_bu', 'value'),
    State('input_gms', 'value'),
    State("input_unit", "value"))
def update_kpi_two(n_clicks, measure, bu, gms, unit):
    rmse_scores = load_redis_objects(measure, bu, gms, unit, 'rmse_prophet')
    print(rmse_scores)
    return round(np.mean(rmse_scores['RMSE']),2)



# KPI 3
@app.callback(
    Output("KPI_3", "children"),
    Input("submit-val", "n_clicks"),
    State('input_measure', 'value'),
    State('input_bu', 'value'),
    State('input_gms', 'value'),
    State("input_unit", "value"))
def update_KPI_three(n_clicks, measure, bu, gms, unit):
    mape_scores = load_redis_objects(measure, bu, gms, unit, 'mape_prophet')
    print(mape_scores)
    return round(np.mean(mape_scores['MAPE']),2)
'''


# bar chart: baseline comparison
@app.callback(
    Output("baseline_comparison", "figure"),
    Input("submit-val", "n_clicks"),
    Input('memory-output', 'data'),
    State('input_measure', 'value'),
    State("input_calendar", "value"),
    State('input_unit', 'value'))
def update_fig_one(n_clicks, data, measure, calendar, unit):
    
    dat = pd.DataFrame.from_dict(data)

    if unit == 'Energy (GJ)':
        column_name = 'Energy Unit (Gigajoules)'
    else:
        column_name = 'GHG Emissions (Metric tons)'

    # determine the current fiscal year
    if datetime.now().month in [1,2,3]:
        fiscal_year = datetime.now().year-1
    else:
        fiscal_year = datetime.now().year
    
    # create sums: y with spot per fiscal year or per calendar year. group by x and define x
    if calendar == 'Fiscal Year':
        x_var = 'F_YEAR'
    else:
        x_var = 'C_YEAR'

    dat['y_sum'] = dat.groupby([x_var, 'type'])['y_with_spot'].transform(lambda x: x.sum())
    dat = dat.loc[(dat[x_var]==2016) | (dat[x_var]==fiscal_year)] # reference year is set to 2016
    dat = dat[['y_sum', x_var, 'type']].drop_duplicates()
    fig = px.bar(
        dat, 
        x=x_var, 
        y="y_sum", 
        color="type", 
        title="Timeseries of Historicals & Predicted")
    
    fig = fig.update_layout(
            xaxis_title=calendar, 
            yaxis_title=column_name,
            xaxis = dict(
                tickmode = 'array',
                tickvals = [2016, fiscal_year],
                ticktext = ['2016', str(fiscal_year)])
            )
    
    return fig



# bar chart: historic and predicted data
@app.callback(
    Output("global_box", "figure"),
    Input('memory-output', 'data'),
    Input("submit-val", "n_clicks"),
    Input("input_box_range", "value"),
    Input("box_radio", "value"),
    State("input_calendar", "value"),
    State("input_unit", "value"))
def update_fig_two(data, n_clicks, timerange, radio, calendar, unit):
    
    # do not differentiate between actual and predicted values
    # Simply interested in monthly, quarterly or yearly display
    dat = pd.DataFrame.from_dict(data)
    
    if unit == 'Energy (GJ)':
        column_name = 'Energy Unit (Gigajoules)'
    else:
        column_name = 'GHG Emissions (Metric tons)'

    # display total (else per portfolio owner)
    if radio == 'Total':
        dat['PortfolioOwner'] = 'Global'
    
    # monthly calendar year
    if timerange == 'Monthly' and calendar == 'Calendar Year':
        dat['y_sum'] = dat.groupby(['C_YEAR', 'C_QRTR', 'C_MNTH', 'PortfolioOwner'])['y_with_spot'].transform(lambda x: x.sum())
        dat = dat[['y_sum', 'PortfolioOwner', 'C_YEAR', 'C_QRTR', 'C_MNTH']].drop_duplicates()
        dat['MNTH_LABEL'] = pd.to_datetime(dat['C_YEAR'].astype(str) + '-'+ dat['C_MNTH'].astype(str) + '-01')
        fig = px.bar(
            dat, 
            x="MNTH_LABEL", 
            y="y_sum", 
            color="PortfolioOwner").update_layout(
                xaxis_title='Calendar Month'
            )
    
    # monthly fiscal year
    elif timerange == 'Monthly' and calendar == 'Fiscal Year':
        dat['y_sum'] = dat.groupby(['F_YEAR', 'F_QRTR', 'F_MNTH', 'PortfolioOwner'])['y_with_spot'].transform(lambda x: x.sum())
        dat = dat[['y_sum', 'PortfolioOwner', 'F_YEAR', 'F_QRTR', 'F_MNTH']].drop_duplicates()
        dat['MNTH_LABEL'] = dat['F_MNTH'].astype(str) + '-'+ dat['F_YEAR'].astype(str)
        fig = px.bar(
            dat, 
            x="MNTH_LABEL", 
            y="y_sum", 
            color="PortfolioOwner").update_layout(
                xaxis_title='Fiscal Month'
            )

    # quarterly calendar year
    elif timerange == 'Quarterly' and calendar == 'Calendar Year':
        dat['y_sum'] = dat.groupby(['C_YEAR', 'C_QRTR', 'PortfolioOwner'])['y_with_spot'].transform(lambda x: x.sum())
        dat = dat[['y_sum', 'PortfolioOwner', 'C_YEAR', 'C_QRTR']].drop_duplicates()
        dat['Calendar Quarter'] = dat['C_QRTR'].astype(str) + '-'+ dat['C_YEAR'].astype(str)
        fig = px.bar(
            dat, 
            x="Calendar Quarter", 
            y="y_sum", 
            color="PortfolioOwner").update_layout(
                xaxis_title='Calendar Quarter'
            )
    
    # yearly calendar year
    elif timerange == 'Yearly' and calendar == 'Calendar Year':
        dat['y_sum'] = dat.groupby(['C_YEAR', 'PortfolioOwner'])['y_with_spot'].transform(lambda x: x.sum())
        dat = dat[['y_sum', 'PortfolioOwner', 'C_YEAR']].drop_duplicates()
        fig = px.bar(
            dat, 
            x="C_YEAR", 
            y="y_sum", 
            color="PortfolioOwner").update_layout(
                xaxis_title='Calendar Year'
            )
    
    # quarterly fiscal year
    elif timerange == 'Quarterly' and calendar == 'Fiscal Year':
        dat['y_sum'] = dat.groupby(['F_YEAR', 'F_QRTR', 'PortfolioOwner'])['y_with_spot'].transform(lambda x: x.sum())
        dat = dat[['y_sum', 'PortfolioOwner', 'F_YEAR', 'F_QRTR']].drop_duplicates()
        dat['Fiscal Quarter'] = dat['F_QRTR'].astype(str) + '-' + dat['F_YEAR'].astype(str)
        fig = px.bar(
            dat, 
            x="Fiscal Quarter", 
            y="y_sum", 
            color="PortfolioOwner").update_layout(
                xaxis_title='Fiscal Quarter'
            )
    
    # yearly fiscal year
    elif timerange == 'Yearly' and calendar == 'Fiscal Year':
        dat['y_sum'] = dat.groupby(['F_YEAR', 'PortfolioOwner'])['y_with_spot'].transform(lambda x: x.sum())
        dat = dat[['y_sum', 'PortfolioOwner', 'F_YEAR']].drop_duplicates()
        fig = px.bar(
            dat, 
            x="F_YEAR", 
            y="y_sum", 
            color="PortfolioOwner").update_layout(
                xaxis_title='Fiscal Quarter'
            )
    
    fig = fig.update_layout(
            title="GHG Emission: Historicals & Predicted", 
            yaxis_title=column_name)
    
    return fig





@app.callback(
    Output("fig_global", "figure"),
    Input('memory-output', 'data'),
    Input("submit-val", "n_clicks"),
    Input('radio_fig_three', 'value'),
    State('input_unit', 'value'))
def update_fig_three(data, n_clicks, radio, unit):

    dat = pd.DataFrame.from_dict(data)

    if unit == 'Energy (GJ)':
        column_name = 'Energy Unit (Gigajoules)'
    else:
        column_name = 'GHG Emissions (Metric tons)'

    if radio == 'Total':
        dat['PortfolioOwner'] = 'Global'
        # calculate the sums
        dat['y_global_with_spot'] = dat.groupby(['Impact Month'])['y_with_spot'].transform(lambda x: x.sum())
        dat['y_global_wo_spot'] = dat.groupby(['Impact Month'])['y'].transform(lambda x: x.sum())
        dat['upper_global_wo_spot'] = dat.groupby(['Impact Month'])['yhat_upper'].transform(lambda x: x.sum())
        dat['lower_global_wo_spot'] = dat.groupby(['Impact Month'])['yhat_lower'].transform(lambda x: x.sum())
        dat = dat[['Impact Month', 'y_global_with_spot', 'y_global_wo_spot', 'upper_global_wo_spot', 'lower_global_wo_spot']].drop_duplicates()
        dat = dat.sort_values(by=['Impact Month'])
        
        figure = go.Figure().add_trace(
                go.Scatter(
                    x=dat['Impact Month'],
                    y=dat['y_global_with_spot'],
                    mode = 'lines', name='Actuals & Predictions (after spot)')).add_traces(
                go.Scatter(
                    x=dat['Impact Month'],
                    y=dat['y_global_wo_spot'],
                    mode = 'lines', name='Raw Predictions'))
                    
        if unit == 'Energy (GJ)':
            figure = figure.add_traces(
                go.Scatter(
                    x=dat['Impact Month'],
                    y=dat['upper_global_wo_spot'],
                    mode = 'lines', line_color='grey', name='upper ci')).add_traces(
                go.Scatter(
                    x=dat['Impact Month'],
                    y=dat['lower_global_wo_spot'],
                    mode = 'lines', line_color='grey', fill='tonexty', name='lower ci'))
        
        figure = figure.update_layout(
                legend={'title_text':''}, 
                    xaxis_title='Date',
                    yaxis_title=column_name)

    else:
        dat = dat[['Impact Month', 'y_with_spot', 'PortfolioOwner', 'type']].drop_duplicates()
        dat['y_with_spot_sum'] = dat.groupby(['Impact Month', 'PortfolioOwner', 'type'])['y_with_spot'].transform(lambda x: x.sum())
        dat = dat[['Impact Month', 'y_with_spot_sum', 'PortfolioOwner', 'type']].drop_duplicates()
        dat = dat.sort_values(by=['Impact Month'])
        
        figure=px.line(
            x=dat['Impact Month'],
            y=dat['y_with_spot_sum'],
            color=dat['PortfolioOwner'],
            line_dash=dat['type']).update_layout(
                title='Environmental Portfolio Owner Forecast of Carbon Emissions',
                xaxis_title='Time',
                yaxis_title=column_name).update_layout(
                legend={'title_text':''}
        )
        
    return figure





# callback function for data table
@app.callback(
    Output('data_table', 'data'),
    Output('data_table', 'columns'),
    Input('memory-output', 'data'),
    Input("submit-val", "n_clicks"),
    State("input_unit", "value"))
def update_data_table(data, n_clicks, unit):
    df = pd.DataFrame.from_dict(data)
    # select SPOT Energy Impact if Energy (GJ) is selected, else select Emission Impact
    if unit == 'Energy (GJ)':
        cols = ['Impact Month', 'PortfolioOwner', 'type','y', 'yhat_lower', 'yhat_upper', 'Energy Impact Accumulated', 'y_with_spot']
    else:
        cols = ['Impact Month', 'PortfolioOwner', 'type','y', 'yhat_lower', 'yhat_upper', 'Emission Impact Accumulated', 'y_with_spot']
    df = df[cols]
    df.columns = ['Month', 'Portfolio Owner', 'Type', 'Model Prediction', 'Lower CI', 'Upper CI', 'SPOT Impact', 'Prediction (incl. SPOT Impact)']
    df = df.sort_values(by=['Month', 'Portfolio Owner'])
    columns = [{"name": i, "id": i} for i in df.columns]
    return df.to_dict('records'), columns





# download button
@app.callback(
    Output("download-dataframe-csv", "data"),
    State('memory-output', 'data'),
    State("input_unit", "value"),
    Input("btn_csv", "n_clicks"),
    prevent_initial_call=True
)
def download_data_table(data, n_clicks, unit):
    df = pd.DataFrame.from_dict(data)
     # select SPOT Energy Impact if Energy (GJ) is selected, else select Emission Impact
    if unit == 'Energy (GJ)':
        cols = ['Impact Month', 'PortfolioOwner', 'type','y', 'yhat_lower', 'yhat_upper', 'Energy Impact Accumulated', 'y_with_spot']
    else:
        cols = ['Impact Month', 'PortfolioOwner', 'type','y', 'yhat_lower', 'yhat_upper', 'Emission Impact Accumulated', 'y_with_spot']
    df = df[cols]
    df.columns = ['Month', 'Portfolio Owner', 'Type', 'Model Prediction', 'Lower CI', 'Upper CI', 'SPOT Impact', 'Prediction (incl. SPOT Impact)']
    df = df.sort_values(by=['Month', 'Portfolio Owner'])
    columns = [{"name": i, "id": i} for i in df.columns]
    return dcc.send_data_frame(df.to_csv, "ghg_emssions_data.csv")





# spot table
@app.callback(
    Output('spot_table', 'data'),
    Output('spot_table', 'columns'),
    Input("submit-val", "n_clicks"))
def update_spot_table(n_clicks):
    spot_json = redis_client.get('spot')
    spot = pickle.loads(spot_json)
    spot = spot.sort_values(by=['Impact Month', 'PortfolioOwner'])
    now = pd.to_datetime(datetime.now().strftime("%Y-%m-%d"))
    spot = spot.loc[(spot['Impact Month'] > now)]
    columns = [{"name": i, "id": i} for i in spot.columns]
    return spot.to_dict('records'), columns





# spot download button
@app.callback(
    Output("download-spot-dataframe-csv", "data"),
    Input("btn_spot_csv", "n_clicks"),
    prevent_initial_call=True
)
def download_spot_table(n_clicks):
    spot_json = redis_client.get('spot')
    spot = pickle.loads(spot_json)
    spot = spot.sort_values(by=['Impact Month', 'PortfolioOwner'])
    now = pd.to_datetime(datetime.now().strftime("%Y-%m-%d"))
    spot = spot.loc[(spot['Impact Month'] > now)]
    columns = [{"name": i, "id": i} for i in spot.columns]
    return dcc.send_data_frame(spot.to_csv, "spot_emssions_impact_data.csv")





# vppa table
@app.callback(
    Output('vppa_table', 'data'),
    Output('vppa_table', 'columns'),
    Input("submit-val", "n_clicks"))
def update_vppa_table(n_clicks):
    vppa_json = redis_client.get('vppa')
    vppa = pickle.loads(vppa_json)
    vppa = vppa[['ProblemID', 
        'ProjectDescription', 
        'EmissionsImpactRealizationDate',
        'CalculatedEmissionsImpact', 
        'EnergyImpact', 
        'PortfolioOwner']]
    columns = [{"name": i, "id": i} for i in vppa.columns]
    return vppa.to_dict('records'), columns





# vppa download button
@app.callback(
    Output("download-vppa-dataframe-csv", "data"),
    Input("btn_vppa_csv", "n_clicks"),
    prevent_initial_call=True
)
def download_vppa_table(n_clicks):
    vppa_json = redis_client.get('vppa')
    vppa = pickle.loads(vppa_json)
    vppa = vppa[['ProblemID', 
        'ProjectDescription', 
        'EmissionsImpactRealizationDate',
        'CalculatedEmissionsImpact', 
        'EnergyImpact', 
        'PortfolioOwner']]
    columns = [{"name": i, "id": i} for i in vppa.columns]
    return dcc.send_data_frame(vppa.to_csv, "vppa_list.csv")





# flag table
@app.callback(
    Output('flag_table', 'data'),
    Output('flag_table', 'columns'),
    Input("submit-val", "n_clicks"))
def update_flag_table(n_clicks):
    flag_json = redis_client.get('flag')
    flag = pickle.loads(flag_json)
    columns = [{"name": i, "id": i} for i in flag.columns]
    return flag.to_dict('records'), columns





# flag download button
@app.callback(
    Output("download-flag-dataframe-csv", "data"),
    Input("btn_flag_csv", "n_clicks"),
    prevent_initial_call=True
)
def download_flag_table(n_clicks):
    flag_json = redis_client.get('flag')
    flag = pickle.loads(flag_json)
    columns = [{"name": i, "id": i} for i in flag.columns]
    return dcc.send_data_frame(flag.to_csv, "flagged_sites.csv")




'''
# line plot: future predictions per site of top emitters
@app.callback(
    Output("prd_po", "figure"),
    Input("submit-val", "n_clicks"),
    Input('memory-output', 'data'),
    Input("top_n", "value"),
    Input('radio_fig_three', 'value'))
def update_fig_four(n_clicks, data, top, radio):
    
    dat = pd.DataFrame.from_dict(data)
    if radio == 'Total':
        dat['PortfolioOwner'] = 'Global'

    dat['y_total_per_po'] = dat.groupby(['PortfolioOwner'])['y_with_spot'].transform(lambda x: x.sum())
    top_emitters = dat[['PortfolioOwner', 'y_total_per_po']].drop_duplicates().sort_values('y_total_per_po', ascending=False).head(top)
    top_emitters = top_emitters['PortfolioOwner'].tolist()

    def get_top_emitters(x):
        if x in top_emitters:
            return x
        else:
            return 'low emitting environmental PO'
    
    dat['PortfolioOwner_TopEmitters'] = dat['PortfolioOwner'].apply(get_top_emitters)
    # sum the low emitting po
    dat['y_per_month_per_po'] = dat.groupby(['Impact Month', 'PortfolioOwner_TopEmitters'])['y_with_spot'].transform(lambda x: x.sum())
    # find minimum prediction date of low emittign envrionmental PO
    min_date = min(dat.loc[(dat['PortfolioOwner_TopEmitters']=='low emitting environmental PO')&(dat['type']=='Predicted'),'Impact Month'])
    # replace actual with predicted starting from first prediction
    dat.loc[(dat['PortfolioOwner_TopEmitters']=='low emitting environmental PO')&(dat['Impact Month']>=min_date), 'type'] = 'Predicted'
    dat_plot = dat[['Impact Month', 'y_per_month_per_po', 'PortfolioOwner_TopEmitters', 'type']].drop_duplicates()

    fig=px.line(
        x=dat_plot['Impact Month'],
        y=dat_plot['y_per_month_per_po'],
        color=dat_plot['PortfolioOwner_TopEmitters'],
        line_dash=dat_plot['type']).update_layout(
        title='Environmental Portfolio Owner Forecast of Carbon Emissions',
        xaxis_title='Time',
        yaxis_title='Carbon Emissions (tons of CO2)').update_layout(
        legend={'title_text':''})
    
    return fig
'''


# model diagnostics one
@app.callback(
    Output("graph_diagnostics_one", "figure"),
    Input("input_diagnostics_msr", "value"),
    Input('input_diagnostics_po', 'value'))
def update_graph_diagnostics(measure, portfolio_owner):
    
    prophet_residuals_json = redis_client.get('prophet_residuals'+measure)
    prophet_residuals = pickle.loads(prophet_residuals_json)

    residuals = prophet_residuals[portfolio_owner]['residual']
    residuals_norm = stats.zscore(residuals.to_list())

    fig = px.scatter(
        x=prophet_residuals[portfolio_owner]['ds'],
        y=residuals_norm).update_layout(
        xaxis_title='Time',
        yaxis_title='Standardized Residuals')
 
    return fig



# model diagnostics two
@app.callback(
    Output("graph_diagnostics_two", "figure"),
    Input("input_diagnostics_msr", "value"),
    Input('input_diagnostics_po', 'value'))
def update_graph_diagnostics(measure, portfolio_owner):
    
    prophet_residuals_json = redis_client.get('prophet_residuals'+measure)
    prophet_residuals = pickle.loads(prophet_residuals_json)

    # Add histogram of residuals
    residuals = prophet_residuals[portfolio_owner]['residual']
    residuals = stats.zscore(residuals.to_list()).tolist()

    fig = px.histogram(x=residuals, histnorm='probability density')

    x_randn = np.random.normal(0, 1, 1000)
    x_randn = stats.zscore(x_randn)
    x_randn = x_randn.tolist()
    fig_random = ff.create_distplot([x_randn], ['distplot'], curve_type='normal')
    normal_x = fig_random.data[1]['x']
    normal_y = fig_random.data[1]['y']
    
    fig.add_traces(go.Scatter(x=normal_x, y=normal_y, mode = 'lines',
        line = dict(color='red', width = 1), name = 'normal'))


    fig2 = ff.create_distplot([residuals], ['distplot'], curve_type = 'kde')
    kde_x = fig2.data[1]['x']
    kde_y = fig2.data[1]['y']

    fig.add_traces(go.Scatter(x=kde_x, y=kde_y, mode = 'lines',
                              line = dict(color='rgba(0,255,0, 0.6)', width = 1), name = 'kde'))

    fig.update_layout(xaxis_title='Standardized Residuals',
                      yaxis_title='Probability Density')
    
    return fig



# model diagnostics three
@app.callback(
    Output("graph_diagnostics_three", "figure"),
    Input("input_diagnostics_msr", "value"),
    Input("input_diagnostics_po", "value"))
def update_graph_diagnostics(measure, portfolio_owner):
    
    prophet_residuals_json = redis_client.get('prophet_residuals'+measure)
    prophet_residuals = pickle.loads(prophet_residuals_json)
    
    #data = sarima_models[portfolio_owner].resid().sort_values()
    data = prophet_residuals[portfolio_owner]['residual'].sort_values()
    data_norm = preprocessing.normalize([data])
    data_norm = np.squeeze(data_norm)
    size = data_norm.shape[0]
    
    # Create the Q-Q plot
    fig = px.scatter(
        x=np.sort(data_norm), 
        y=np.sort(np.random.normal(loc=0, scale=1, size=size)),
        trendline="ols", 
        trendline_color_override="red").update_layout(
        xaxis_title='Sample Quantiles',
        yaxis_title='Theoretical Quantiles')
    
    return fig





# prophet diagnostics one
@app.callback(
    Output("prophet_one", "figure"),
    Input("input_diagnostics_msr", "value"),
    Input("input_diagnostics_po", "value"))
def update_plot_plotly(measure, portfolio_owner):
    
    prophet_models_json = redis_client.get('prophet_models'+measure)
    prophet_models = pickle.loads(prophet_models_json)
    
    prophet_fcst_json = redis_client.get('prophet_fcst'+measure)
    prophet_fcst = pickle.loads(prophet_fcst_json)

    fig = plot_plotly(prophet_models[portfolio_owner], prophet_fcst[portfolio_owner])

    return fig





# prophet diagnostics two
@app.callback(
    Output("prophet_two", "figure"),
    Input("input_diagnostics_msr", "value"),
    Input('input_diagnostics_po', 'value'))
def update_plot_components(measure, portfolio_owner):
    
    prophet_models_json = redis_client.get('prophet_models'+measure)
    prophet_models = pickle.loads(prophet_models_json)
    
    prophet_fcst_json = redis_client.get('prophet_fcst'+measure)
    prophet_fcst = pickle.loads(prophet_fcst_json)
    
    fig = plot_components_plotly(prophet_models[portfolio_owner], prophet_fcst[portfolio_owner])
    
    return fig





# prophet diagnostics three
@app.callback(
    Output("prophet_three", "figure"),
    Input("input_diagnostics_msr", "value"),
    Input('input_diagnostics_po', 'value'),
    Input("input_metric", "value"))
def update_cv_metric(measure, portfolio_owner, metric):
    
    cv_dict_json = redis_client.get('cv_dict'+measure)
    cv_dict = pickle.loads(cv_dict_json)

    fig = plot_cross_validation_metric(cv_dict[portfolio_owner], metric=metric)
    fig = mpl_to_plotly(fig) 
    
    return fig





# model performance
@app.callback(
    Output('model_performance', 'data'),
    Output('model_performance', 'columns'),
    Input("input_diagnostics_msr", "value"),
    Input('input_diagnostics_po', 'value'))
def update_model_performance(measure, building_id):
    rmse_json = redis_client.get('rmse_scores'+measure)
    rmse = pickle.loads(rmse_json)
    mape_json = redis_client.get('mape_scores'+measure)
    mape = pickle.loads(mape_json)
    # filter for building id
    rmse = rmse.loc[rmse['BUILDING_ID']==building_id]
    # unit conversion: modeling done in joules - results reported in gigajoules
    rmse['RMSE'] = rmse['RMSE']/1000000000
    mape = mape.loc[mape['BUILDING_ID']==building_id]
    mtrc = pd.merge(rmse, mape, on = 'BUILDING_ID')
    mtrc = mtrc[['BUILDING_ID', 'RMSE', 'MAPE']]
    mtrc[['RMSE', 'MAPE']] = round(mtrc[['RMSE', 'MAPE']],6)
    print(mtrc)
    columns = [{"name": i, "id": i} for i in mtrc.columns]
    print(columns)
    return mtrc.to_dict('records'), columns





# volume coefficient
@app.callback(
    Output('volume_coefficient', 'data'),
    Output('volume_coefficient', 'columns'),
    Input("input_diagnostics_msr", "value"),
    Input('input_diagnostics_po', 'value'))
def update_volume_coefficient(measure, building_id):
    vol_coeff_json = redis_client.get('prophet_reg_coeff'+measure)
    vol_coeff = pickle.loads(vol_coeff_json)
    # filter for building id
    vol_coeff = vol_coeff[building_id]
    cols = ['center', 'coef_lower', 'coef', 'coef_upper']
    # unit conversion: modeling done in joules - results reported in gigajoules
    vol_coeff[cols] = vol_coeff[cols]/1000000000
    vol_coeff[cols] = vol_coeff[cols].apply(lambda x: round(x, 6))
    print(vol_coeff)
    columns = [{"name": i, "id": i} for i in vol_coeff.columns]
    print(columns)
    return vol_coeff.to_dict('records'), columns



if __name__ == '__main__':
    app.run(debug=True)
    

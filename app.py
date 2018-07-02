import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, Event, State
from datetime import datetime as dt
from google.cloud import bigquery
from flask import Flask, request, Response
import pandas_gbq as pd_gbq
import pandas as pd
import urllib.request, json
import plotly.graph_objs as go


mapbox_access_token = 'pk.eyJ1IjoiZ2lsdHJhcG8iLCJhIjoiY2o4eWJyNzY4MXQ1ZDJ3b2JsZHZxb3N0ciJ9.MROnmydnXtfjqjIBtC-P5g'

client = bigquery.Client()
job_config = bigquery.QueryJobConfig()
job_config.use_legacy_sql = False

app = dash.Dash(__name__)
server = app.server

app.config.supress_callback_exceptions = True

from units import *

app.css.append_css({'external_url': 'https://codepen.io/sptkl/pen/MXQKoQ.css'})

control_dict = {
    'borough': [{'label': i, 'value': i} for i in borough],
    'cb2010': [{'label': i, 'value': str(i)} for i in cb2010],
    'ct2010': [{'label': i, 'value': str(i)} for i in ct2010],
    'incident_zip':[{'label': i, 'value': str(i)} for i in incident_zip],
    'community_board': [{'label': i, 'value': i} for i in community_board],
    'agency':[{'label': i, 'value': i} for i in agency],
    'category':[{'label': i, 'value': i} for i in category],
    'season':[{'label': i, 'value': i} for i in season],
    'day_night':[{'label': i, 'value': i} for i in day_night],
    'dayofweek':[{'label': i, 'value': i} for i in dayofweek],
    'council':[{'label': i, 'value': i} for i in council],
}

app.layout = html.Div(children=[
    html.Div(children=[
        html.Div([
        html.Img(src='https://rawgit.com/SPTKL/Data_Visualization_Project/master/_flavicon_.jpg', height='60')
        ], className='one column', style={'margin':'15'}),
        html.Ul(children=[
            html.Li(children=[
                html.A('NYC 311 One-Stop Shop',
                    href='https://cart-gate-pkkx.squarespace.com/',
                    style={'font-size': '20px'}),
            ], style={'float':'left'}, className='nav'),
            html.Li(children=[
                html.A('Manual',
                    href='https://cart-gate-pkkx.squarespace.com/new-page-1/',
                ),
            ], className='nav'),
            html.Li(children=[
                html.A('Resources',
                    href='https://cart-gate-pkkx.squarespace.com/new-page/',
                )
            ], className='nav'),
            html.Li(children=[
                html.A('About US',
                    href='https://cart-gate-pkkx.squarespace.com/about-us/',
                )
            ], className='nav'),
            html.Li(children=[
                html.A('Contact',
                    href='https://cart-gate-pkkx.squarespace.com/contact-us/',
                )
            ], className='nav'),
        ], className='eleven columns', style={'margin-left':'0', 'padding':'0'}),
    ], className='row'),
    #controls
    html.Div(children=[
        html.Div(children = [
            html.Div([
                html.H2('Data Quick Access',
                    style={'text-align':'center'}),

                html.Div([
                    dcc.Dropdown(
                        id='geospatial',
                        options=[{'label': i, 'value': i} for i in ['borough','community_board',
                                                                    'incident_zip','ct2010', 'cb2010',
                                                                    'council']],
                        placeholder="Filter by Administrative Boundry")
                    ], style={'padding': '20px 20px 0px 20px'}),

                html.Div([
                    dcc.Dropdown(
                        id='geospatial_child',
                        multi=True)
                    ], style={'padding': '20px 20px 0px 20px'}),

                html.Div([
                    dcc.Dropdown(
                        id='categorical',
                        options=[{'label': i, 'value': i} for i in ['category','agency', 'day_night', 'dayofweek', 'season']],
                        placeholder="Filter by Categories")
                        ], style={'padding': '20px 20px 0px 20px'}),

                html.Div([
                    dcc.Dropdown(
                        id='categorical_child',
                        multi=True)
                        ], style={'padding': '20px 20px 0px 20px'}),

               html.Div([
                   dcc.DatePickerRange(
                       id='date_picker',
                       clearable=True,
                       min_date_allowed=dt(2010, 1, 1),
                       max_date_allowed=dt(2018, 5, 2),
                       initial_visible_month=dt(2018, 5, 2),
                       start_date=dt(2010, 5, 2),
                       end_date=dt(2018, 5, 2)),
                   ], style={'padding': '20px 20px 0px 20px'}),

            html.Div([
                dcc.RadioItems(
                    id='aggregation',
                    options=[{'label': i, 'value': i} for i in ['Aggregate', 'No Aggregate']],
                    value='No Aggregate',
                    labelStyle={'display': 'inline-block'})
                    ], style={'padding': '20px 20px 0px 20px'}),

            html.Div(id='aggregation_area', children = [
                    dcc.Dropdown(
                        id='aggregation_dropdown',
                        multi=True,
                        placeholder="Select Aggregation Units",
                        options=[{'label': i, 'value': i} for i in ['borough','community_board','incident_zip','ct2010', 'cb2010',
                                                                    'year','month','season','day_and_night',
                                                                    'category','agency']]),
                ], style={'padding': '20px 20px 0px 20px'}),

            html.Div(children=[
                html.A(id = 'download_link', children=[
                        html.Button('Download',
                            id='submit',
                            type='submit',
                            className='button-primary',
                            style={'background-color':'rgba(0,221,85,1)',
                                    'border-color' : 'rgba(0,221,85,1)',
                                    'width': '100%'})])
                ], style={'padding': '20px 20px 20px 20px'})
            ])
        ], className='three columns offset-by-one',
           style={'margin-top':'30'}),

        html.Div([
            dcc.Tabs(
                tabs=[{'label': item, 'value': item} for item in ['User Guide', 'Agency Plot', 'Category Plot']],
                value='User Guide',
                id='tabs'),
            html.Div(id='tab_output'),
        ], className='seven columns', style={'margin-top':'30'}),
    ], className='row'),
    html.Div(children=[
        html.Div([
            html.P(
            dcc.Markdown('''Developed by Baiyue Cao - [caobaiyue@nyu.edu](mailto:caobaiyue@nyu.edu)''')),
        ], className='footer', style={'text-align':'left'})
    ], className='ten columns')
])

@app.callback(
    Output('geospatial_child', 'options'),
    [Input('geospatial', 'value')])
def get_geospatial_child(spatial_unit):
    return control_dict[spatial_unit]

@app.callback(
    Output('geospatial_child', 'placeholder'),
    [Input('geospatial', 'value')])
def get_geospatial_placeholder(spatial_unit):
    if spatial_unit is None:
        return 'Leave Dropdown Empty to Select All'
    else:
        return 'Select {}'.format(spatial_unit)

@app.callback(
    Output('categorical_child', 'options'),
    [Input('categorical', 'value')])
def get_categorical_child(categorical_unit):
    return control_dict[categorical_unit]

@app.callback(
    Output('categorical_child', 'placeholder'),
    [Input('categorical', 'value')])
def get_categorical_placeholder(categorical_unit):
    if categorical_unit is None:
        return 'Leave Dropdown Empty to Select All'
    else:
        return 'Select {}'.format(categorical_unit)

@app.callback(
    Output('aggregation_area', 'style'),
    [Input('aggregation', 'value')])
def get_aggregation(value):
    if value == 'No Aggregate':
        return {'display': 'none',
                'padding': '20px 20px 0px 20px'}
    else:
        return {'display': 'block',
                'padding': '20px 20px 0px 20px'}

@app.callback(
    Output('download_link', 'href'),
    [],
    [State('geospatial', 'value'),
    State('geospatial_child', 'value'),
    State('categorical', 'value'),
    State('categorical_child', 'value'),
    State('date_picker', 'start_date'),
    State('date_picker', 'end_date'),
    State('aggregation_dropdown', 'value'),
    State('aggregation', 'value')],
    [Event('submit', 'click')])
def build_query(geo, geo_child, cat, cat_child, start_date, end_date, agg_select, agg):
    if geo == None or geo_child == None:
        geo_query = ''
    else:
        geo_query = ' '.join(("AND", geo, "in", str(geo_child).replace('[', '(').replace(']', ')')))

    if cat == None or cat_child == None:
        cat_query = ''
    else:
        cat_query = ' '.join(("AND", cat, "in", str(cat_child).replace('[', '(').replace(']', ')')))

    filter = ' '.join(("SELECT * FROM `nodal-component-204421.311_complaints.complaints`",
                        "WHERE created_date BETWEEN '{}' AND '{}'".format(str(start_date), str(end_date)), geo_query, cat_query))

    if agg == 'No Aggregate' or agg_select == None:
        query = filter
    else:
        agg_query_select = 'SELECT ' + ', '.join(agg_select) + ', count({})'.format(agg_select[0]) + ' from ({})'.format(filter)
        agg_query_groupby = ' GROUP BY ' + ', '.join(agg_select)
        query = ' '.join((agg_query_select, agg_query_groupby))
    return '/download.csv?value={}'.format(query)

@app.callback(
    Output('tab_output', 'children'),
    [Input('tabs', 'value')],
    [State('geospatial', 'value'),
    State('geospatial_child', 'value'),
    State('categorical', 'value'),
    State('categorical_child', 'value'),
    State('date_picker', 'start_date'),
    State('date_picker', 'end_date')])
def get_tab_output(value, geo, geo_child, cat, cat_child, start_date, end_date):
    if geo == None or geo_child == None:
        geo_query = ''
    else:
        geo_query = ' '.join(("AND", geo, "in", str(geo_child).replace('[', '(').replace(']', ')')))

    if cat == None or cat_child == None:
        cat_query = ''
    else:
        cat_query = ' '.join(("AND", cat, "in", str(cat_child).replace('[', '(').replace(']', ')')))

    filter = ' '.join(("SELECT * FROM `nodal-component-204421.311_complaints.complaints`",
                        "WHERE created_date BETWEEN '{}' AND '{}'".format(str(start_date), str(end_date)), geo_query, cat_query))

    if value == 'Category Plot':
        query = 'SELECT category, date_trunc(Date(created_date), MONTH) AS year_month, count(*) as count FROM ({}) GROUP BY year_month, category ORDER  BY 1,2;'.format(filter)
        df = pd_gbq.read_gbq(query, dialect='standard')
        traces = [{'x':df.year_month.unique().tolist(),
                   'y':df[df.category == cat]['count'].tolist(),
                   'type': 'line+markers',
                   'name': cat} for cat in df.category.unique().tolist()]
        return html.Div([
                dcc.Graph(
                    id='category_timeseries',
                    figure= {
                        'data': traces,
                        'layout': {
                            'title': 'Complaint Counts by Master Categories',
                            'xaxis': {'title': 'Year-month'},
                            'yaxis': {'title': 'Complaint Count'},
                            'height': 700
                            }
                        })
                    ])
    if value == 'Agency Plot':
        query = 'SELECT agency, date_trunc(Date(created_date), MONTH) AS year_month, count(*) as count FROM ({}) GROUP BY year_month, agency ORDER  BY 1,2;'.format(filter)
        df = pd_gbq.read_gbq(query, dialect='standard')
        traces = [{'x':df.year_month.unique().tolist(),
                   'y':df[df.agency == agc]['count'].tolist(),
                   'type': 'line+markers',
                   'name': agc} for agc in df.agency.unique().tolist()]
        return html.Div([
                dcc.Graph(
                    id='agency_timeseries',
                    figure= {
                        'data': traces,
                        'layout': {
                            'title': 'Complaint Counts by Agency',
                            'xaxis': {'title': 'Year-month'},
                            'yaxis': {'title': 'Complaint Count'},
                            'height': 700
                            }
                        })
                    ])
    if value == 'User Guide':
        return html.Div([
            dcc.Markdown('''
            ###### __1. Filter by Administrative Areas__
            Interested in a specific area of the city?  Select an administrative level in the first drop-down menu (i.e. by borough, community board, incident zip code,  census track, census block or  council). The dropdown directly below dynamically updates where you can select the specific administrative area(s) you are looking for. The tool will select all by default if you do not select any. Learn more about
             [how NYC311 data are geocoded](https://cart-gate-pkkx.squarespace.com/news-notes/2018/5/17/heres-another-manual-entry).
            '''.replace('   ','')),

            dcc.Markdown('''
            ###### __2. Filter by Category__
            Interested in a specific category of data such as city agencies, master complaint categories or temporal groupings (day vs night, weekday vs weekend or season)?  Select the type of category from the dropdown. The dropdown directly below dynamically updates where you can select the specific grouping you are looking for. The tool will select all by default if you do not select any. Learn more about
            [how master complaint categories are grouped](https://cart-gate-pkkx.squarespace.com/news-notes/2018/5/17/we-3-open-data).
            '''.replace('   ','')),

            dcc.Markdown('''
            ###### __3. Filter by Date__
            Interested in a specific time frame? By default, the tool selects all the dates but we encourage you to pick your own desired date range. In fact, selecting a specific year or month range would decrease the files size allowing the tool to run faster.
            '''.replace('   ','')),

            dcc.Markdown('''
            ###### __4. Aggregate Counts by Comparative Groups__
            The aggregate feature allows you to summarize the data by specific groupings. In effect, this calculates the total number of service requests in comparative groups thus making the data ready for quick analysis. Click the Aggregate radius button for the aggregation dropdown menu to appear then select the feature(s) you want to aggregate on.  The tool allows you to select and aggregate multiple administrative areas and categories using the same attributes from above. Start with one or two groups to review the output and data structure then add more attributes as needed.
            '''.replace('   ','')),

            dcc.Markdown('''
            ###### __5. Visualizations__
            Explore the data based on your filter selections by selecting the menu tab “Agency Plot” or “Category Plot.” Note that the datasets used can be further filtered and downloaded directly from the graph.Have fun playing with it!
            '''.replace('   ','')),
        ], style={'padding': '45px 35px 0px 35px', 'font-size': '14px'})


@server.route('/download.csv')
def download_csv():
    query = request.args.get('value')
    query = query.replace('%20', ' ').replace('%27', "'")
    query_job = client.query(
    query,
    location='US',
    job_config=job_config)
    def generate():
        i = 0
        for row in query_job:
            if i == 0:
                yield ','.join(str(i) for i in row.keys()) + '\n'
                i += 1
            yield ','.join(str(x) for x in row.values()) + '\n'
    return Response(generate(), mimetype = 'text/csv')

if __name__ == '__main__':
    app.run_server(debug=True)

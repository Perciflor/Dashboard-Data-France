import dash
import dash_core_components as dcc 
import dash_html_components as html 
import plotly
import plotly.graph_objs as go 
from dash.dependencies import Input, Output
import dash_table
import pandas as pd 
import numpy as np 
import folium
import os
from operator import itemgetter
import base64

# Importer tous nos datasets
df = pd.read_csv("dataset\\liensVilles.csv", dtype='unicode')
df_auto = pd.read_csv("dataset\\auto.csv")
df_chomage = pd.read_csv("dataset\\chomage.csv")
df_csp = pd.read_csv("dataset\\csp.csv")
df_del = pd.read_csv("dataset\\delinquance.csv")
df_demo = pd.read_csv("dataset\\demographie.csv", dtype='unicode')
df_elections = pd.read_csv("dataset\\elections.csv", dtype='unicode')
df_emploi = pd.read_csv("dataset\\emploi.csv")
df_entreprises = pd.read_csv("dataset\\entreprises.csv")
df_immo = pd.read_csv("dataset\\immobilier.csv")
df_infos = pd.read_csv("dataset\\infos.csv", dtype='unicode')
df_salaires = pd.read_csv("dataset\\salaires.csv")
df_sante = pd.read_csv("dataset\\santeSocial.csv", dtype='unicode')
df_candidats = pd.read_csv("dataset\\candidats_2019.csv")

# Dropdown
villes = [{'label': ville, 'value' : ville} for ville in df['ville'].unique()]

app = dash.Dash(__name__)

app.layout = html.Div([
    html.Div([
        html.H4(" Choisissez votre ville: "),
        dcc.Dropdown(
            id= 'ville-picker',
            options= villes,
            value= "Bordeaux (33000)"
        )
    ], style={
        'width':'35%',
        'border':'1px solid #eee',
        'padding':'30px 30px 30px 120px',
        'box-shadow': '0 2px 2px #ccc',
        'display':'inline-block',
        'verticalAlign': 'top'
    }),
    html.Div([
        dcc.Tabs(id = 'tabs', value='tab-1', children=[
            # ONGLET Infos Generales
            dcc.Tab(label="Infos Générales", children=[
                html.Div([
                    html.H3("Infos Générales")
                        ], style= {'background':'#F27034', 'color':'black','font-family': 'sans-serif', 'font-size' : '12', 'textAlign':'center','padding':'5px 0px 5px 0px'}),
                html.Div([
                    dash_table.DataTable(
                        id = "table_infos",
                        style_cell = {'font-family': 'sans-serif', 'textAlign' : 'center'},
                        style_data_conditional = [
                            {
                                'if' : {'column_id' : 'intitule'},
                                'textAlign' : 'left'
                            }] + [
                            {
                                'if': {'row_index' : 'odd'},
                                'backgroundColor' : 'rgb(232, 231, 231)'
                            }
                        ],
                        style_header = {
                            'backgroundColor': 'rgb(230, 230, 230)',
                            'fontWeight' : 'bold', 'textAlign' : 'center'
                        }
                    )
                ], style= {'display': 'inline-block','verticalAlign':'top','width':'99%'}),
                html.Div(id = "map", style= {'display':'inline-block','verticalAlign':'top','width':'99%', 
                                            'padding':'15px 0px 15px 10px'}),
            ]),
            # ONGLET Demographie
            dcc.Tab(label="Population", children = [
                html.H3("Population Française", style = {'background':'#F27034','font-family': 'sans-serif', 'font-size' : '12', 'color':'black', 'textAlign':'center','padding':'5px 0px 5px 0px'}),
                html.Div([
                    dcc.Graph(id= "population")
                ], style= {}),
                html.Div([
                    dcc.Graph(id= "naissances_deces")
                ], style= {}),
                html.Div([
                    dcc.Graph(id= "hommes_femmes")
                ], style= {'display': 'inline-block','verticalAlign':'top','width':'50%', 'border-right':'2px solid black'}),
                html.Div([
                    dcc.Graph(id= "ages")
                ], style= {'display': 'inline-block','verticalAlign':'top','width':'49%'}),
                html.Div([
                    dash_table.DataTable(
                        id= "repartitions",
                        style_cell = {'font-family': 'sans-serif', 'textAlign' : 'center'},
                        style_data_conditional = [
                            {
                                'if' : {'column_id' : 'intitule'},
                                'textAlign' : 'left'
                            }] + [
                            {
                                'if': {'row_index' : 'odd'},
                                'backgroundColor' : 'rgb(232, 231, 231)'
                            }
                        ],
                        style_header = {
                            'backgroundColor': 'rgb(230, 230, 230)',
                            'fontWeight' : 'bold'
                        }
                    )
                ], style= {}),
                html.Div([
                    dcc.Graph(id= "familles")
                ], style= {'display': 'inline-block','verticalAlign':'top','width':'50%', 'border-right':'2px solid black'}),
                html.Div([
                    dcc.Graph(id= "statut_marital")
                ], style= {'display': 'inline-block','verticalAlign':'top','width':'49%'}),
                html.Div([
                    dash_table.DataTable(
                        id= "repartitions_2",
                        style_cell = {'font-family': 'sans-serif', 'textAlign' : 'center'},
                        style_data_conditional = [
                            {
                                'if' : {'column_id' : 'intitule'},
                                'textAlign' : 'left'
                            }] + [
                            {
                                'if': {'row_index' : 'odd'},
                                'backgroundColor' : 'rgb(232, 231, 231)'
                            }
                        ],
                        style_header = {
                            'backgroundColor': 'rgb(230, 230, 230)',
                            'fontWeight' : 'bold', 'background':'#DEDEDE','font-family': 'sans-serif', 'font-size' : '12', 'color':'black', 'textAlign':'center','padding':'5px 0px 5px 0px'
                        }
                    )
                ], style= {}),
                html.H3("Population étrangère", style = {'background':'#F27034','font-family': 'sans-serif', 'font-size' : '12', 'color':'black', 'textAlign':'center','padding':'5px 0px 5px 0px'}),
                html.Div([
                    dcc.Graph(id= "evolution_etrangers")
                ], style= {}),
                html.Div([
                    dcc.Graph(id = "repartitions_etrangers_HF")
                ], style= {'display': 'inline-block','verticalAlign':'top','width':'50%', 'border-right':'2px solid black'}),
                html.Div([
                    dcc.Graph(id= "repartitions_etrangers_ages")
                ], style= {'display': 'inline-block','verticalAlign':'top','width':'49%'}),
                html.Div([
                    dash_table.DataTable(
                        id= 'tableau_etrangers',
                        style_cell = {'font-family': 'sans-serif', 'textAlign' : 'center'},
                        style_data_conditional = [
                            {
                                'if' : {'column_id' : 'intitule'},
                                'textAlign' : 'left'
                            }] + [
                            {
                                'if': {'row_index' : 'odd'},
                                'backgroundColor' : 'rgb(232, 231, 231)'
                            }
                        ],
                        style_header = {
                            'backgroundColor': 'rgb(230, 230, 230)',
                            'fontWeight' : 'bold'
                        }
                    )
                ], style= {}),
                html.H3("Population Immigrée", style = {'background':'#F27034','font-family': 'sans-serif', 'font-size' : '12', 'color':'black', 'textAlign':'center','padding':'5px 0px 5px 0px'}),
                html.Div([
                    dcc.Graph(id = "evolution_immigres")
                ], style= {}),
                html.Div([
                    dcc.Graph(id = "repartitions_immigres_HF")
                ], style= {'display': 'inline-block','verticalAlign':'top','width':'50%', 'border-right':'2px solid black'}),
                html.Div([
                    dcc.Graph(id = "repartitions_immigres_ages")
                ], style= {'display': 'inline-block','verticalAlign':'top','width':'49%'}),
                html.Div([
                    dash_table.DataTable(
                        id= "tableau_immigres",
                        style_cell = {'font-family': 'sans-serif', 'textAlign' : 'center'},
                        style_data_conditional = [
                            {
                                'if' : {'column_id' : 'intitule'},
                                'textAlign' : 'left'
                            }] + [
                            {
                                'if': {'row_index' : 'odd'},
                                'backgroundColor' : 'rgb(232, 231, 231)'
                            }
                        ],
                        style_header = {
                            'backgroundColor': 'rgb(230, 230, 230)',
                            'fontWeight' : 'bold'
                        }
                    )
                ], style= {}),
            ]),
            # ONGLET Sante et social
            dcc.Tab(label="Santé et social", children = [
                html.Div([
                    html.H1("Santé", style= {'background':'#F27034', 'color':'black','font-family': 'sans-serif', 'font-size' : '12', 'textAlign':'center','padding':'5px 0px 5px 0px'}),
                    html.Div([
                        dash_table.DataTable(
                            id= "tableau_praticiens",
                            style_cell = {'font-family': 'sans-serif', 'textAlign' : 'center'},
                            style_data_conditional = [
                                {
                                    'if' : {'column_id' : 'intitule'},
                                    'textAlign' : 'left'
                                }] + [
                                {
                                    'if': {'row_index' : 'odd'},
                                    'backgroundColor' : 'rgb(232, 231, 231)'
                                }
                            ],
                            style_header = {
                                'backgroundColor': 'rgb(230, 230, 230)',
                                'fontWeight' : 'bold'
                            }
                        )
                    ], style = {}),
                    html.Div([
                        dcc.Graph(id = "etablissements")
                    ], style= {'display': 'inline-block','verticalAlign':'top','width':'50%', 'border-right':'2px solid black'}),
                    html.Div([
                        dcc.Graph(id = "praticiens")
                    ], style= {'display': 'inline-block','verticalAlign':'top','width':'49%'}),
                    html.Div([
                        dash_table.DataTable(
                            id= "tableau_etablissements",
                            style_cell = {'font-family': 'sans-serif', 'textAlign' : 'center'},
                            style_data_conditional = [
                                {
                                    'if' : {'column_id' : 'intitule'},
                                    'textAlign' : 'left'
                                }] + [
                                {
                                    'if': {'row_index' : 'odd'},
                                    'backgroundColor' : 'rgb(232, 231, 231)'
                                }
                            ],
                            style_header = {
                                'backgroundColor': 'rgb(230, 230, 230)',
                                'fontWeight' : 'bold'
                            }
                        )
                    ], style = {})
                ]),
                html.Div([
                    html.H1("Social", style= {'background':'#F27034', 'color':'black','font-family': 'sans-serif', 'font-size' : '12','textAlign':'center','padding':'15px 0px'}),
                    html.Div([
                        dcc.Graph(id="caf")
                        ], style={'border':'1px solid #eee','box-shadow':'0 2px 2px #cc', 'display':'inline-block','width':'50%'}),
                    html.Div([
                        dcc.Graph(id = "rsa")
                        ], style={'border':'1px solid #eee','box-shadow':'0 2px 2px #cc', 'display':'inline-block','width':'49%'}),
                    html.Div([
                        dcc.Graph(id = "apl")
                        ], style={'border':'1px solid #eee','box-shadow':'0 2px 2px #cc', 'display':'inline-block','width':'50%'}),
                    html.Div([
                        dcc.Graph(id= "alloc")
                        ], style={'border':'1px solid #eee','box-shadow':'0 2px 2px #cc', 'display':'inline-block','width':'49%'}),
                ])
            ]),
            # ONGLET Immobilier
            dcc.Tab(label="Immobilier", children = [
                html.Div([
                    html.H1("Immobilier")
                ], style={'background':'#F27034', 'color':'black', 'textAlign':'center','padding':'5px 0px 5px 0px'}),
                html.Div([
                    html.Div([
                        html.H4("Prix au m²: "),
                        html.P(id="prixM2", style= {'fontSize': '30px', 'color' :'green', 'fontWeight' : '600'})
                    ], style= {'display': 'inline-block','width':'99%', 'border' : 'px solid black',
                                'paddingLeft':'20px', 'paddingRight' : '20px', 'textAlign' : 'center'}),
                    html.Div([
                        html.H4("Moyenne France: "),
                        html.P(id="prix_moyen", style= {'fontSize': '30px', 'color' :'orange', 'fontWeight' : '600'})
                    ], style= {'display': 'inline-block','width':'99%', 'border' : '1px solid black',
                                'paddingLeft':'20px', 'paddingRight' : '20px', 'textAlign' : 'center'}),
                    html.Div([
                        html.H4("Informations complementaires: "),
                        html.P(id= "nb_logements")
                    ], style= {'display': 'inline-block','width':'99%','paddingLeft':'20px', 'border' : '1px solid black', 'paddingRight' : '20px', 
                                'textAlign' : 'center'})
                ], style={}),
                html.Div([
                    dcc.Graph(id= "residences")
                ], style={'display': 'inline-block','verticalAlign':'top','width':'50%', 'border-right':'2px solid black'}),
                html.Div([
                    dcc.Graph(id= "maisons_apparts")
                ], style={'display': 'inline-block','verticalAlign':'top','width':'49%'}),
                html.Div([
                    dash_table.DataTable(
                        id= "tableau_immo",
                        style_cell = {'font-family': 'sans-serif', 'textAlign' : 'center'},
                        style_data_conditional = [
                            {
                                'if' : {'column_id' : 'intitule'},
                                'textAlign' : 'left'
                            }] + [
                            {
                                'if': {'row_index' : 'odd'},
                                'backgroundColor' : 'rgb(232, 231, 231)'
                            }
                        ],
                        style_header = {
                            'backgroundColor': 'rgb(230, 230, 230)',
                            'fontWeight' : 'bold'
                        }
                        )
                ], style={}),
                html.Div([
                    dcc.Graph(id= "types")
                ], style={'display': 'inline-block','verticalAlign':'top','width':'50%', 'border-right':'2px solid black'}),
                html.Div([
                    dcc.Graph(id = "pieces")
                ], style={'display': 'inline-block','verticalAlign':'top','width':'49%'}),
            ]),
            # ONGLET Entreprises
            dcc.Tab(label="Entreprises", children = [
                html.Div([
                        html.H3('Entreprises',  style = {'background':'#F27034', 'color':'black','font-family': 'sans-serif', 'textAlign':'center','padding':'5px 0px 5px 0px'})
                    ]),
                html.Div([
                        dcc.Graph(id="nb_entreprises")
                    ]),
                html.Div([
                    dash_table.DataTable(
                        id="entreprises_table",
                        style_cell = {'font-family': 'sans-serif', 'textAlign' : 'center'},
                        style_data_conditional = [
                            {
                                'if' : {'column_id' : 'intitule'},
                                'textAlign' : 'left'
                            }] + [
                            {
                                'if': {'row_index' : 'odd'},
                                'backgroundColor' : 'rgb(232, 231, 231)'
                            }
                        ],
                        style_header = {
                            'backgroundColor': 'rgb(230, 230, 230)',
                            'fontWeight' : 'bold'
                        }
                    )
                ], style={}),
                html.Div([
                    html.Div([
                        dcc.Graph(id="nb_creations")
                    ])
                ], style={}),
            ]),
            # ONGLET Emploi
            dcc.Tab(label="Emploi", children = [
                html.Div([
                    html.H1("Emploi / Chomage")
                ], style= {'background':'#F27034', 'color':'black','font-family': 'sans-serif', 'textAlign':'center','padding':'5px 0px 5px 0px'}),
                 html.Div([
                    dash_table.DataTable(
                        id= "table_emploi_HF",
                        style_cell = {'font-family': 'sans-serif', 'textAlign' : 'center'},
                        style_data_conditional = [
                            {
                                'if' : {'column_id' : 'intitule'},
                                'textAlign' : 'left'
                            }] + [
                            {
                                'if': {'row_index' : 'odd'},
                                'backgroundColor' : 'rgb(232, 231, 231)'
                            }
                        ],
                        style_header = {
                            'backgroundColor': 'rgb(230, 230, 230)',
                            'fontWeight' : 'bold'
                        }
                    )
                ], style={}),
                html.Div([
                    dcc.Graph(id= "evolution_chomage")
                ], style= {}),
                html.Div([
                    dash_table.DataTable(
                        id= "table_emploi_ages",
                        style_cell = {'font-family': 'sans-serif', 'textAlign' : 'center'},
                        style_data_conditional = [
                            {
                                'if' : {'column_id' : 'intitule'},
                                'textAlign' : 'left'
                            }] + [
                            {
                                'if': {'row_index' : 'odd'},
                                'backgroundColor' : 'rgb(232, 231, 231)'
                            }
                        ],
                        style_header = {
                            'backgroundColor': 'rgb(230, 230, 230)',
                            'fontWeight' : 'bold'
                        }
                    )
                ], style={}),
                html.Div([
                    dcc.Graph(id = "emploi_HF")
                ], style={'display': 'inline-block','verticalAlign':'top','width':'50%', 'border-right':'2px solid black'}),
                html.Div([
                    dcc.Graph(id = "emploi_ages")
                ], style={'display': 'inline-block','verticalAlign':'top','width':'49%'}),
                html.Div([
                    dash_table.DataTable(
                        id= "table_parts_actifs",
                        style_cell = {'font-family': 'sans-serif', 'textAlign' : 'center'},
                        style_data_conditional = [
                            {
                                'if' : {'column_id' : 'intitule'},
                                'textAlign' : 'left'
                            }] + [
                            {
                                'if': {'row_index' : 'odd'},
                                'backgroundColor' : 'rgb(232, 231, 231)'
                            }
                        ],
                        style_header = {
                            'backgroundColor': 'rgb(230, 230, 230)',
                            'fontWeight' : 'bold'
                        }
                    )
                ], style={}),
                html.Div([
                    dcc.Graph(id = "salaries")
                ], style={'display': 'inline-block','verticalAlign':'top','width':'50%', 'border-right':'2px solid black'}),
                html.Div([
                    dcc.Graph(id = "salaries_partiels_ages")
                ], style={'display': 'inline-block','verticalAlign':'top','width':'49%'}),
                html.Div([
                    dash_table.DataTable(
                        id= "table_salaries",
                        style_cell = {'font-family': 'sans-serif', 'textAlign' : 'center'},
                        style_data_conditional = [
                            {
                                'if' : {'column_id' : 'intitule'},
                                'textAlign' : 'left'
                            }] + [
                            {
                                'if': {'row_index' : 'odd'},
                                'backgroundColor' : 'rgb(232, 231, 231)'
                            }
                        ],
                        style_header = {
                            'backgroundColor': 'rgb(230, 230, 230)',
                            'fontWeight' : 'bold'
                        }
                    )
                ], style={}),
                html.Div([
                    dcc.Graph(id = "salaries_partiels_HF")
                ], style={'display': 'inline-block','verticalAlign':'top','width':'50%', 'border-right':'2px solid black'}),
                                html.Div([
                    dcc.Graph(id = "parts_actifs")
                ], style={'display': 'inline-block','verticalAlign':'top','width':'49%'}),
                html.Div([
                    dash_table.DataTable(
                        id= "table_salaires_partiels",
                        style_cell = {'font-family': 'sans-serif', 'textAlign' : 'center'},
                        style_data_conditional = [
                            {
                                'if' : {'column_id' : 'intitule'},
                                'textAlign' : 'left'
                            }] + [
                            {
                                'if': {'row_index' : 'odd'},
                                'backgroundColor' : 'rgb(232, 231, 231)'
                            }
                        ],
                        style_header = {
                            'backgroundColor': 'rgb(230, 230, 230)',
                            'fontWeight' : 'bold'
                        }
                    )
                ], style={}),
            ]),
            # ONGLET Salaires
            dcc.Tab(label="Salaires", children=[
                html.Div([
                        html.H3('Salaires',  style = {'background':'#F27034', 'color':'black','font-family': 'sans-serif', 'textAlign':'center','padding':'5px 0px 5px 0px'})
                    ]),
                html.Div([
                    dcc.Graph(
                    id='graph')
                ], style={}),
                html.Div([
                    dash_table.DataTable(
                        id="table_salaires",
                        style_cell = {'font-family': 'sans-serif', 'textAlign' : 'center'},
                        style_data_conditional = [
                            {
                                'if' : {'column_id' : 'intitule'},
                                'textAlign' : 'left'
                            }] + [
                            {
                                'if': {'row_index' : 'odd'},
                                'backgroundColor' : 'rgb(232, 231, 231)'
                            }
                        ],
                        style_header = {
                            'backgroundColor': 'rgb(230, 230, 230)',
                            'fontWeight' : 'bold'
                        })
                ], style={})
            ]),
            # ONGLET CSP
            dcc.Tab(label="CSP", children = [
                html.Div([
                        html.H3('Catégories socioprofessionnelles (CSP)',  style = {'background':'#F27034', 'color':'black','font-family': 'sans-serif', 'textAlign':'center','padding':'5px 0px 5px 0px'})
                    ]),
                html.Div([
                    dcc.Graph(id="diplomes_hf_bar")
                ], style={}),
                html.Div([
                    dash_table.DataTable(
                        id="diplomes_table",
                        style_cell = {'font-family': 'sans-serif', 'textAlign' : 'center'},
                        style_data_conditional = [
                            {
                                'if' : {'column_id' : 'intitule'},
                                'textAlign' : 'left'
                            }] + [
                            {
                                'if': {'row_index' : 'odd'},
                                'backgroundColor' : 'rgb(232, 231, 231)'
                            }
                        ],
                        style_header = {
                            'backgroundColor': 'rgb(230, 230, 230)',
                            'fontWeight' : 'bold'
                        }
                    )
                ], style={}),
                html.Div([
                    dcc.Graph(id="diplomes_pie")
                ], style={'display': 'inline-block','verticalAlign':'top','width':'50%', 'border-right':'2px solid black'}),
                html.Div([
                    dcc.Graph(id="metiers_pie")
                ], style={'display': 'inline-block','verticalAlign':'top','width':'49%'}),
            ]),
            # ONGLET Automobiles
            dcc.Tab(label="Automobiles", children= [
                html.Div([
                        html.H3('Automobiles/Accidents de la route',  style = {'background':'#F27034', 'color':'black','font-family': 'sans-serif', 'textAlign':'center','padding':'5px 0px 5px 0px'})
                    ]),
                html.Div([
                    html.Div([
                        html.H4("Nombre de Voitures: "),
                        html.P(id="total_voitures", style={'display': 'inline-block','verticalAlign':'top','width':'99%', 'border':'2px solid black'})
                    ], style= {}),
                    html.Div([
                        html.H4("Nombre total d'accidents: "),
                        html.P(id="total_accidents", style={'display': 'inline-block','verticalAlign':'top','width':'99%', 'border':'2px solid black'})
                    ], style= {}),
                    html.Div([
                        html.H4("Ménages avec place(s) de stationnement: "),
                        html.P(id="total_stationnement", style={'display': 'inline-block','verticalAlign':'top','width':'99%', 'border':'2px solid black'})
                    ], style= {})
                ], style={}),
                html.Div([
                    dcc.Graph(
                        id="voitures_pie"
                    )
                ], style={'display': 'inline-block','verticalAlign':'top','width':'50%', 'border-right':'2px solid black'}),
                html.Div([
                    dcc.Graph(
                        id="accidents_pie"
                    )
                ], style={'display': 'inline-block','verticalAlign':'top','width':'49%'})
            ]),
            # ONGLET Délinquance
            dcc.Tab(label="Délinquance", children = [
                html.Div([
                        html.H3('Délinquance',  style = {'background':'#F27034', 'color':'black','font-family': 'sans-serif', 'textAlign':'center','padding':'5px 0px 5px 0px'})
                    ]),
                html.Div([
                    dash_table.DataTable(
                        id="delinquance_table",
                        style_cell = {'font-family': 'sans-serif', 'textAlign' : 'center'},
                        style_data_conditional = [
                            {
                                'if' : {'column_id' : 'intitule'},
                                'textAlign' : 'left'
                            }] + [
                            {
                                'if': {'row_index' : 'odd'},
                                'backgroundColor' : 'rgb(232, 231, 231)'
                            }
                        ],
                        style_header = {
                            'backgroundColor': 'rgb(230, 230, 230)',
                            'fontWeight' : 'bold'
                        }
                    )
                ], style={}),
                html.Div([
                    dcc.Graph(id= "delinquance1_pie")
                ], style={'display': 'inline-block','verticalAlign':'top','width':'33%', 'border-right':'2px solid black'}),
                html.Div([
                    dcc.Graph(id= "violence_pie")
                ], style={'display': 'inline-block','verticalAlign':'top','width':'33%', 'border-right':'2px solid black'}),
                html.Div([
                    dcc.Graph(id = "vols_pie")
                ], style={'display': 'inline-block','verticalAlign':'top','width':'33%'}),
            ]),
            # ONGLET Européennes
            dcc.Tab(label="Européennes 2019", children=[
                html.Div([
                    html.H3("Les elections europennes de 2019", style= {'background':'#F27034', 'color':'black','font-family': 'sans-serif', 'textAlign':'center','padding':'5px 0px 5px 0px'})
                ]),
                html.Div(id= "elections", style={'border': '1px solid #eee', 'box-shadow': '0 2px 2px #ccc','width':'65%', 
                                'paddingLeft':'20px','paddingTop' : '30px', 'display': 'inline-block'})
            ]),
            # ONGLET CHOMAGE EN FRANCE
            dcc.Tab(label="Évolution du chomage", children= [
                html.Div([
                    html.H2(id='annee', style={'background':'#F27034', 'color':'black','font-family': 'sans-serif', 'textAlign':'center','padding':'5px 0px 5px 0px'})
                ]),
                html.Div(id="map_chomage"),
                html.Div([
                    dcc.Slider(id='slider', 
                                min=2004, 
                                max=2016, 
                                marks={i: str(i) for i in range(2004,2017)}, 
                                value=2004,)
                ])
            ])
        ])
    ])
])

######## ONGLET INFOS GENERALES ###########
# Afficher les infos generales
@app.callback([Output('table_infos','data'), Output('table_infos','columns')],
                [Input('ville-picker','value')])
def update_generales(ville_choisie):
    colonnes = df_infos.columns
    colonnes_off = ['lien','Unnamed: 0','ville']

    listeInfos = [info for info in colonnes if info not in colonnes_off]
    infos = {
        'intitule': listeInfos,
        'donnee' : [df_infos[df_infos['ville'] == ville_choisie][col].iloc[0] for col in listeInfos]
    }

    table = pd.DataFrame(infos)
    data = table.to_dict("rows")

    entete = {'id': 'intitule', 'name': "   "}, {'id': 'donnee', 'name': ville_choisie}

    return data, entete

# Afficher la localisation sur une carte
@app.callback(Output('map', 'children'), [Input('ville-picker','value')])
def update_location(ville_choisie):
    longitude = df_infos[df_infos['ville'] == ville_choisie]['Longitude'].iloc[0]
    latitude = df_infos[df_infos['ville'] == ville_choisie]['Latitude'].iloc[0]

    carte = folium.Map(location= (latitude, longitude), zoom_start=10)
    marker = folium.Marker(location = [latitude, longitude])
    marker.add_to(carte)

    fichier = "locations\\localisation_" + ville_choisie + ".html"

    if not os.path.isfile(fichier):
        carte.save(fichier)

    return html.Iframe(srcDoc = open(fichier, 'r').read(), width='100%', height = '600')

######## ONGLET DEMOGRAPHIE #############
# Affficher le graphe de la popuation
@app.callback(Output('population','figure'), [Input('ville-picker','value')])
def popuation_graph(ville_choisie):
    x_axis = np.array(range(2006,2016))
    y_axis = [
        df_demo[df_demo['ville'] == ville_choisie]["nbre habitants (" + str(annee) + ")"].iloc[0] for annee in range (2006,2016)
    ]

    ville_choisie = ville_choisie.split('(')[0].strip()

    traces = []
    traces.append(
        go.Scatter(
            x= x_axis,
            y= y_axis,
            mode= 'lines+markers',
            line= {'shape':'spline', 'smoothing' : 1},
        )
    )

    return {
        'data' : traces,
        'layout' : go.Layout(
            title = "Évolution de la population à " + ville_choisie,
            xaxis= {'title' : ''},
            yaxis= dict(title = "NOmbre d'habitants"),
            hovermode= 'closest',
            legend_orientation= 'h'
        )
    }

# Afficher l'evolution des naissances et deces
@app.callback(Output("naissances_deces", 'figure'), [Input('ville-picker','value')])
def naissances_deces_graph(ville_choisie):
    x_axis = np.array(range(1999,2017))
    y_axis_naissances = [
        df_demo[df_demo["ville"] == ville_choisie]["nbre naissances (" + str(a) + ")"].iloc[0] for a in range(1999,2017)
    ]
    y_axis_deces = [
        df_demo[df_demo["ville"] == ville_choisie]["nbre deces (" + str(a) + ")"].iloc[0] for a in range(1999,2017)
    ]

    ville_choisie = ville_choisie.split('(')[0].strip()

    traces = [
        go.Scatter(
            x= x_axis,
            y= y_axis_naissances,
            mode= 'lines+markers',
            line={'shape':'spline','smoothing':1},
            name= "Naissances à " + ville_choisie
        ),
        go.Scatter(
            x=x_axis,
            y= y_axis_deces,
            mode='lines+markers',
            line= {'shape':'spline','smoothing':1},
            name= "Deces à " + ville_choisie
        )
    ]

    return {
        'data': traces,
        'layout': go.Layout(
            title = "Évolution des Naissances et Déces à " + ville_choisie,
            xaxis= {'title':''},
            yaxis= dict(title = "Nombre de personnes"),
            hovermode= 'closest',
            legend_orientation= 'h'

        )
    }
    
# Afficher le camenbert repartition hommes/ femmes
@app.callback(Output("hommes_femmes","figure"), [Input('ville-picker','value')])
def repartition_HF(ville_choisie):
    nb_hommes = df_demo[df_demo['ville'] == ville_choisie]["Hommes"].iloc[0]
    nb_femmes = df_demo[df_demo['ville'] == ville_choisie]["Femmes"].iloc[0]

    labels = ['Hommes', 'Femmes']
    values = [float(nb_hommes), float(nb_femmes)]
    total = sum(values)

    traces = [
        go.Pie(labels= labels, values= values)
    ]

    return {
        'data': traces,
        'layout': go.Layout(
            title= "Répartition Hommes/Femmes<br> (Total: " + str(total) + ")",
            legend_orientation= 'h'
        )
    }

# Repartition par tranches d'ages
@app.callback(Output('ages','figure'), [Input('ville-picker','value')])
def repartition_ages(ville_choisie):
    colonnes = ["Moins de 15 ans","15 - 29 ans","30 - 44 ans","45 - 59 ans","60 - 74 ans","75 ans et plus"]

    labels = colonnes
    values = [float(df_demo[df_demo['ville'] == ville_choisie][colonne].iloc[0]) for colonne in colonnes]
    total = sum(values)

    traces = [
        go.Pie(labels= labels, values= values)
    ]

    return {
        'data': traces,
        'layout': go.Layout(
            title= "Repartition par Tranches d'ages<br> (Total: " + str(total) + ")",
            legend_orientation= 'h'
        )
    }

# Tableau des repartitions hommes/femmes et tranches d'ages
@app.callback([Output('repartitions','data'), Output('repartitions','columns')], [Input('ville-picker','value')])
def table_repartitions(ville_choisie):
    colonnes = ["Hommes","Femmes","Moins de 15 ans","15 - 29 ans","30 - 44 ans","45 - 59 ans","60 - 74 ans","75 ans et plus"]

    infos = {
        'intitule': colonnes,
        'donnee' : [df_demo[df_demo['ville'] == ville_choisie][colonne].iloc[0] for colonne in colonnes]
    }
    table = pd.DataFrame(infos)
    data = table.to_dict('rows')

    entete = [{'id': 'intitule', 'name': '   '}, {'id': 'donnee', 'name' : ville_choisie.split('(')[0].strip()}]

    return data, entete

# Repartition des familles
@app.callback(Output('familles','figure'), [Input('ville-picker','value')])
def repartition_familles(ville_choisie):
    colonnes = ["Familles monoparentales","Couples sans enfant","Couples avec enfant",
                "Familles sans enfant","Familles avec un enfant","Familles avec deux enfants","Familles avec trois enfants",
                "Familles avec quatre enfants ou plus"]

    labels = colonnes
    values = [
        float(df_demo[df_demo['ville'] == ville_choisie][colonne].iloc[0]) for colonne in colonnes
    ]
    total = sum(values)

    traces = [
        go.Pie(labels= labels, values= values)
    ]

    return {
        'data' : traces,
        'layout': go.Layout(
            title= "Composition des familles<br> (Total: " + str(total) + ")",
            legend_orientation = 'h'
        )
    }

# Repartition du statut marital
@app.callback(Output('statut_marital','figure'), [Input('ville-picker','value')])
def repartition_statut_marital(ville_choisie):
    colonnes = ["Personnes célibataires","Personnes mariées","Personnes divorcées","Personnes veuves"]

    labels = colonnes
    values = [
        float(df_demo[df_demo['ville'] == ville_choisie][colonne].iloc[0]) for colonne in colonnes
    ]
    total = sum(values)

    traces = [
        go.Pie(labels= labels, values= values)
    ]

    return {
        'data': traces,
        'layout': go.Layout(
            title="Status Marital<br> (Total: " + str(total) + ")",
            legend_orientation="h"
        )
    }

# Tableau 2 des repartitions
@app.callback([Output('repartitions_2','data'), Output('repartitions_2','columns')], [Input('ville-picker','value')])
def table_repartitions(ville_choisie):
    colonnes = ["Familles monoparentales","Couples sans enfant","Couples avec enfant",
                "Familles sans enfant","Familles avec un enfant","Familles avec deux enfants","Familles avec trois enfants",
                "Familles avec quatre enfants ou plus","Personnes célibataires","Personnes mariées","Personnes divorcées",
                "Personnes veuves"]

    infos = {
        'intitule': colonnes,
        'donnee' : [df_demo[df_demo['ville'] == ville_choisie][colonne].iloc[0] for colonne in colonnes]
    }
    table = pd.DataFrame(infos)
    data = table.to_dict('rows')

    entete = [{'id': 'intitule', 'name': '   '}, {'id': 'donnee', 'name' : ville_choisie.split('(')[0].strip()}]

    return data, entete

# Évolution Etrangers et immigres
@app.callback([Output('evolution_etrangers','figure'), Output('evolution_immigres','figure')], 
                [Input('ville-picker','value')])
def evolution_etrangers_immigres(ville_choisie):
    x_axis= np.array(range(2006,2016))
    y_axis_etrangers = [
        df_demo[df_demo['ville'] == ville_choisie]["nbre étrangers (" + str(a) + ")"].iloc[0] for a in range(2006,2016)
    ]
    y_axis_immigres = [
        df_demo[df_demo['ville'] == ville_choisie]["nbre immigrés (" + str(a) + ")"].iloc[0] for a in range(2006,2016)
    ]

    ville_choisie = ville_choisie.split('(')[0].strip()

    traceEtrangers = [
        go.Scatter(
            x=x_axis,
            y=y_axis_etrangers,
            mode='lines+markers',
            line= {'shape': 'spline','smoothing': 1}
        )
    ]

    traceImmigres = [
        go.Scatter(
            x=x_axis,
            y=y_axis_immigres,
            mode='lines+markers',
            line= {'shape': 'spline','smoothing': 1}
        )
    ]

    figureEtrangers = {
        'data': traceEtrangers,
        'layout': go.Layout(
            title= "Évolution de la population etrangere<br> à " + ville_choisie,
            xaxis = {'title': ''},
            yaxis= dict(title= "Nombre d'étrangers"),
            hovermode='closest'
        )
    }

    figureImmigres = {
        'data': traceImmigres,
        'layout': go.Layout(
            title= "Évolution de la population immigree<br> à " + ville_choisie,
            xaxis = {'title': ''},
            yaxis= dict(title= "Nombre d'étrangers"),
            hovermode='closest'
        )
    }

    return figureEtrangers, figureImmigres

# Camembert HF etrangers
@app.callback(Output("repartitions_etrangers_HF","figure"), [Input('ville-picker','value')])
def repartition_HF_Etrangers(ville_choisie):
    nb_hommes = df_demo[df_demo['ville'] == ville_choisie]["Hommes étrangers"].iloc[0]
    nb_femmes = df_demo[df_demo['ville'] == ville_choisie]["Femmes étrangères"].iloc[0]

    labels = ["Hommes étrangers","Femmes étrangères"]
    values = [float(nb_hommes), float(nb_femmes)]
    total = sum(values)

    traces = [
        go.Pie(labels= labels, values= values)
    ]

    return {
        'data': traces,
        'layout': go.Layout(
            title= "Population Etrangere | Hommes/Femmes<br> (Total: " + str(total) + ")",
            legend_orientation= 'h'
        )
    }

# Camembert Ages etrangers
@app.callback(Output('repartitions_etrangers_ages','figure'), [Input('ville-picker','value')])
def repartition_ages(ville_choisie):
    colonnes = ["Moins de 15 ans étrangers","15-24 ans étrangers","25-54 ans étrangers","55 ans et plus étrangers"]

    labels = colonnes
    values = [float(df_demo[df_demo['ville'] == ville_choisie][colonne].iloc[0]) for colonne in colonnes]
    total = sum(values)

    traces = [
        go.Pie(labels= labels, values= values)
    ]

    return {
        'data': traces,
        'layout': go.Layout(
            title= "Répartition par Tranches d'ages des Etrangers<br> (Total: " + str(total) + ")",
            legend_orientation= 'h'
        )
    }

# Tableau repartitions etrangers
@app.callback([Output('tableau_etrangers','data'), Output('tableau_etrangers','columns')], [Input('ville-picker','value')])
def table_repartitions(ville_choisie):
    colonnes = ["Hommes étrangers","Femmes étrangères","Moins de 15 ans étrangers",
                "15-24 ans étrangers","25-54 ans étrangers","55 ans et plus étrangers"]

    infos = {
        'intitule': colonnes,
        'donnee' : [df_demo[df_demo['ville'] == ville_choisie][colonne].iloc[0] for colonne in colonnes]
    }
    table = pd.DataFrame(infos)
    data = table.to_dict('rows')

    entete = [{'id': 'intitule', 'name': '   '}, {'id': 'donnee', 'name' : ville_choisie.split('(')[0].strip()}]

    return data, entete

# Camembert HF immigres
@app.callback(Output("repartitions_immigres_HF","figure"), [Input('ville-picker','value')])
def repartition_HF_Etrangers(ville_choisie):
    nb_hommes = df_demo[df_demo['ville'] == ville_choisie]["Hommes immigrés"].iloc[0]
    nb_femmes = df_demo[df_demo['ville'] == ville_choisie]["Femmes immigrées"].iloc[0]

    labels = ["Hommes immigrés","Femmes immigrées"]
    values = [float(nb_hommes), float(nb_femmes)]
    total = sum(values)

    traces = [
        go.Pie(labels= labels, values= values)
    ]

    return {
        'data': traces,
        'layout': go.Layout(
            title= "Population Immigree | Hommes/Femmes<br> (Total: " + str(total) + ")",
            legend_orientation= 'h'
        )
    }

# Camembert Ages immigres
@app.callback(Output('repartitions_immigres_ages','figure'), [Input('ville-picker','value')])
def repartition_ages(ville_choisie):
    colonnes = ["Moins de 15 ans immigrés","15-24 ans immigrés","25-54 ans immigrés","55 ans et plus immigrés"]

    labels = colonnes
    values = [float(df_demo[df_demo['ville'] == ville_choisie][colonne].iloc[0]) for colonne in colonnes]
    total = sum(values)

    traces = [
        go.Pie(labels= labels, values= values)
    ]

    return {
        'data': traces,
        'layout': go.Layout(
            title= "Répartition par Tranches d'ages des Immigres<br> (Total: " + str(total) + ")",
            legend_orientation= 'h'
        )
    }

# Tableau repartitions immigres
@app.callback([Output('tableau_immigres','data'), Output('tableau_immigres','columns')], [Input('ville-picker','value')])
def table_repartitions(ville_choisie):
    colonnes = ["Hommes immigrés","Femmes immigrées","Moins de 15 ans immigrés","15-24 ans immigrés",
                "25-54 ans immigrés","55 ans et plus immigrés"]

    infos = {
        'intitule': colonnes,
        'donnee' : [df_demo[df_demo['ville'] == ville_choisie][colonne].iloc[0] for colonne in colonnes]
    }
    table = pd.DataFrame(infos)
    data = table.to_dict('rows')

    entete = [{'id': 'intitule', 'name': '   '}, {'id': 'donnee', 'name' : ville_choisie.split('(')[0].strip()}]

    return data, entete


############ SANTE ET SOCIAL ############
# Afficher le camembert des praticiens
@app.callback(Output('praticiens','figure'), [Input('ville-picker','value')])
def praticiens(ville_choisie):
    colonnes = ["Médecins généralistes", "Masseurs-kinésithérapeutes", "Dentistes", "Infirmiers",
                "Spécialistes ORL", "Ophtalmologistes", "Dermatologues", "Sage-femmes", "Pédiatres", "Gynécologues", 
                "Pharmacies", "Ambulances"]
    labels = colonnes
    values = [
        float(df_sante[df_sante['ville'] == ville_choisie][colonne].iloc[0]) for colonne in colonnes
    ]
    total = sum(values)

    trace = [
        go.Pie(
            labels= labels, values= values
        )
    ]

    return {
        'data': trace,
        'layout': go.Layout(
            title= "Praticiens à " + ville_choisie.split('(')[0].strip() + "<br> (Total: " + str(total) + ")",
        )
    }

# Afficher le tableau des praticiens
@app.callback([Output('tableau_praticiens','data'), Output('tableau_praticiens','columns')], [Input('ville-picker','value')])
def tableau_praticiens(ville_choisie):
    colonnes = ["Médecins généralistes", "Masseurs-kinésithérapeutes", "Dentistes", "Infirmiers",
                "Spécialistes ORL", "Ophtalmologistes", "Dermatologues", "Sage-femmes", "Pédiatres", "Gynécologues", 
                "Pharmacies", "Ambulances"]

    infos = {
        'intitule' : colonnes,
        'donnee': [df_sante[df_sante['ville'] == ville_choisie][colonne].iloc[0] for colonne in colonnes]
    }

    table = pd.DataFrame(infos)
    data = table.to_dict('rows')

    entete = [{'id':'intitule', 'name': "    "},{'id':'donnee', 'name': ville_choisie.split('(')[0].strip()}]

    return data, entete

# Afficher le camembert des etablissements de sante
@app.callback(Output('etablissements','figure'), [Input('ville-picker','value')])
def etablissements(ville_choisie):
    colonnes = ["Urgences", "Etablissements de santé de court séjour", "Etablissements de santé de moyen séjour", 
                "Etablissements de santé de long séjour", "Etablissement d'accueil du jeune enfant",
                "Maisons de retraite", "Etablissements pour enfants handicapés", "Etablissements pour adultes handicapés"]
    labels = colonnes
    values = [
        float(df_sante[df_sante['ville'] == ville_choisie][colonne].iloc[0]) for colonne in colonnes
    ]
    total = sum(values)

    trace = [
        go.Pie(
            labels= labels, values= values
        )
    ]

    return {
        'data': trace,
        'layout': go.Layout(
            title= "Etalissements de Sante à " + ville_choisie.split('(')[0].strip() + "<br> (Total: " + str(total) + ")",
        )
    }

# Afficher le tableau des etablissements
@app.callback([Output('tableau_etablissements','data'), Output('tableau_etablissements','columns')], [Input('ville-picker','value')])
def tableau_etablissements(ville_choisie):
    colonnes = ["Urgences", "Etablissements de santé de court séjour", "Etablissements de santé de moyen séjour", 
                "Etablissements de santé de long séjour", "Etablissement d'accueil du jeune enfant",
                "Maisons de retraite", "Etablissements pour enfants handicapés", "Etablissements pour adultes handicapés"]

    infos = {
        'intitule' : colonnes,
        'donnee': [df_sante[df_sante['ville'] == ville_choisie][colonne].iloc[0] for colonne in colonnes]
    }

    table = pd.DataFrame(infos)
    data = table.to_dict('rows')

    entete = [{'id':'intitule', 'name': "    "},{'id':'donnee', 'name': ville_choisie.split('(')[0].strip()}]

    return data, entete

# Graphes evolutions caf, apl, rsa, alloc
@app.callback([Output('caf','figure'), Output('rsa','figure'), Output('apl','figure'), Output('alloc','figure')], [Input('ville-picker','value')])
def evolution_presstations_sociales(ville_choisie):
    x_axis = np.array(range(2009,2018))
    y_caf = [
        df_sante[df_sante['ville'] == ville_choisie]["nbre allocataires (" + str(a) + ")"].iloc[0] for a in range(2009,2018)
    ]
    y_rsa = [
        df_sante[df_sante['ville'] == ville_choisie]["nbre RSA (" + str(a) + ")"].iloc[0] for a in range(2009,2018)
    ]
    y_apl = [
        df_sante[df_sante['ville'] == ville_choisie]["nbre APL (" + str(a) + ")"].iloc[0] for a in range(2009,2018)
    ]
    y_alloc = [
        df_sante[df_sante['ville'] == ville_choisie]["nbre Alloc Familiales (" + str(a) + ")"].iloc[0] for a in range(2009,2018)
    ]

    traceCAF = [
        go.Scatter(
            x=x_axis,
            y=y_caf,
            mode='lines+markers',
            line= {'shape':'spline','smoothing':1}
        )
    ]

    traceRSA = [
        go.Scatter(
            x=x_axis,
            y=y_rsa,
            mode='lines+markers',
            line= {'shape':'spline','smoothing':1}
        )
    ]

    traceAPL = [
        go.Scatter(
            x=x_axis,
            y=y_apl,
            mode='lines+markers',
            line= {'shape':'spline','smoothing':1}
        )
    ]

    traceAlloc = [
        go.Scatter(
            x=x_axis,
            y=y_alloc,
            mode='lines+markers',
            line= {'shape':'spline','smoothing':1}
        )
    ]

    figCAF = {
        'data': traceCAF,
        'layout': go.Layout(
            title= "Évolution du Nombre d'Allocataires à " + ville_choisie.split('(')[0].strip(),
            xaxis= {'title': ''},
            yaxis= dict(title= "Nombre de bénéficiaires"),
            hovermode='closest'
        )
    }
    figRSA = {
        'data': traceRSA,
        'layout': go.Layout(
            title= "Évolution du Nombre de bénéficiaires du RSA à " + ville_choisie.split('(')[0].strip(),
            xaxis= {'title': ''},
            yaxis= dict(title= "Nombre de bénéficiaires"),
            hovermode='closest'
        )
    }
    figAPL = {
        'data': traceAPL,
        'layout': go.Layout(
            title= "Évolution du Nombre de bénéficiaires de l'APL à " + ville_choisie.split('(')[0].strip(),
            xaxis= {'title': ''},
            yaxis= dict(title= "Nombre de bénéficiaires"),
            hovermode='closest'
        )
    }
    figAlloc = {
        'data': traceAlloc,
        'layout': go.Layout(
            title= "Évolution du Nombre de bénéficiaires des Allocations Familiales à " + ville_choisie.split('(')[0].strip(),
            xaxis= {'title': ''},
            yaxis= dict(title= "Nombre de bénéficiaires"),
            hovermode='closest'
        )
    }

    return figCAF, figRSA, figAPL, figAlloc

########### ONGLET IMMOBILIER ################
# Afficher les informations
@app.callback([Output('prixM2','children'), Output('prix_moyen','children'),
                Output("nb_logements",'children')],
                [Input('ville-picker','value')])
def infos_immo(ville_choisie):
    prixM2 = int(df_immo[df_immo['ville'] == ville_choisie]['prix_m2'].iloc[0])
    # prixMoyen = df_immo['prix_m2'].mean()
    prixMoyen = "1 252 €/m²"
    nbLogements = int(df_immo[df_immo['ville'] == ville_choisie]["Nombre de logements"].iloc[0])

    prixM2 = str(prixM2) + " €/m²"
    nbLogements = "Nombre de Logements: " + str(nbLogements)

    return prixM2, prixMoyen, nbLogements


# Afficher le camembert types de residences
@app.callback([Output("residences",'figure'), Output("maisons_apparts",'figure'),
                Output("types",'figure'), Output("pieces",'figure')], [Input('ville-picker','value')])
def camemberts_immo(ville_choisie):
    labels_residences = ['Résidences principales','Résidences secondaires', 'Logements vacants']
    labels_maisons_apparts = ['Maisons','Appartements', 'Autres types de logements']
    labels_types = ['Propriétaires', 'Locataires', '- dont locataires HLM',"Locataires hébergés à titre gratuit"]
    labels_pieces = ['Studios', '2 pièces', '3 pièces', '4 pièces', '5 pièces et plus']

    values_residences = [int(df_immo[df_immo["ville"] == ville_choisie][colonne]) for colonne in labels_residences]
    values_maisons_apparts = [int(df_immo[df_immo["ville"] == ville_choisie][colonne]) for colonne in labels_maisons_apparts]
    values_types = [int(df_immo[df_immo["ville"] == ville_choisie][colonne]) for colonne in labels_types]
    values_pieces = [int(df_immo[df_immo["ville"] == ville_choisie][colonne]) for colonne in labels_pieces]

    total_residences = sum(values_residences)
    total_maisons_apparts = sum(values_maisons_apparts)
    total_types = sum(values_types)
    total_pieces = sum(values_pieces)

    trace1 = [go.Pie(labels= labels_residences, values= values_residences)]
    trace2 = [go.Pie(labels= labels_maisons_apparts, values= values_maisons_apparts)]
    trace3 = [go.Pie(labels= labels_types, values= values_types)]
    trace4 = [go.Pie(labels= labels_pieces, values= values_pieces)]

    fig1 = {'data': trace1, 'layout': go.Layout(title= "Residences principales et secondaires<br> à " + ville_choisie.split('(')[0].strip() + " (Total: " + str(total_residences) + ")",)}
    fig2 = {'data': trace2, 'layout': go.Layout(title= "Maisons et appartements<br> à " + ville_choisie.split('(')[0].strip() + " (Total: " + str(total_maisons_apparts) + ")",)}
    fig3 = {'data': trace3, 'layout': go.Layout(title= "Logements par types d'occupants<br> à " + ville_choisie.split('(')[0].strip() + " (Total: " + str(total_types) + ")",)}
    fig4 = {'data': trace4, 'layout': go.Layout(title= "Répartitions par Nombre de pieces<br> à " + ville_choisie.split('(')[0].strip() + " (Total: " + str(total_pieces) + ")",)}

    return fig1, fig2, fig3, fig4

# Afficher notre tableau
@app.callback([Output("tableau_immo",'data'), Output('tableau_immo','columns')],
                [Input('ville-picker','value')])
def tableau_immo(ville_choisie):
    colonnes = ['Résidences principales',
            'Résidences secondaires', 'Logements vacants', 'Maisons','Appartements', 'Autres types de logements', 'Propriétaires',
            'Locataires', '- dont locataires HLM',"Locataires hébergés à titre gratuit", 'Studios', '2 pièces',
            '3 pièces', '4 pièces', '5 pièces et plus']

    infos = {
        'intitule': colonnes,
        'donnee': [df_immo[df_immo["ville"] == ville_choisie][colonne] for colonne in colonnes]
    }

    table = pd.DataFrame(infos)
    data = table.to_dict("rows")

    entete = [{'id':'intitule', 'name': "    "},{'id':'donnee', 'name': ville_choisie.split('(')[0].strip()}]

    return data, entete


############ Entreprises   #############
# Tableau de donnees
@app.callback([Output('entreprises_table', 'data'), Output('entreprises_table', 'columns')], [Input('ville-picker', 'value')])
def entreprises_infos(ville_choisie):
    liste_infos = ['Commerces', 'Services aux particuliers',
       'Services publics', 'Epiceries', 'Boulangeries',
       'Boucheries, charcuteries', 'Librairies, papeteries, journaux',
       'Drogueries et quincalleries', 'Banques', 'Bureaux de Poste',
       'Garages, réparation automobile', 'Electriciens', 'Grandes surfaces',
       'Commerces spécialisés alimentaires',
       'Commerces spécialisés non alimentaires', 'Services généraux',
       'Services automobiles', 'Services du bâtiment', 'Autres services']

    infos = {'intitule' : liste_infos,
                'donnee' : [int(df_entreprises[df_entreprises['ville'] == ville_choisie][colonne].iloc[0]) for colonne in liste_infos]}

    table = pd.DataFrame(infos)
    data = table.to_dict('rows')

    entete = [{'id': "intitule", 'name': "   "}, {'id': "donnee", 'name': ville_choisie.split('(')[0].strip()}]

    return data, entete

# Graphe Nombre d'entreprises
@app.callback(Output('nb_entreprises', 'figure'),
                [Input('ville-picker','value')])
def nb_entreprises(ville_choisie):
    x_axis = np.array(range(2005,2016))
    y_axis = [df_entreprises[df_entreprises['ville']== ville_choisie][str(annee) + " (nbre d'entreprises)"].iloc[0] for annee in range(2005,2016)]

    traces = []
    traces.append(go.Scatter(
        x= x_axis, 
        y= y_axis, 
        mode='lines+markers', 
        line={'shape' : 'spline', 'smoothing' : 1} ,
        name=ville_choisie
        ))

    return {'data':traces,
            'layout': go.Layout(
                title = "Évolution du nombre d'entreprises<br> à " + ville_choisie.split('(')[0].strip(),
                xaxis = {'title':''},
                yaxis = dict(title = "Nombre d'entreprises"),
                hovermode = 'closest'
            )}

# Graphe Creations d'entreprises
@app.callback(Output('nb_creations', 'figure'),
                [Input('ville-picker','value')])
def nb_creations(ville_choisie):
    x_axis = np.array(range(2005,2016))
    y_axis = [df_entreprises[df_entreprises['ville']== ville_choisie][str(annee) + ' (nbre de creations)'].iloc[0] for annee in range(2005,2016)]

    traces = []
    traces.append(go.Scatter(
        x= x_axis, 
        y= y_axis, 
        mode='lines+markers', 
        line={'shape' : 'spline', 'smoothing' : 1} ,
        name=ville_choisie
        ))

    return {'data':traces,
            'layout': go.Layout(
                title = "Créations d'entreprises<br> à " + ville_choisie.split('(')[0].strip(),
                xaxis = {'title':''},
                yaxis = dict(title = "Nombre de créations d'entreprises"),
                hovermode = 'closest'
            )}


############# EMPLOI  ##################
# Afficher l'evolution du taux de chomage
@app.callback(Output("evolution_chomage",'figure'), [Input('ville-picker','value')])
def evolution_chomage(ville_choisie):
    x_axis = np.array(range(2004,2017))
    y_axis = [df_chomage[df_chomage['ville'] == ville_choisie][str(annee)].iloc[0] for annee in range(2004,2017)]
    y_mean = [df_chomage[str(annee)].mean() for annee in range(2004,2017)]

    traces = [
        go.Scatter(x= x_axis, y= y_axis, mode= 'lines+markers', 
                    line= {'shape':'spline', 'smoothing':1}, name="Taux de chomage à " + ville_choisie.split('(')[0].strip()),
        go.Scatter(x= x_axis, y= y_mean, mode= 'lines+markers', 
                    line= {'shape':'spline', 'smoothing':1}, name= "Moyenne de France")
    ]

    return {
        'data' : traces,
        'layout': go.Layout(
            title= "Évolution du taux de chomage<br> à " + ville_choisie.split('(')[0].strip(),
            xaxis= {'title' : ""},
            yaxis= dict(title= "% de la population"),
            hovermode= "closest"
        )
    }

# Afficher Emploi Hommes/femmes
@app.callback(Output("emploi_HF", 'figure'), [Input('ville-picker','value')])
def emploi_HF(ville_choisie):
    colonnes_h = ['Part des actifs hommes (%)', "Taux d'activité hommes (%)", "Taux d'emploi hommes (%)", 'Taux de chômage hommes (%)']
    colonnes_f = ['Part des actifs femmes (%)', "Taux d'activité femmes (%)", "Taux d'emploi femmes (%)", 'Taux de chômage femmes (%)']

    traces = [
        go.Bar(x= ["Part des actifs", "Taux d'activité", "Taux d'emploi", "Taux de chômage"],
                y= [df_emploi[df_emploi['ville'] == ville_choisie][colonne].iloc[0] for colonne in colonnes_h],
                name= "Hommes"
                ),
        go.Bar(x= ["Part des actifs", "Taux d'activité", "Taux d'emploi", "Taux de chômage"],
                y= [df_emploi[df_emploi['ville'] == ville_choisie][colonne].iloc[0] for colonne in colonnes_f],
                name= "Femmes"
                )
    ]

    return {
        'data': traces,
        'layout': go.Layout(
            title= "Emploi, activite et chomage<br> des hommes et des femmes (en %) à " + ville_choisie.split('(')[0].strip()
        )
    }

# Tableau emploi HF
@app.callback([Output("table_emploi_HF",'data'), Output('table_emploi_HF','columns')],
                [Input('ville-picker','value')])
def tableau_emploi_HF(ville_choisie):
    colonnes_h = ['Part des actifs hommes (%)', "Taux d'activité hommes (%)", "Taux d'emploi hommes (%)", 'Taux de chômage hommes (%)']
    colonnes_f = ['Part des actifs femmes (%)', "Taux d'activité femmes (%)", "Taux d'emploi femmes (%)", 'Taux de chômage femmes (%)']

    infos = {
        'intitule': ["Part des actifs", "Taux d'activité", "Taux d'emploi", "Taux de chômage"],
        'hommes': [df_emploi[df_emploi["ville"] == ville_choisie][colonne] for colonne in colonnes_h],
        'femmes': [df_emploi[df_emploi["ville"] == ville_choisie][colonne] for colonne in colonnes_f]
    }

    table = pd.DataFrame(infos)
    data = table.to_dict("rows")

    entete = [{'id':'intitule', 'name': "    "},{'id':'hommes', 'name': "% Hommes"}, {'id':'femmes', 'name': "% Femmes"}]

    return data, entete

# Emploi Ages
@app.callback(Output("emploi_ages", 'figure'), [Input('ville-picker','value')])
def emploi_ages(ville_choisie):
    colonnes_1524 = ['Part des actifs 15-24 ans (%)',  "Taux d'emploi 15-24 ans (%)", 'Taux de chômage 15-24 ans (%)']
    colonnes_2554 = ['Part des actifs 25-54 ans (%)', "Taux d'emploi 25-54 ans (%)", 'Taux de chômage 25-54 ans (%)']
    colonnes_5564 = ['Part des actifs 55-64 ans (%)', "Taux d'emploi 55-64 ans (%)", 'Taux de chômage 55-64 ans (%)']

    traces = [
        go.Bar(x= ["Part des actifs", "Taux d'emploi", "Taux de chômage"],
                y= [df_emploi[df_emploi['ville'] == ville_choisie][colonne].iloc[0] for colonne in colonnes_1524],
                name= "15-24 ans"
                ),
        go.Bar(x= ["Part des actifs", "Taux d'emploi", "Taux de chômage"],
                y= [df_emploi[df_emploi['ville'] == ville_choisie][colonne].iloc[0] for colonne in colonnes_2554],
                name= "25-54 ans"
                ),
        go.Bar(x= ["Part des actifs", "Taux d'emploi", "Taux de chômage"],
                y= [df_emploi[df_emploi['ville'] == ville_choisie][colonne].iloc[0] for colonne in colonnes_5564],
                name= "25-54 ans"
                )
    ]

    return {
        'data': traces,
        'layout': go.Layout(
            title= "Emploi, activite et chomage<br> des hommes et des femmes (en %) à " + ville_choisie.split('(')[0].strip()
        )
    }

# Tableau des ages
@app.callback([Output("table_emploi_ages",'data'), Output('table_emploi_ages','columns')],
                [Input('ville-picker','value')])
def tableau_emploi_ages(ville_choisie):
    colonnes_1524 = ['Part des actifs 15-24 ans (%)',  "Taux d'emploi 15-24 ans (%)", 'Taux de chômage 15-24 ans (%)']
    colonnes_2554 = ['Part des actifs 25-54 ans (%)', "Taux d'emploi 25-54 ans (%)", 'Taux de chômage 25-54 ans (%)']
    colonnes_5564 = ['Part des actifs 55-64 ans (%)', "Taux d'emploi 55-64 ans (%)", 'Taux de chômage 55-64 ans (%)']

    infos = {
        'intitule': ["Part des actifs", "Taux d'emploi", "Taux de chômage"],
        '1524': [df_emploi[df_emploi["ville"] == ville_choisie][colonne] for colonne in colonnes_1524],
        '2554': [df_emploi[df_emploi["ville"] == ville_choisie][colonne] for colonne in colonnes_2554],
        '5564': [df_emploi[df_emploi["ville"] == ville_choisie][colonne] for colonne in colonnes_5564],
    }

    table = pd.DataFrame(infos)
    data = table.to_dict("rows")

    entete = [{'id':'intitule', 'name': "    "},{'id':'1524', 'name': "% des 15-24 ans"}, 
                {'id':'2554', 'name': "% des 25-54 ans"}, {'id':'5564', 'name': "% des 55-64 ans"}]

    return data, entete


# Camemberts 
@app.callback([Output("parts_actifs",'figure'), Output("salaries",'figure'),
                Output("salaries_partiels_HF",'figure'), Output("salaries_partiels_ages",'figure')], [Input('ville-picker','value')])
def camemberts_emploi(ville_choisie):
    labels_actifs = ['Retraités et pré-retraités de 15-64 ans','Stagiaires et étudiants de 15-64 ans','Autres personnes sans activité de 15-64 ans']
    labels_salaires = ['CDI et fonction publique', 'CDD', 'Intérimaires', 'Emplois aidés', 'Stages et apprentissages']
    labels_partiels_hf = ['Hommes à temps partiel', 'Femmes à temps partiel']
    labels_partiels_ages = ['Les 15 à 24 ans à temps partiel', 'Les 25 à 54 ans à temps partiel', 'Les 55 à 64 ans à temps partiel']

    values_actifs = [int(df_emploi[df_emploi["ville"] == ville_choisie][colonne]) for colonne in labels_actifs]
    values_salaires = [int(df_emploi[df_emploi["ville"] == ville_choisie][colonne]) for colonne in labels_salaires]
    values_partiels_hf = [int(df_emploi[df_emploi["ville"] == ville_choisie][colonne]) for colonne in labels_partiels_hf]
    values_partiels_ages = [int(df_emploi[df_emploi["ville"] == ville_choisie][colonne]) for colonne in labels_partiels_ages]

    total_actifs = sum(values_actifs)
    total_salaires = sum(values_salaires)
    total_partiels_hf = sum(values_partiels_hf)
    total_partiels_ages = sum(values_partiels_ages)

    trace1 = [go.Pie(labels= labels_actifs, values= values_actifs)]
    trace2 = [go.Pie(labels= labels_salaires, values= values_salaires)]
    trace3 = [go.Pie(labels= labels_partiels_hf, values= values_partiels_hf)]
    trace4 = [go.Pie(labels= labels_partiels_ages, values= values_partiels_ages)]

    fig1 = {'data': trace1, 'layout': go.Layout(title= "Retraites, etudiants et autres inactifs<br> à " + ville_choisie.split('(')[0].strip() + " (Total: " + str(total_actifs) + ")",)}
    fig2 = {'data': trace2, 'layout': go.Layout(title= "Les salaries<br> à " + ville_choisie.split('(')[0].strip() + " (Total: " + str(total_salaires) + ")",)}
    fig3 = {'data': trace3, 'layout': go.Layout(title= "Répartitions Temps Partiels | Hommes-Femmes<br> à " + ville_choisie.split('(')[0].strip() + " (Total: " + str(total_partiels_hf) + ")",)}
    fig4 = {'data': trace4, 'layout': go.Layout(title= "Répartitions Temps Partiels | Par Tranches d'ages<br> à " + ville_choisie.split('(')[0].strip() + " (Total: " + str(total_partiels_ages) + ")",)}

    return fig1, fig2, fig3, fig4

# Tableau parts_actifs
@app.callback([Output("table_parts_actifs",'data'), Output('table_parts_actifs','columns')],
                [Input('ville-picker','value')])
def tableau_parts_actifs(ville_choisie):
    colonnes =  ['Retraités et pré-retraités de 15-64 ans','Stagiaires et étudiants de 15-64 ans','Autres personnes sans activité de 15-64 ans']
 
    infos = {
        'intitule': colonnes,
        'donnee': [df_emploi[df_emploi["ville"] == ville_choisie][colonne] for colonne in colonnes]
    }

    table = pd.DataFrame(infos)
    data = table.to_dict("rows")

    entete = [{'id':'intitule', 'name': "    "},{'id':'donnee', 'name': ville_choisie.split('(')[0].strip()}]

    return data, entete

# Tableau salaries
@app.callback([Output("table_salaries",'data'), Output('table_salaries','columns')],
                [Input('ville-picker','value')])
def tableau_salaries(ville_choisie):
    colonnes =  ['CDI et fonction publique', 'CDD', 'Intérimaires', 'Emplois aidés', 'Stages et apprentissages']
 
    infos = {
        'intitule': colonnes,
        'donnee': [df_emploi[df_emploi["ville"] == ville_choisie][colonne] for colonne in colonnes]
    }

    table = pd.DataFrame(infos)
    data = table.to_dict("rows")

    entete = [{'id':'intitule', 'name': "    "},{'id':'donnee', 'name': ville_choisie.split('(')[0].strip()}]

    return data, entete

# Tableau Partiels Hommes Femmes
@app.callback([Output("table_salaires_partiels",'data'), Output('table_salaires_partiels','columns')],
                [Input('ville-picker','value')])
def tableau_partiels_HF(ville_choisie):
    colonnes = ['Salariés à temps partiel','Hommes à temps partiel', 'Femmes à temps partiel',
                'Les 15 à 24 ans à temps partiel', 'Les 25 à 54 ans à temps partiel', 'Les 55 à 64 ans à temps partiel']
 
    infos = {
        'intitule': colonnes,
        'donnee': [df_emploi[df_emploi["ville"] == ville_choisie][colonne] for colonne in colonnes]
    }

    table = pd.DataFrame(infos)
    data = table.to_dict("rows")

    entete = [{'id':'intitule', 'name': "    "},{'id':'donnee', 'name': ville_choisie.split('(')[0].strip()}]

    return data, entete


############ SALAIRES    #################
# Afficher le graphe
@app.callback(Output('graph', 'figure'),
                [Input('ville-picker','value')])
def update_figure(ville_choisie):
    x_axis = np.array(range(2006,2016))
    y_axis = [df_salaires[df_salaires['ville']== ville_choisie][str(annee)].iloc[0] for annee in range(2006,2016)]
    y_axis_mean = [df_salaires[str(annee)].mean() for annee in range(2006,2016)]

    traces = []

    traces.append(go.Scatter(
        x= x_axis, 
        y= y_axis, 
        mode='lines+markers', 
        line={'shape' : 'spline', 'smoothing' : 1} ,
        name='Salaire moyen de '+ ville_choisie.split('(')[0].strip()
        ))
    traces.append(go.Scatter(
        x=x_axis, 
        y=y_axis_mean, 
        mode='lines+markers', 
        line={'shape' : 'spline', 'smoothing' : 1}, 
        name='Moyenne France'
        ))

    return {'data':traces,
            'layout': go.Layout(
                title = 'Evolution des Salaires<br> à ' + ville_choisie.split('(')[0].strip(),
                xaxis = {'title':''},
                yaxis = dict(title = 'Montant par mois (Eur)'),
                hovermode = 'closest'
            )}

# Afficher les Donnees
@app.callback([Output('table_salaires', 'data'), Output('table_salaires','columns')],
                [Input('ville-picker','value')])
def update_salaires(ville_choisie):
    colonnes = [
        "Salaire moyen des cadres" , "Salaire moyen des professions intermédiaires", "Salaire moyen des employés",
        "Salaire moyen des ouvriers", "Salaire moyen des femmes", "Salaire moyen des hommes", "Salaire moyen des moins de 26 ans",
        "Salaire moyen des 26-49 ans", "Salaire moyen des 50 ans et plus", "Revenu mensuel moyen par foyer fiscal",
        "Nombre de foyers fiscaux"
    ]

    infos = {'intitule': colonnes,
                'donnee': [int(df_salaires[df_salaires['ville'] == ville_choisie][colonne].iloc[0]) for colonne in colonnes]}
    
    table = pd.DataFrame(infos)
    data = table.to_dict('rows')

    entete = [{'id': "intitule", 'name': "    "}, {'id': "donnee", 'name': ville_choisie.split('(')[0].strip()}]
    
    return data, entete

############ CSP #########################

@app.callback(Output('metiers_pie', 'figure'), [Input('ville-picker','value')])
def graph_metiers_pie(ville_choisie):
    labels_metiers = ['Agriculteurs exploitants',
       "Artisans, commerçants, chefs d'entreprise",
       'Cadres et professions intellectuelles supérieures',
       'Professions intermédiaires', 'Employés', 'Ouvriers']

    values_metiers = [
        int(df_csp[df_csp['ville'] == ville_choisie][colonne].iloc[0]) for colonne in labels_metiers
    ]

    total = sum(values_metiers)

    trace = [go.Pie(labels=labels_metiers, values=values_metiers)]

    return {"data": trace,
            'layout': go.Layout(
                            title='Catégorie SocioProfessionnels<br> à ' + ville_choisie.split('(')[0].strip() + ' (Total: ' + str(total) + ')'
                        )}

@app.callback(Output('diplomes_pie', 'figure'), [Input('ville-picker','value')])
def graph_diplomes_pie(ville_choisie):
    labels_diplomes = ['Aucun diplôme',
       'CAP / BEP ', 'Baccalauréat / brevet professionnel',
       "Diplôme de l'enseignement supérieur"]

    values_diplomes = [
        int(df_csp[df_csp['ville'] == ville_choisie][colonne].iloc[0]) for colonne in labels_diplomes
    ]

    total = sum(values_diplomes)

    trace = [go.Pie(labels=labels_diplomes, values=values_diplomes)]

    return {"data": trace,
            'layout': go.Layout(
                            title='Niveau de diplôme à ' + ville_choisie.split('(')[0].strip() + '<br> (Total: ' + str(total) + ')'
                        )}

@app.callback(Output('diplomes_hf_bar', 'figure'), [Input('ville-picker','value')])
def graph_diplomes_hf_bar(ville_choisie):
    diplomes_hommes = ['Aucun diplôme (%) hommes', 'CAP / BEP  (%) hommes',
       'Baccalauréat / brevet professionnel (%) hommes',
       "Diplôme de l'enseignement supérieur (%) hommes",]
    diplomes_femmes = ['Aucun diplôme (%) femmes', 'CAP / BEP  (%) femmes',
       'Baccalauréat / brevet professionnel (%) femmes',
       "Diplôme de l'enseignement supérieur (%) femmes"]

    traces = [go.Bar(x= ['Aucun Diplome', 'CAP/BEP','BAC/BAC Pro', 'Diplôme Universitaire'],
                    y=[df_csp[df_csp['ville'] == ville_choisie][i].iloc[0] for i in diplomes_hommes],
                    name="Hommes"),
                go.Bar(x=['Aucun Diplome', 'CAP/BEP','BAC/BAC Pro', 'Diplôme Universitaire'],
                        y=[df_csp[df_csp['ville'] == ville_choisie][i].iloc[0] for i in diplomes_femmes],
                        name="Femmes")]

    return {"data": traces,
            'layout': go.Layout(
                            title='Répartition Hommes | Femmes (en %)<br> à ' + ville_choisie.split('(')[0].strip() ,
                            barmode='stack'
                        )}

@app.callback([Output('diplomes_table', 'data'), Output('diplomes_table', 'columns')], [Input('ville-picker', 'value')])
def diplomes_table(ville_choisie):
    colonnes = [
        'Agriculteurs exploitants', "Artisans, commerçants, chefs d'entreprise", 'Cadres et professions intellectuelles supérieures',
        'Professions intermédiaires', 'Employés', 'Ouvriers',
        'Aucun diplôme', 'CAP / BEP ', 'Baccalauréat / brevet professionnel', "Diplôme de l'enseignement supérieur"
    ]

    infos = {'intitule' : colonnes,
            'donnee' : [
                int(df_csp[df_csp['ville'] == ville_choisie][colonne].iloc[0]) for colonne in colonnes
            ]}
    table = pd.DataFrame(infos)
    data = table.to_dict('rows')

    entete = [{'id': "intitule", 'name': "    "}, {'id': "donnee", 'name': ville_choisie.split('(')[0].strip()}]

    return data, entete

############ AUTO  #######################

@app.callback([Output('total_voitures','children'),Output('total_accidents', 'children'),Output('total_stationnement','children')],
                [Input('ville-picker','value')])
def display_automobiles(ville_choisie):
    colonnes = ['total de voitures', "Nombre total d'accidents", 'Ménages avec place(s) de stationnement']

    donnees = [
        int(df_auto[df_auto['ville'] == ville_choisie][colonne].iloc[0]) for colonne in colonnes
    ]

    return str(donnees[0]), str(donnees[1]), str(donnees[2])

@app.callback(Output('voitures_pie','figure'), [Input('ville-picker','value')])
def nbre_voitures_pie(ville_choisie):
    labels_voitures = ['Ménages sans voiture', 'Ménages avec une voiture', 'Ménages avec deux voitures ou plus']
    values_voitures = [
        df_auto[df_auto['ville'] == ville_choisie][colonne].iloc[0] for colonne in labels_voitures
    ]

    traces = [
        go.Pie(labels=labels_voitures, values=values_voitures)
    ]

    return {
        'data': traces,
        'layout': go.Layout(title="Nombre de voitures par ménage<br> à " + ville_choisie.split('(')[0].strip())
    }

@app.callback(Output('accidents_pie', 'figure'), [Input('ville-picker', 'value')])
def nbre_accidents_pie(ville_choisie):
    colonnes = ['Nombre de personnes tuées', 'Nombre de personnes indemnes',' - dont blessés graves', ' - dont blessés légers']
    labels_accidents = ['Personnes tuées', 'Personnes indemnes','Blessées graves', 'Blessées légers' ]

    values_accidents = [
        int(df_auto[df_auto['ville'] == ville_choisie][colonne].iloc[0]) for colonne in colonnes
    ]

    traces = [
        go.Pie(labels=labels_accidents, values=values_accidents)
    ]

    return {
        'data' : traces,
        'layout' : go.Layout(title="Accidents de la route<br> à " + ville_choisie.split('(')[0].strip())
    }


############ DELINQUANCE #################

@app.callback(Output('delinquance1_pie','figure'), [Input('ville-picker', 'value')])
def delinquance1_pie(ville_choisie):
    labels_delinquance = ['Violences aux personnes', 'Vols et dégradations', 'Délinquance économique et financière', 
                            'Autres crimes et délits']
    values_delinquance = [
        int(df_del[df_del['ville'] == ville_choisie][i].iloc[0]) for i in labels_delinquance
    ]

    total = sum(values_delinquance)

    traces = [go.Pie(labels=labels_delinquance, values=values_delinquance)]

    return {'data' : traces,
            'layout' : go.Layout(title="Principaux crimes et délits<br> à " + ville_choisie.split('(')[0].strip() + ' (' + str(total) + ')')}

@app.callback(Output('violence_pie','figure'), [Input('ville-picker', 'value')])
def violence_pie(ville_choisie):
    labels_violence = ['Violences gratuites', 'Violences crapuleuses', 'Violences sexuelles', 
    'Menaces de violence', 'Atteintes à la dignité', ]

    values_violence = [
        int(df_del[df_del['ville'] == ville_choisie][i].iloc[0]) for i in labels_violence
    ]

    total = sum(values_violence)

    traces = [go.Pie(labels=labels_violence, values=values_violence)]

    return {'data' : traces,
            'layout' : go.Layout(title="Agressions et violences aux personnes<br> à " + ville_choisie.split('(')[0].strip() + ' (' + str(total) + ')')}

@app.callback(Output('vols_pie','figure'), [Input('ville-picker', 'value')])
def vols_pie(ville_choisie):
    labels_vols = ['Cambriolages', 'Vols à main armée (arme à feu)', 'Vols avec entrée par ruse',
    "Vols liés à l'automobile", 'Vols de particuliers', "Vols d'entreprises", 'Violation de domicile',
    'Destruction et dégradations de biens']

    values_vols = [
        int(df_del[df_del['ville'] == ville_choisie][i].iloc[0]) for i in labels_vols
    ]

    total = sum(values_vols)

    traces = [go.Pie(labels=labels_vols, values=values_vols)]

    return {'data' : traces,
            'layout' : go.Layout(title="Vols et dégradations<br> à " + ville_choisie.split('(')[0].strip() + ' (' + str(total) + ')')}

@app.callback([Output('delinquance_table','data'), Output('delinquance_table', 'columns')], [Input('ville-picker','value')])
def delinquance_infos(ville_choisie):
    colonnes = [
        'Violences aux personnes', 'Vols et dégradations', 'Délinquance économique et financière', 'Autres crimes et délits',
        'Violences gratuites', 'Violences crapuleuses', 'Violences sexuelles', 'Menaces de violence', 'Atteintes à la dignité',
        'Cambriolages', 'Vols à main armée (arme à feu)', 'Vols avec entrée par ruse',"Vols liés à l'automobile", 
        "Vols de particuliers", "Vols d'entreprises", 'Violation de domicile', 'Destruction et dégradations de biens',
        'Escroqueries, faux et contrefaçons', 'Trafic, revente et usage de drogues', 'Infractions au code du Travail',"Infractions liées à l'immigration", 
        'Différends familiaux', 'Proxénétisme', "Ports ou détentions d'arme prohibée", 'Recels',
        "Délits des courses et jeux d'argent", 'Délits liés aux débits de boisson et de tabac', "Atteintes à l'environnement", 'Délits liés à la chasse et la pêche',
        'Cruauté et délits envers les animaux', 'Atteintes aux intérêts fondamentaux de la Nation'
    ]

    infos = {'intitule' : colonnes,
                'Cas' : [
                    int(df_del[df_del['ville'] == ville_choisie][colonne].iloc[0]) for colonne in colonnes
                ]}

    table = pd.DataFrame(infos)
    data = table.to_dict('rows')

    entete = [{'id': "intitule", 'name': "       "}, {'id': "Cas", 'name': "Nombre de cas"}]

    return data, entete


############ EUROPEENES  #################
@app.callback(Output('elections', 'children'), [Input('ville-picker', 'value')])
def elections(ville_choisie):
    liste_candidats = df_candidats['candidat']

    donnees = []

    for candidat in liste_candidats:
        parti = df_candidats[df_candidats['candidat'] == candidat]['parti'].iloc[0]
        pourcentage = float(df_elections[df_elections['ville'] == ville_choisie][candidat].iloc[0])
        photo = df_candidats[df_candidats['candidat'] == candidat]['photo'].iloc[0]
        color = df_candidats[df_candidats['candidat'] == candidat]['color'].iloc[0]

        donnees.append([pourcentage, candidat, photo, parti, color])

    donnees = sorted(donnees, reverse=True, key=itemgetter(0))

    liste_div = []

    for candidat in donnees:
        image_filename = "candidats\\" + candidat[2]
        encoded_image = base64.b64encode(open(image_filename, 'rb').read())
        liste_div.append(
            html.Div([
                html.Div([
                    html.Img(src='data:image/png;base64,{}'.format(encoded_image.decode()))
                ], style= {'backgroundColor': candidat[-1],
                            'width': str(candidat[0]) + "%",
                            'height': '100%',
                            'fontSize':'20px',
                            'verticalAlign':'top',
                            'border':'1px solid grey'}),
                html.Div([
                    html.Div([
                        html.P(candidat[1])
                    ], style={'display':'inline-block', 'width':'30%','fontWeight':'bold','verticalAlign':'top'}),
                    html.Div([
                        html.P(candidat[3])
                    ], style={'display':'inline-block','width':'50%','fontWeight':'italic','fontSize':'12px','verticalAlign':'top'}),
                    html.Div([
                        html.P(str(candidat[0]) + "%")
                    ], style={'display':'inline-block', 'width':'20%','fontWeight':'bold', 'textAlign':'right'}),
                ], style= {'width' : '100%',
                            'position' : 'absolute',
                            'height': '100%',
                            'top' : 0,
                            'left': 0,
                            'paddingLeft': '60px',
                            'border' : '1px solid' + candidat[-1]})
            ], style = {'width':'70%', 'height': '50px','position': 'relative','marginBottom':'5px'})
        )

    return liste_div


############   Evolution chomage    ##########
@app.callback([Output('annee','children'), Output('map_chomage','children')], [Input('slider','value')])
def chomage_france(annee):
    affichage_annee = str(annee)
    carte = 'maps\\france_chomage_' + str(annee) + '.html'
    affichage_carte = html.Iframe(srcDoc = open(carte, 'r').read(), width= '80%', height= '800')

    return affichage_annee, affichage_carte

server = app.server

if __name__ == "__main__":
    app.run_server(debug= True)

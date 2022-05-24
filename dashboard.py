import streamlit as st

from plotly.subplots import make_subplots
from plotly.graph_objs import *
import plotly.graph_objects as go
from matplotlib import cm

import pandas as pd
import numpy as np
import json


from googleapiclient.discovery import build
from google.oauth2 import service_account
from apiclient import discovery
import httplib2
import plotly.express as px 


def get_dataframe():
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    KEY='AIzaSyDwHm39fC1PJzkwPTiaYumzPh8yIXKVbBU'
    
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?version=v4')
    service = discovery.build(
        'sheets',
        'v4',
        http=httplib2.Http(),
        discoveryServiceUrl=discoveryUrl,
        developerKey=KEY)

    spreadsheetId = '1Stei88CUwjCGuBT1u9r9a6Cbt0Lt7e68r5Eyh5yRpA8'
    rangeName = 'states!A1:K'
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheetId,range=rangeName,valueRenderOption='UNFORMATTED_VALUE').execute()
    values= result.get('values', [])
    if not values:
        print('No data found.')
    return pd.DataFrame(values[1:],columns=values[0])


def create_kpi(val,text,format,color,header_size,value_size):
	return (go.Indicator(
        value = val,
        title= {"text":text,"font":{"size":10}},
        number={'valueformat':format,"font":{"size":40,"family":'Times New Roman',"color": color}}
    ))
	
def return_kpi_grid(df):
    timely_complaint= df.loc[df['timely'] == 'Yes','complaint_id'].sum()
    total_complaint= df['complaint_id'].sum()
    percentage=(timely_complaint/total_complaint)*100

    closed=df.loc[df['company_response'].str.contains("closed",case=False),'complaint_id'].sum()
    inprogress=df.loc[df['company_response'].str.contains("In progress",case=False),'complaint_id'].sum()

    header1_fig= make_subplots(rows=1, cols=4, specs=[[{'type': 'domain'}]*4])
    header1_fig.add_trace(create_kpi(total_complaint,'Total Complaints',',.0f','#7f7f7f',12,35), row=1, col=1)
    header1_fig.add_trace(create_kpi(closed,'Closed Complaints',',.0f','#7f7f7f',12,35), row=1, col=2)
    header1_fig.add_trace(create_kpi(percentage,'Timely Complaints %',',.0f','#7f7f7f',12,35), row=1, col=3)
    header1_fig.add_trace(create_kpi(inprogress,'In Progress Complaints',',.0f','#7f7f7f',12,35), row=1, col=4)
    header1_fig.update_xaxes(showticklabels=False)
    header1_fig.update_yaxes(showticklabels=False)
    header1_fig.update_layout(Layout(paper_bgcolor='rgba(0,0,0,0)',plot_bgcolor='rgba(0,0,0,0)',autosize=True,width=100,height=100))
    
    return header1_fig



def create_selectbox(states):
    state_selected= st.selectbox("Select a state", options=states)
    return state_selected


def get_bar_fig(df):
    
    labels =  df.groupby('product')['complaint_id'].sum().reset_index().sort_values(by='complaint_id')['product'].values
    values = df.groupby('product')['complaint_id'].sum().reset_index().sort_values(by='complaint_id')['complaint_id'].values
    
    fig = px.bar(df, x=values, y=labels, orientation='h',title='Number of Complaints by Products',color_discrete_sequence=px.colors.sequential.Rainbow)
    fig.update_layout(paper_bgcolor='rgba(0,0,0,0)',plot_bgcolor='rgba(0,0,0,0)')

    #hiding labels as labels are too large
    fig.update_yaxes(showgrid=False,showline=False,showticklabels=False,zeroline=False)
    
    

    return fig

def  get_line_chart(df):
    
    labels =  df.groupby('MonthYear')['complaint_id'].sum().reset_index().sort_values(by='complaint_id')['MonthYear'].values
    values = df.groupby('MonthYear')['complaint_id'].sum().reset_index().sort_values(by='complaint_id',ascending=True)['complaint_id'].values
    fig = px.line(df, x=labels, y=values,title='Number of Complaints by Months',color_discrete_sequence=px.colors.sequential.RdBu_r)
    fig.update_layout(paper_bgcolor='rgba(0,0,0,0)',plot_bgcolor='rgba(0,0,0,0)')
    fig.update_xaxes(tickangle=-45, showticklabels = True, type = 'category')

    print("months are ", labels)


    return fig

def  get_pie_chart(df):
    
    labels =  df.groupby('submitted_via')['complaint_id'].sum().reset_index().sort_values(by='submitted_via')['submitted_via'].values
    values = df.groupby('submitted_via')['complaint_id'].sum().reset_index().sort_values(by='complaint_id')['complaint_id'].values
    fig = px.pie(df,labels,values,title='Number of Complaints Submitted Via Channel',color_discrete_sequence=px.colors.sequential.RdBu_r)
    fig.update_layout(paper_bgcolor='rgba(0,0,0,0)',plot_bgcolor='rgba(0,0,0,0)')


    return fig


def  get_tree_map(df):
    
   
    tf = df.groupby(['issue','sub_issue'],as_index=True).size().reset_index(name='complaint_id')

    
    fig = px.treemap(tf, path=[px.Constant("Treemap"),'issue', 'sub_issue'],values='complaint_id',title='Number of Complaints by Issues and Sub Issues',color_discrete_sequence=px.colors.sequential.RdBu)
    
    fig.update_traces(root_color="lightgrey")
    fig.update_layout(margin = dict(t=50, l=25, r=25, b=25))
    return fig


st.set_page_config(layout="wide")
st.markdown("<h1 style='text-align: center; color: back;'>Consumer Financial Protection Bureau  Complaints Dashboard</h1>", unsafe_allow_html=True)
#st.title("Consumer Financial Protection Bureau  Complaints Dashboard")
df=get_dataframe()
data_df=pd.DataFrame()

with st.container():
    col1, col2= st.columns((4,1))


with col2:
    states=['All states']
    states.extend(df.state.unique())
    print(len(states))
    selected_state=create_selectbox(states)
    print(selected_state)
with col1:
    st.header("Complaint Statstics")
    if selected_state != 'All states':
        data_df= df.loc[df['state'] == selected_state ]
        df=data_df
        print(data_df['state'])
        st.plotly_chart(return_kpi_grid(data_df), use_container_width=True)
    else:
        print(df['state'])
        st.plotly_chart(return_kpi_grid(df), use_container_width=True)


with st.container():
	col3, col4= st.columns((2))
	
	col4.plotly_chart(get_line_chart(df), use_container_width=True)
	col3.plotly_chart(get_bar_fig(df), use_container_width=True)



with st.container():
	col5, col6= st.columns((2))
	
	col5.plotly_chart(get_pie_chart(df), use_container_width=True)
	col6.plotly_chart(get_tree_map(df), use_container_width=True)



    
	
	





    



	





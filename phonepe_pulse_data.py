import requests
from pprint import pprint
import json
import git
import streamlit as st
import pandas as pd
from functools import reduce
from datetime import datetime
from rich_dataframe import prettify
import plotly.express as px
import mysql.connector
import os
import git

# path to clone the repository

# path = 'E:\Guvi-MDS'
# url = 'https://github.com/PhonePe/pulse.git'
# git.Git(path).clone(url)



def country_feature_generation(url,year,type_of_data):
    df1= pd.read_json(f"{url}\\1.json")
    if type_of_data == 'transaction':
        df1_empty={'state':[],'count':[],'amount':[],'date':[],'time_H_M_S':[],'year':[]}
        for i in pd.Series(df1['data'])[0]:
            df1_empty['state'].append(i.get('name'))
            df1_empty['year'].append(year)
            df1_empty['count'].append(i.get('metric')[0].get('count'))
            df1_empty['amount'].append(round(float(i.get('metric')[0].get('amount')),2))
            df1_empty['date'].append(datetime.utcfromtimestamp(df1['responseTimestamp']/1000).strftime('%d-%m-%Y'))
            df1_empty['time_H_M_S'].append(datetime.utcfromtimestamp(df1['responseTimestamp']/1000).strftime('%I-%M-%S-%p'))
    if type_of_data == 'user':
        df1_empty={'state':list(df1['data'][0].keys()),'date':[],'time_H_M_S':[],'year':[],'registered_users':[]}
        for i in pd.Series(df1['data'][0]):
            df1_empty['year'].append(year)
            df1_empty['registered_users'].append(i.get('registeredUsers'))
            df1_empty['date'].append(datetime.utcfromtimestamp(df1['responseTimestamp']/1000).strftime('%d-%m-%Y'))
            df1_empty['time_H_M_S'].append(datetime.utcfromtimestamp(df1['responseTimestamp']/1000).strftime('%I-%M-%S-%p'))
    df1_segregated = pd.DataFrame(df1_empty)
    
    df2= pd.read_json(f"{url}\\2.json")
    if type_of_data == 'transaction':
        df2_empty={'state':[],'count':[],'amount':[],'date':[],'time_H_M_S':[],'year':[]}
        for i in pd.Series(df2['data'])[0]:
            df2_empty['state'].append(i.get('name'))
            df2_empty['year'].append(year)
            df2_empty['count'].append(i.get('metric')[0].get('count'))
            df2_empty['amount'].append(round(float(i.get('metric')[0].get('amount')),2))
            df2_empty['date'].append(datetime.utcfromtimestamp(df2['responseTimestamp']/1000).strftime('%d-%m-%Y'))
            df2_empty['time_H_M_S'].append(datetime.utcfromtimestamp(df2['responseTimestamp']/1000).strftime('%I-%M-%S-%p'))
    if type_of_data == 'user':
        df2_empty={'state':list(df1['data'][0].keys()),'date':[],'time_H_M_S':[],'year':[],'registered_users':[]}
        for i in pd.Series(df2['data'][0]):
            df2_empty['year'].append(year)
            df2_empty['registered_users'].append(i.get('registeredUsers'))
            df2_empty['date'].append(datetime.utcfromtimestamp(df2['responseTimestamp']/1000).strftime('%d-%m-%Y'))
            df2_empty['time_H_M_S'].append(datetime.utcfromtimestamp(df2['responseTimestamp']/1000).strftime('%I-%M-%S-%p'))
    df2_segregated = pd.DataFrame(df2_empty)
    
    df3= pd.read_json(f"{url}\\3.json")
    if type_of_data == 'transaction':
        df3_empty={'state':[],'count':[],'amount':[],'date':[],'time_H_M_S':[],'year':[]}
        for i in pd.Series(df3['data'])[0]:
            df3_empty['state'].append(i.get('name'))
            df3_empty['year'].append(year)
            df3_empty['count'].append(i.get('metric')[0].get('count'))
            df3_empty['amount'].append(round(float(i.get('metric')[0].get('amount')),2))
            df3_empty['date'].append(datetime.utcfromtimestamp(df3['responseTimestamp']/1000).strftime('%d-%m-%Y'))
            df3_empty['time_H_M_S'].append(datetime.utcfromtimestamp(df3['responseTimestamp']/1000).strftime('%I-%M-%S-%p'))
    if type_of_data == 'user':
        df3_empty={'state':list(df1['data'][0].keys()),'date':[],'time_H_M_S':[],'year':[],'registered_users':[]}
        for i in pd.Series(df3['data'][0]):
            df3_empty['year'].append(year)
            df3_empty['registered_users'].append(i.get('registeredUsers'))
            df3_empty['date'].append(datetime.utcfromtimestamp(df3['responseTimestamp']/1000).strftime('%d-%m-%Y'))
            df3_empty['time_H_M_S'].append(datetime.utcfromtimestamp(df3['responseTimestamp']/1000).strftime('%I-%M-%S-%p'))
    df3_segregated = pd.DataFrame(df3_empty)
    
    df4= pd.read_json(f"{url}\\4.json")
    if type_of_data == 'transaction':
        df4_empty={'state':[],'count':[],'amount':[],'date':[],'time_H_M_S':[],'year':[]}
        for i in pd.Series(df4['data'])[0]:
            df4_empty['state'].append(i.get('name'))
            df4_empty['year'].append(year)
            df4_empty['count'].append(i.get('metric')[0].get('count'))
            df4_empty['amount'].append(round(float(i.get('metric')[0].get('amount')),2))
            df4_empty['date'].append(datetime.utcfromtimestamp(df4['responseTimestamp']/1000).strftime('%d-%m-%Y'))
            df4_empty['time_H_M_S'].append(datetime.utcfromtimestamp(df4['responseTimestamp']/1000).strftime('%I-%M-%S-%p'))
    if type_of_data == 'user':
        df4_empty={'state':list(df1['data'][0].keys()),'date':[],'time_H_M_S':[],'year':[],'registered_users':[]}
        for i in pd.Series(df4['data'][0]):
            df4_empty['year'].append(year)
            df4_empty['registered_users'].append(i.get('registeredUsers'))
            df4_empty['date'].append(datetime.utcfromtimestamp(df4['responseTimestamp']/1000).strftime('%d-%m-%Y'))
            df4_empty['time_H_M_S'].append(datetime.utcfromtimestamp(df4['responseTimestamp']/1000).strftime('%I-%M-%S-%p'))
    df4_segregated = pd.DataFrame(df4_empty)
    
    concat_df=pd.concat([df1_segregated,df2_segregated,df3_segregated,df4_segregated])
    return concat_df



def data_preprocessing(url,type_of_data):
    empty_dataframe = []
    for i in range(2018,2023):
        c_data=country_feature_generation(f'{url}\{i}',i,type_of_data)
        empty_dataframe.append(c_data)
    single_df=pd.concat(empty_dataframe)
    single_df.reset_index(drop=True, inplace=True)
    return single_df

# states=('andaman-&-nicobar-islands','andhra-pradesh','arunachal-pradesh','assam','bihar','chandigarh','chhattisgarh',
#        'dadra-&-nagar-haveli-&-daman-&-diu','delhi','goa','haryana','gujarat','himachal-pradesh','jammu-&-kashmir',
#        'jharkhand','karnataka','kerala','ladakh','lakshadweep','madhya-pradesh','maharashtra','manipur','meghalaya',
#        'mizoram','nagaland','odisha','puducherry','punjab','rajasthan','sikkim','tamil-nadu','telangana','tripura',
#        'uttarakhand','uttar-pradesh','west-bengal')

def country_map_data(type_of_data):
    country_transaction_map_data=data_preprocessing(fr'E:\Guvi-MDS\pulse\data\map\{type_of_data}\hover\country\india',type_of_data)
    country_transaction_map_data['country'],country_transaction_map_data['type_of_data']='india',type_of_data
    return country_transaction_map_data

# create a connection to the database
con = mysql.connector.connect(
  host="localhost",
  user="root",
  password="mysqlpassword1!",
  database="phonepe_db"
)
cur=con.cursor()

# cur.execute("DROP TABLE IF EXISTS Phonepe_transaction")
# cur.execute("CREATE TABLE Phonepe_transaction (state VARCHAR(100), count VARCHAR(100), amount VARCHAR(100), date VARCHAR(100), time_H_M_S VARCHAR(100), country VARCHAR(100), type_of_data VARCHAR(100), year VARCHAR(5))")
# df_t = country_map_data("transaction")
# for index, row in df_t.iterrows():
#     cur.execute('INSERT INTO Phonepe_transaction (state, count,amount,date,time_H_M_S,country,type_of_data, year) VALUES (%s, %s,%s, %s,%s, %s,%s,%s)', (row['state'], row['count'], row['amount'], row['date'], row['time_H_M_S'], row['country'], row['type_of_data'], row['year']))

# cur.execute("DROP TABLE IF EXISTS Phonepe_user")
# cur.execute("CREATE TABLE Phonepe_user (state VARCHAR(100), date VARCHAR(100), time_H_M_S VARCHAR(100), country VARCHAR(100), type_of_data VARCHAR(100), year VARCHAR(5),registered_users INT)"  )
# df_u = country_map_data("user")
# for index, row in df_u.iterrows():
#     cur.execute('INSERT INTO Phonepe_user (state,date,time_H_M_S,country,type_of_data, year,registered_users) VALUES (%s, %s,%s, %s,%s, %s,%s)', (row['state'],  row['date'], row['time_H_M_S'], row['country'], row['type_of_data'], row['year'],row['registered_users']))
# con.commit()

def fetch_data_trans(query):
    cur.execute(query)
    myresult = cur.fetchall()
    dffffff=pd.DataFrame(myresult,columns=['state', 'count','amount','date','time_H_M_S','country','type_of_data','year'])
    dffffff["amount"] = pd.to_numeric(dffffff["amount"])
    dffffff["count"] = pd.to_numeric(dffffff["count"])
    return dffffff

data_output_trans=fetch_data_trans("SELECT * FROM Phonepe_transaction")


def fetch_data_user(query):
    cur.execute(query)
    myresult = cur.fetchall()
    dffffff=pd.DataFrame(myresult,columns=['state','date','time_H_M_S','country','type_of_data', 'year','registered_users'])
    return dffffff

data_output_user=fetch_data_user("SELECT * FROM Phonepe_user")


st.header("Phonepe Data on PAN - India", anchor=None)

st.header("Number of Transactions", anchor=None)
col1, col2 = st.columns(2)
with col1:
    dropdown1=st.multiselect("Year",
                    data_output_trans['year'].unique(), 
                    default=None,
                    key='dropdown1', help=None, on_change=None, args=None, 
                    kwargs=None,  disabled=False, label_visibility="visible", max_selections=1)
with col2:
    dropdown2=st.multiselect("State",
                    data_output_trans['state'], 
                    default=None,
                    key='dropdown2', help=None, on_change=None, args=None, 
                    kwargs=None,  disabled=False, label_visibility="visible", max_selections=1)

if dropdown1:
    data_output_trans=fetch_data_trans(f"SELECT * FROM Phonepe_transaction WHERE year={dropdown1[0]}")
    fig=px.scatter(data_output_trans,x='year',y='count',color='amount',hover_name='country',log_x=True,size_max=100,
               range_x=[int(data_output_trans['year'].min()),int(data_output_trans['year'].max())],range_y=[int(data_output_trans['count'].min()),int(data_output_trans['count'].max())])
    st.write(fig)
if dropdown2:
    state=str(dropdown2[0])
    data_output_trans=fetch_data_trans(f"SELECT * FROM Phonepe_transaction WHERE state='{state}'")
    fig=px.scatter(data_output_trans,x='count',y='year',color='count',hover_name='country',log_x=True,size_max=100,
               range_x=[int(data_output_trans['count'].min()),int(data_output_trans['count'].max())],range_y=[int(data_output_trans['year'].min()),int(data_output_trans['year'].max())])
    st.write(fig)


st.header("Total Transaction amount", anchor=None)
col3, col4 = st.columns(2)
with col3:
    dropdown3=st.multiselect("Year",
                    data_output_trans['year'].unique(), 
                    default=None,
                    key='dropdown3', help=None, on_change=None, args=None, 
                    kwargs=None,  disabled=False, label_visibility="visible", max_selections=1)
with col4:
    dropdown4=st.multiselect("State",
                    data_output_trans['state'], 
                    default=None,
                    key='dropdown4', help=None, on_change=None, args=None, 
                    kwargs=None,  disabled=False, label_visibility="visible", max_selections=1)


if dropdown3:
    data_output_trans=fetch_data_trans(f"SELECT * FROM Phonepe_transaction WHERE year={dropdown3[0]}")
    fig=px.scatter(data_output_trans,x='year',y='amount',color='amount',hover_name='country',log_x=True,size_max=100,
               range_x=[int(data_output_trans['year'].min()),int(data_output_trans['year'].max())],range_y=[int(data_output_trans['amount'].min()),int(data_output_trans['amount'].max())])
    st.write(fig)
if dropdown4:
    state=str(dropdown4[0])
    data_output_trans=fetch_data_trans(f"SELECT * FROM Phonepe_transaction WHERE state='{state}'")
    fig=px.scatter(data_output_trans,x='amount',y='year',color='amount',hover_name='country',log_x=True,size_max=100,
               range_x=[int(data_output_trans['amount'].min()),int(data_output_trans['amount'].max())],range_y=[int(data_output_trans['year'].min()),int(data_output_trans['year'].max())])
    st.write(fig)



st.header("Total number of Registered Users", anchor=None)
col5, col6 = st.columns(2)
with col5:
    dropdown5=st.multiselect("Year",
                    data_output_user['year'].unique(), 
                    default=None,
                    key='dropdown5', help=None, on_change=None, args=None, 
                    kwargs=None,  disabled=False, label_visibility="visible", max_selections=1)
with col6:
    dropdown6=st.multiselect("State",
                    data_output_user['state'], 
                    default=None,
                    key='dropdown6', help=None, on_change=None, args=None, 
                    kwargs=None,  disabled=False, label_visibility="visible", max_selections=1)


if dropdown5:
    data_output=fetch_data_user(f"SELECT * FROM Phonepe_user WHERE year={dropdown5[0]}")
    fig=px.scatter(data_output,x='year',y='registered_users',color='registered_users',hover_name='country',log_x=True,size_max=100,
               range_x=[int(data_output['year'].min()),int(data_output['year'].max())],range_y=[int(data_output['registered_users'].min()),int(data_output['registered_users'].max())])
    st.write(fig)
    
    # import plotly
    # import json
    # import requests
    # from datetime import date

    # wiki_df =pd.read_html('https://en.wikipedia.org/wiki/COVID-19_pandemic_in_India')[5].dropna(how='all',axis=1)

    # print(wiki_df)
    # india_geojson = json.load(open("E:\Downloads\states_india.geojson", "r"))
    # state_id_map = {}
    # for feature in india_geojson["features"]:
    #     feature["id"] = feature["properties"]["state_code"]
    #     state_id_map[feature["properties"]["st_nm"]] = feature["id"]
    #     print("jjjjjj",data_output["state"])
    #     # data_output["id"] = data_output["state"].apply(lambda x: state_id_map[x])
    # from urllib.request import urlopen
    # with urlopen('https://raw.githubusercontent.com/Subhash9325/GeoJson-Data-of-Indian-States/master/Indian_States') as response:
    #     states = json.load(response)
    # fig = px.choropleth_mapbox(
    # data_output,
    # locations=states,
    # geojson=india_geojson,
    # color="registered_users",
    # hover_name="state",
    # hover_data=["registered_users"],
    # title="",
    # mapbox_style="carto-positron",
    # color_continuous_scale='Inferno_r',
    # center={"lat": 23, "lon": 88},
    # zoom=3.45,
    # opacity=1,
    # )
    # fig.update_layout(height=700, width=950, font=dict(family="Arial, Helvetica, sans-serif",size=24,), title_x=0.5, coloraxis_colorbar=dict(title="Covid cases"),)
    # fig.update_layout(coloraxis_colorbar_x=-0.25,)
    # st.write(fig)
    
if dropdown6:
    
    state=str(dropdown6[0])
    print(dropdown6[0],f"SELECT * FROM Phonepe_user WHERE state='{state}'")
    data_output=fetch_data_user(f"SELECT * FROM Phonepe_user WHERE state='{state}'")
    fig=px.scatter(data_output,x='registered_users',y='year',color='registered_users',hover_name='country',log_x=True,size_max=100,
               range_x=[int(data_output['registered_users'].min()),int(data_output['registered_users'].max())],range_y=[int(data_output['year'].min()),int(data_output['year'].max())])
    st.write(fig)

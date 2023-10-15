import pandas as pd
import mysql.connector as sql
import streamlit as st
import plotly.express as px
import os
import json
from streamlit_option_menu import option_menu
from PIL import Image
from git.repo.base import Repo

icon = Image.open("phonepe.png")
st.set_page_config(page_title= "Phonepe Pulse Data",
                   layout= "wide",
                   page_icon= icon,
                   initial_sidebar_state= "expanded"
                   )


# #To clone the Github Pulse repository use the following code
# Reference Syntax - Repo.clone_from("Clone Url", "Your working directory")
# Repo.clone_from("https://github.com/PhonePe/pulse.git", "E:/Guvi-MDS/D55D56/pulse/data")


# path1 = "E:/Guvi-MDS/D55D56/pulse/data/data/map/transaction/hover/country/india/state"
# aggregated_transactions_list = os.listdir(path1)

# # Give any column names that you want
# columns1 = {'State': [], 'Year': [], 'Quarter': [], 'district': [], 'Transaction_count': [],'Transaction_amount': []}
# # "E:\Guvi-MDS\D55D56\pulse\data\top\transaction\country\india\state\andaman-&-nicobar-islands"

# for state in aggregated_transactions_list:
#     cur_state = path1 +"/"+ state + "/"
#     agg_year_list = os.listdir(cur_state)

#     for year in agg_year_list:
#         cur_year = cur_state + year + "/"
#         agg_file_list = os.listdir(cur_year)

#         for file in agg_file_list:
#             cur_file = cur_year + file
#             data = open(cur_file, 'r')
#             A = json.load(data)

#             for i in A['data']['hoverDataList']:
#                 district = i['name']
#                 count = i['metric'][0]['count']
#                 amount = i['metric'][0]['amount']
#                 columns1['district'].append(district)
#                 columns1['Transaction_count'].append(count)
#                 columns1['Transaction_amount'].append(amount)
#                 columns1['State'].append(state)
#                 columns1['Year'].append(year)
#                 columns1['Quarter'].append(int(file.strip('.json')))
            
# df = pd.DataFrame(columns1)
# print(df)



mydb = sql.connect(host="localhost",
                   user="root",
                   password="mysqlpassword1!",
                   database= "phonepe_pulse"
                  )
mycursor = mydb.cursor(buffered=True)

col10,col11=st.columns([1,30],gap="small")
with col10:
    st.image('phonepe.png',width=75)
with col11:
    st.markdown("<h1 style='text-align: center;'>Phonepe Pulse Data</h1>", unsafe_allow_html=True) 
     
chart_select=st.selectbox("More info : ",
                          ('Top states and districts',
                           'Transaction amount on PAN-India',
                           'Transaction count on PAN-India',
                           'Most used Transaction type',
                           'More info on states and districts'),key='chart_select')

st.header("Phonepe Data on PAN - India", anchor=None)


if chart_select=='Top states and districts':
    col1, col2 = st.columns(2)
    with col1:
        Yearr=st.selectbox("Year",('2018','2019','2020','2021','2021','2022'),key='Yearr')
    with col2:
        Quarterr =st.selectbox("Quarter",('1','2','3','4'),key='Quarterr')

   
    col1,col2 = st.columns([1,1],gap="small")
    
    with col1:
        st.markdown("### :violet[Top 10 States]")
        mycursor.execute(f"select state, sum(Transaction_count) as Total_Transactions_Count, sum(Transaction_amount) as Total from aggregated_transactions where year = {Yearr} and quarter = {Quarterr} group by state order by Total desc limit 10")
        df = pd.DataFrame(mycursor.fetchall(), columns=['State', 'Transactions_Count','Total_Amount'])
        fig = px.pie(df, values='Total_Amount',
                            names='State',
                            color_discrete_sequence=px.colors.sequential.Tealgrn_r,
                            hover_data=['Transactions_Count'],
                            labels={'Transactions_Count':'Transactions_Count'})

        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig,use_container_width=True)
        
    with col2:
        st.markdown("### :violet[Top 10 Districts]")
        mycursor.execute(f"select district , sum(Count) as Total_Count, sum(Amount) as Total from map_transaction where year = {Yearr} and quarter = {Quarterr} group by district order by Total desc limit 10")
        df = pd.DataFrame(mycursor.fetchall(), columns=['District', 'Transactions_Count','Total_Amount'])

        fig = px.pie(df, values='Total_Amount',
                            names='District',
                            color_discrete_sequence=px.colors.sequential.Purples_r,
                            hover_data=['Transactions_Count'],
                            labels={'Transactions_Count':'Transactions_Count'})

        fig.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig,use_container_width=True)
        
            


if chart_select=='Transaction amount on PAN-India':
    col1, col2 = st.columns(2)
    with col1:
        Year=st.selectbox("Year",('2018','2019','2020','2021','2021','2022'),key='Year')
    with col2:
        Quarter =st.selectbox("Quarter",('1','2','3','4'),key='Quarter')
        
    st.markdown("## :violet[Total Transaction Amount on PAN-India]")
    mycursor.execute(f"select state, sum(count) as Total_Transactions, sum(amount) as Total_amount from map_transaction where year = {Year} and quarter = {Quarter} group by state order by state")
    df1 = pd.DataFrame(mycursor.fetchall(),columns= ['State', 'Total_Transactions', 'Total_amount'])
    df2 = pd.read_csv("E:\Guvi-MDS\DT5\capstone_projects\Phonepe-Pulse-Data-Visualization-and-Exploration-A-User-Friendly-Tool-Using-Streamlit-and-Plotly\Statenames.csv")
    df1.State = df2

    fig = px.choropleth(df1,geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',
                locations='State',
                color='Total_amount',
                color_continuous_scale='Tealgrn_r')

    fig.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig,use_container_width=True)
            
if chart_select=='Transaction count on PAN-India':
    col1, col2 = st.columns(2)
    with col1:
        Year=st.selectbox("Year",('2018','2019','2020','2021','2021','2022'),key='Year')
    with col2:
        Quarter =st.selectbox("Quarter",('1','2','3','4'),key='Quarter')
              
    st.markdown("## :violet[Total Transaction Count on PAN-India]")
    mycursor.execute(f"select state, sum(count) as Total_Transactions, sum(amount) as Total_amount from map_transaction where year = {Year} and quarter = {Quarter} group by state order by state")
    df1 = pd.DataFrame(mycursor.fetchall(),columns= ['State', 'Total_Transactions', 'Total_amount'])
    df2 = pd.read_csv("E:\Guvi-MDS\DT5\capstone_projects\Phonepe-Pulse-Data-Visualization-and-Exploration-A-User-Friendly-Tool-Using-Streamlit-and-Plotly\Statenames.csv")
    df1.Total_Transactions = df1.Total_Transactions.astype(int)
    df1.State = df2

    fig = px.choropleth(df1,geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                featureidkey='properties.ST_NM',
                locations='State',
                color='Total_Transactions',
                color_continuous_scale='sunset')

    fig.update_geos(fitbounds="locations", visible=False)
    st.plotly_chart(fig,use_container_width=True)
             
        
if chart_select=='Most used Transaction type':
    col1, col2 = st.columns(2)
    with col1:
        Year=st.selectbox("Year",('2018','2019','2020','2021','2021','2022'),key='Year')
    with col2:
        Quarter =st.selectbox("Quarter",('1','2','3','4'),key='Quarter')
    st.markdown("## :violet[Most used Transaction type on PAN-India]")
    mycursor.execute(f"select Transaction_type, sum(Transaction_count) as Total_Transactions, sum(Transaction_amount) as Total_amount from aggregated_transactions where year= {Year} and quarter = {Quarter} group by transaction_type order by Transaction_type")
    df = pd.DataFrame(mycursor.fetchall(), columns=['Transaction_type', 'Total_Transactions','Total_amount'])

    fig = px.bar(df,
                    title='Transaction Types vs Total_Transactions',
                    x="Transaction_type",
                    y="Total_Transactions",
                    orientation='v',
                    color='Total_amount',
                    color_continuous_scale=px.colors.sequential.Agsunset)
    st.plotly_chart(fig,use_container_width=False)
    
    
if chart_select=='More info on states and districts':  
    col1, col2 = st.columns(2)
    with col1:
        Year=st.selectbox("Year",('2018','2019','2020','2021','2021','2022'),key='Year')
    with col2:
        Quarter =st.selectbox("Quarter",('1','2','3','4'),key='Quarter')     
        
    st.markdown("## :violet[Select any State]")
    selected_state = st.selectbox("",
                            ('andaman-&-nicobar-islands','andhra-pradesh','arunachal-pradesh','assam','bihar',
                            'chandigarh','chhattisgarh','dadra-&-nagar-haveli-&-daman-&-diu','delhi','goa','gujarat','haryana',
                            'himachal-pradesh','jammu-&-kashmir','jharkhand','karnataka','kerala','ladakh','lakshadweep',
                            'madhya-pradesh','maharashtra','manipur','meghalaya','mizoram',
                            'nagaland','odisha','puducherry','punjab','rajasthan','sikkim',
                            'tamil-nadu','telangana','tripura','uttar-pradesh','uttarakhand','west-bengal'),index=30)
        
    mycursor.execute(f"select State, District,year,quarter, sum(count) as Total_Transactions, sum(amount) as Total_amount from map_transaction where year = {Year} and quarter = {Quarter} and State = '{selected_state}' group by State, District,year,quarter order by state,district")
    
    df1 = pd.DataFrame(mycursor.fetchall(), columns=['State','District','Year','Quarter',
                                                        'Total_Transactions','Total_amount'])
    fig = px.bar(df1,
                    title=selected_state,
                    x="District",
                    y="Total_Transactions",
                    orientation='v',
                    color='Total_amount',
                    color_continuous_scale=px.colors.sequential.Agsunset)
    st.plotly_chart(fig,use_container_width=True)
        

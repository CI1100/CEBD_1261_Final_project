import streamlit as st
#from dashboard.py import world_dashboard_page, country_dashboard_page
from data_preprocessing import *
from data_viz import *
from data_analytics import *
from dashboard import *
import pickle5 as pickle
import matplotlib.pyplot as plt
import pandas as pd

joined_wdf = compute_merged_df()#load_joined_data()
countries = pickle.load( open( "./data/countries_names.pkl", "rb" ) )



def world_dashboard_page():
    st.markdown("# World Dashboard")
    with open('./data/world_stats.pkl', 'rb') as f:
        world_stats = pickle.load(f)
    print(world_stats.keys() )
    
    today_stats = world_stats['today']
    yesterday_stats = world_stats['yesterday']
    delta_stats = world_stats['deltas']
    
    st.markdown(f"Total Cases: {today_stats['total_cases']}")
    st.markdown(f"Total Recoveries: {today_stats['total_recoveries']}")
    st.markdown(f"Total Deaths: {today_stats['total_deaths']}")
    st.markdown(f"Changes in cases since the day before: {delta_stats['total_new_cases']}")
    st.markdown(f"Changes in recoveries since the day before: {delta_stats['total_new_recoveries']}")
    st.markdown(f"Changes in deaths since the day before: {delta_stats['total_new_deaths']}")
    
def country_dashboard_page():
    
    selected_country = st.selectbox('Choose a Country:',
                                     countries)
    
    st.markdown(f"Country {selected_country} Dashboard")
    
    country_pdf = joined_wdf.filter(joined_wdf['Country/Region'] == selected_country).toPandas()
    country_pdf['date'] = pd.to_datetime(country_pdf['date'])
    country_pdf = country_pdf.sort_values('date')
    #print(country_pdf.head())
    
    st.pyplot(get_timeline_fig(country_pdf, 'new_cases'))
    st.pyplot(get_timeline_fig(country_pdf, 'new_deaths'))
    st.pyplot(get_timeline_fig(country_pdf, 'new_recoveries'))

st.sidebar.markdown("# Select Data Source")
pages = {'World':world_dashboard_page,
         'Specific Country':country_dashboard_page}

choice = st.sidebar.radio("Choose your page: ", tuple(pages.keys()))
pages[choice]()
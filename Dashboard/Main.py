import streamlit as st
import pandas as pd
import numpy as np
from streamlit_folium import folium_static
from st_pages import show_pages, Page
import visualizations as v

#in dashboard directory, run cmd
#py -m streamlit run Dashboard/Main.py

st.set_page_config(
    page_title="Company_Selection",
)

# Multipage configration from the toml file, contains the sider bar name and icons
show_pages(
    [
        Page("Main.py", "Overall Statistic"), #
        Page("Pages/District_Map.py", "District Level"),
        Page("Pages/Project_Map.py", "Project Level"),
    ]
)

############################################################################
############### Add different parts to the session state ###################
############################################################################

#TODO: retreive from DB
#retreive [property, lat, long, name] from pd.dataframe
# fileter later

if "room_type" not in st.session_state: # HDB-2 ROOM - HDB 3ROOM
    st.session_state['room_type'] = None #TODO: filter from DF

if "transaction_year" not in st.session_state: # Year-Month datetime, 2 drop down list
    st.session_state['transaction_year'] = None #TODO: filter from DF
    
if "transaction_month" not in st.session_state: 
    st.session_state['transaction_month'] = None #TODO: filter from DF

if "district_list" not in st.session_state: 
    st.session_state['district_list'] = None #TODO: filter from DF
    
if "amentity_list" not in st.session_state:
    st.session_state['amentity_list'] = None #TODO: retreive from DB

if "price_per_sqft_min" not in st.session_state:
    st.session_state['price_per_sqft_min'] = None #TODO: retreive from DB

if "price_per_sqft_max" not in st.session_state:
    st.session_state['price_per_sqft_max'] = None #TODO: retreive from DB

if "floor_range_min" not in st.session_state:
    st.session_state['floor_range_min'] = None #TODO: retreive from DB

if "floor_range_max" not in st.session_state:
    st.session_state['floor_range_max'] = None #TODO: retreive from DB

    
############################################################################
st.title("Singapore Property Trend Chart")
st.pyplot(v.plot_price_over_time())

# # Function for Page 2 - Folium Map
# def show_folium_map():
#     st.title('Singapore District Map')
#     m = v.plot_price_per_district()
#     # Display the map
#     folium_static(m)

# # Sidebar navigation
# st.sidebar.title('Navigation')
# page = st.sidebar.radio("Choose a page", ['Random Line Chart', 'District Map', 'Project Map'])

# if page == 'Random Line Chart':
#     st.pyplot(v.plot_price_over_time())
# elif page == 'District Map':
#     show_folium_map()
import streamlit as st
import pandas as pd
import numpy as np
from streamlit_folium import folium_static
from st_pages import show_pages, Page
import visualizations as v
import sys
from pathlib import Path
current_script_path = Path(__file__).resolve()
parent_directory = current_script_path.parent.parent

if str(parent_directory) not in sys.path:
    sys.path.append(str(parent_directory))
from etl.RetrieveDB import RetrieveDB

#in dashboard directory, run cmd
#py -m streamlit run Main.py

st.set_page_config(
    page_title="Company_Selection",
)

try:
    st.session_state.cursor = RetrieveDB(db_connect_type = 'LOCAL')
    # st.session_state.cursor = RetrieveDB(db_connect_type = 'IAM')
except Exception as e:
    st.warning("Cursor is not established")
    st.write(e)
    st.stop()

# Multipage configration from the toml file, contains the sider bar name and icons
show_pages(
    [
        Page("Main.py", "Overall Statistics"), 
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


if "all_transactions" not in st.session_state:
    st.session_state['all_transactions'] = pd.DataFrame(st.session_state.cursor.get_price_per_sqft_dashboard())

if "room_type" not in st.session_state: # HDB-2 ROOM - HDB 3ROOM
    st.session_state['room_type'] = st.session_state.all_transactions['property_type'].unique()
    # Semi-detached
    # Terrace
    # Strata Terrace
    # Condominium
    # Apartment
    # Detached
    # Strata Semi-detached
    # Strata Detached
    # Executive Condominium
    # 2 ROOM
    # 3 ROOM
    # 4 ROOM
    # 5 ROOM
    # EXECUTIVE
    # 1 ROOM
    # MULTI-GENERATION
    # TODO consider using this or type_of_sale from transaction table

if "transaction_year" not in st.session_state:
    st.session_state['transaction_year'] = st.session_state.all_transactions['transaction_year'].unique()
    
if "transaction_month" not in st.session_state: 
    st.session_state['transaction_month'] = st.session_state.all_transactions['transaction_month'].unique()

if "district_list" not in st.session_state:
    districts = pd.DataFrame(st.session_state.cursor.get_districts())
    st.session_state['district_ids'] = districts['id']
    st.session_state['district_list'] = districts[['district_name', 'coordinates']]
    
if "amenities_list" not in st.session_state:
    amenities = pd.DataFrame(st.session_state.cursor.get_amenities())
    st.session_state['amenities_list'] = amenities[['amenity_type', 'amenity_name', 'long', 'lat']].drop_duplicates()

if "price_per_sqft_min" not in st.session_state:
    st.session_state['price_per_sqft_min'] = st.session_state.all_transactions['price_per_sqft'].tolist()[0]

if "price_per_sqft_max" not in st.session_state:
    st.session_state['price_per_sqft_max'] = st.session_state.all_transactions['price_per_sqft'].tolist()[-1]

if "floor_range_min" not in st.session_state:
    st.session_state['floor_range_min'] = st.session_state.all_transactions['floor_range_start'].min()

if "floor_range_max" not in st.session_state:
    st.session_state['floor_range_max'] = st.session_state.all_transactions['floor_range_end'].max()

    
############################################################################
st.title("Singapore Property Trend Chart")
st.pyplot(v.plot_price_over_time())



# ################################## TESTING ###########################
# values to be put into filter
st.write(st.session_state.district_list)
st.write(st.session_state.amenities_list)
st.write(st.session_state.floor_range_max)
st.write(st.session_state.floor_range_min)
st.write(st.session_state.price_per_sqft_max)
st.write(st.session_state.price_per_sqft_min)
st.write(st.session_state.transaction_month)
st.write(st.session_state.transaction_year)
st.write(st.session_state.room_type)

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
import streamlit as st
import datetime
import pandas as pd
import numpy as np
from streamlit_folium import folium_static
from st_pages import show_pages, Page
import visualizations as v
import sys
from pathlib import Path
from Filter import Filter

current_script_path = Path(__file__).resolve()
parent_directory = current_script_path.parent.parent

if str(parent_directory) not in sys.path:
    sys.path.append(str(parent_directory))
from etl.RetrieveDB import RetrieveDB

#in dashboard directory, run cmd
#cd Dashboard
#py -m streamlit run Main.py

st.set_page_config(
    page_title="Overall_Statistics",
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
if "filter" not in st.session_state:
    filter = Filter()
    st.session_state['filter'] = filter

if "all_transactions" not in st.session_state:
    st.session_state['all_transactions'] = pd.DataFrame(st.session_state.cursor.get_price_per_sqft_dashboard())

if "room_type" not in st.session_state:
    st.session_state['room_type']  = sorted(st.session_state.all_transactions['property_type'].unique())
    st.session_state.filter.room_type = st.session_state['room_type']
    # TODO consider using this or type_of_sale from transaction table

# if "transaction_year" not in st.session_state:
#     st.session_state['transaction_year'] = st.session_state.all_transactions['transaction_year'].unique()
#     filter.transaction_year = st.session_state['transaction_year']
    
# if "transaction_month" not in st.session_state: 
#     st.session_state['transaction_month'] = st.session_state.all_transactions['transaction_month'].unique()
#     filter.transaction_month = st.session_state['transaction_month']

if "transaction_date_start" not in st.session_state:
    st.session_state['transaction_date_start'] = datetime.date(2019,1,1)
    
if "transaction_date_end" not in st.session_state: 
    st.session_state['transaction_date_end'] = datetime.datetime.now()


if "district_list" not in st.session_state:
    st.session_state.district_list = sorted(st.session_state.all_transactions['district_name'].unique())
    st.session_state.filter.district_list = st.session_state.district_list

if "amenities_list" not in st.session_state:
    amenities = pd.DataFrame(st.session_state.cursor.get_amenities())
    st.session_state.amenities = amenities
    st.session_state['amenities_list'] = ['MRT','Park']
    st.session_state.filter.amenities_list = set(amenities['amenity_type'])
    
if "price_per_sqft_range" not in st.session_state:
    st.session_state['price_per_sqft_range'] = (st.session_state.all_transactions['price_per_sqft'].tolist()[0], st.session_state.all_transactions['price_per_sqft'].tolist()[-1])
    st.session_state.filter.price_per_sqft_range = st.session_state['price_per_sqft_range']
    
# if "floor_range" not in st.session_state:
#     st.session_state['floor_range'] = (st.session_state.all_transactions['floor_range_start'].min(),st.session_state.all_transactions['floor_range_start'].max())
#     st.session_state.filter.floor_range = st.session_state['floor_range']




############################################################################

st.title("Singapore Property Price Trend")

on = st.toggle('Show Filters')

if on:
    #date selector
    d = st.date_input(
        "Select Transaction Time Range",
        (st.session_state['transaction_date_start'], st.session_state['transaction_date_end']),
        datetime.date(2019,1,1),
        datetime.datetime.now(),
        format="MM.DD.YYYY",
    )
    
    # room type selector
    st.session_state['room_type'] = st.multiselect(
        'Room Type: ',
        st.session_state.filter.room_type,
        st.session_state['room_type'])

    # disctrict selector
    st.session_state['district_list'] = st.multiselect(
        'Districts: ',
        st.session_state.filter.district_list,
        st.session_state['district_list'])

    
    #price list 
    st.session_state.price_per_sqft_range  = st.slider(
        "Price per Sqft:",
        st.session_state.filter.price_per_sqft_range[0],
        st.session_state.filter.price_per_sqft_range[1],
        value=st.session_state.price_per_sqft_range)
    
    # #floor range
    # st.session_state.floor_range  = st.slider(
    #     "Floor Range:",
    #     st.session_state.filter.floor_range[0],
    #     st.session_state.filter.floor_range[1],
    #     value=st.session_state.floor_range)

    
#plot the graph
try:
    st.session_state['transaction_date_start'], st.session_state['transaction_date_end'] = d[0],d[1]
    st.pyplot(v.plot_price_over_time(st.session_state['all_transactions'],
                                    st.session_state['transaction_date_start'],
                                    st.session_state['transaction_date_end'],
                                    st.session_state['room_type'],
                                    st.session_state['district_list'],
                                    st.session_state['price_per_sqft_range']),
            # use_container_width=False
            )
except: #avoid error during user selection
    st.pyplot(v.plot_price_over_time(st.session_state['all_transactions'],
                                    datetime.date(2019,1,1),
                                    datetime.datetime.now(),
                                    st.session_state['room_type'],
                                    st.session_state['district_list'],
                                    st.session_state['price_per_sqft_range']))


# ################################## TESTING ###########################
# values to be put into filter
st.write("TESTING")
st.write(st.session_state['all_transactions'])
# st.write(st.session_state.district_list)
# st.write(st.session_state.amenities_list)
# st.write(st.session_state.floor_range_max)
# st.write(st.session_state.floor_range_min)
# st.write(st.session_state.price_per_sqft_max)
# st.write(st.session_state.price_per_sqft_min)
# st.write(sorted(st.session_state.room_type))

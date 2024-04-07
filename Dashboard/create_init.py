import streamlit as st
from Filter import Filter
import datetime
import pandas as pd

def init_session_state():
    if "filter" not in st.session_state:
        filter = Filter()
        st.session_state['filter'] = filter

    if "all_transactions" not in st.session_state:
        st.session_state['all_transactions'] = pd.DataFrame(st.session_state.cursor.get_price_per_sqft_dashboard())

    if "room_type" not in st.session_state:
        # st.session_state['room_type']  = sorted(st.session_state.all_transactions['property_type'].unique())
        st.session_state.filter.room_type = sorted(st.session_state.all_transactions['property_type'].unique())
        # st.session_state['room_type'] = []
        st.session_state['room_type'] = st.session_state.filter.room_type        
        # TODO consider using this or type_of_sale from transaction table

    if "transaction_date_start" not in st.session_state:
        st.session_state['transaction_date_start'] = datetime.date(2019,1,1)
        
    if "transaction_date_end" not in st.session_state: 
        st.session_state['transaction_date_end'] = datetime.datetime.now()

    if "district_list" not in st.session_state:
        district_project_record = pd.DataFrame(st.session_state.cursor.get_district_popup())
        district_project_record = sorted(district_project_record[district_project_record['no_of_projects'] > 0]['district_name'])
        # st.session_state.district_list=[]
        st.session_state.district_list=district_project_record
        st.session_state.filter.district_list = district_project_record

    if "amenities_list" not in st.session_state:
        amenities = pd.DataFrame(st.session_state.cursor.get_amenities())
        st.session_state.amenities = amenities
        st.session_state['amenities_list'] = ['MRT']
        st.session_state.filter.amenities_list = set(amenities['amenity_type'])
        
    if "price_per_sqft_range" not in st.session_state:
        st.session_state['price_per_sqft_range'] = (st.session_state.all_transactions['price_per_sqft'].tolist()[0], st.session_state.all_transactions['price_per_sqft'].tolist()[-1])
        st.session_state.filter.price_per_sqft_range = st.session_state['price_per_sqft_range']
        
    # if "floor_range" not in st.session_state:
    #     st.session_state['floor_range'] = (st.session_state.all_transactions['floor_range_start'].min(),st.session_state.all_transactions['floor_range_start'].max())
    #     st.session_state.filter.floor_range = st.session_state['floor_range']


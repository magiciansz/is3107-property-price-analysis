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
from create_init import init_session_state

current_script_path = Path(__file__).resolve()
parent_directory = current_script_path.parent.parent

if str(parent_directory) not in sys.path:
    sys.path.append(str(parent_directory))
from etl.RetrieveDB import RetrieveDB

#in dashboard directory, run cmd
#cd Dashboard
#py -m streamlit run Main.py

st.set_page_config(
    page_title="Overall_Trend",
    layout='wide'
)


if 'cursor' not in st.session_state:
    try:
        cursor = RetrieveDB(db_connect_type = 'LOCAL')
        # cursor = RetrieveDB(db_connect_type = 'IAM')
        st.session_state.cursor = cursor
        init_session_state()
    except Exception as e:
        st.warning("Cursor is not established")
        st.write(e)
        st.stop()

# Multipage configration from the toml file, contains the sider bar name and icons
show_pages(
    [
        Page("Main.py", "Overall Price Trend"), 
        Page("Pages/District_Map.py", "Price Breakdown by District"),
        Page("Pages/Project_Map.py", "Housing Projects and Amenities Map"),
    ]
)

############################################################################
############### Add different parts to the session state ###################
############################################################################
    
st.title("Singapore Property Price Trend")

def set_session_states(room_types_selected, district_list_selected, d, floor_range_selected):
    # set session states from user selections
    st.session_state.room_type = room_types_selected
    st.session_state.district_list = district_list_selected
    st.session_state['transaction_date_start'], st.session_state['transaction_date_end'] = d[0],d[1]
    st.session_state.floor_range = floor_range_selected
    

def reset_room_types():
    st.session_state.room_type = []

def reset_districts():
    st.session_state.district_list = []

def reset_floor_range():
    st.session_state.floor_range = st.session_state.filter.floor_range


#plot the graph
def plot_graph():
    # plotting graph with selections
    try:
        if v.plot_price_over_time(st.session_state['all_transactions'],
                                st.session_state['transaction_date_start'],
                                st.session_state['transaction_date_end'],
                                st.session_state['room_type'],
                                st.session_state['district_list'],
                                st.session_state['floor_range'])[0]:
                                        
            st.pyplot(v.plot_price_over_time(st.session_state['all_transactions'],
                                            st.session_state['transaction_date_start'],
                                            st.session_state['transaction_date_end'],
                                            st.session_state['room_type'],
                                            st.session_state['district_list'],
                                            st.session_state['floor_range'])[1],
                    # use_container_width=False
                    )
    except: #avoid error during user selection
        st.write("No filters selected or No available transactions with the seleted filters. Showing all transactions.")
        st.pyplot(v.plot_price_over_time(st.session_state['all_transactions'],
                                        datetime.date(2019,1,1),
                                        datetime.datetime.now(),
                                        st.session_state.filter.room_type,
                                        st.session_state.filter.district_list,
                                        st.session_state.filter.floor_range)[1])



with st.expander(label="Filter values", expanded=False):
    with st.form('test'):
        room_type_selector, disctrict_selector = st.columns([ 0.5, 0.5])
        with room_type_selector:

            all = st.checkbox("Select All Room Types", on_change=reset_room_types())
            
            if all:
                room_types_selected = st.multiselect(
                    'Room Type: ',
                    st.session_state.filter.room_type,
                    st.session_state.filter.room_type)
            else:
                room_types_selected = st.multiselect(
                    'Room Type: ',
                    st.session_state.filter.room_type,
                    st.session_state['room_type'])


        with disctrict_selector:
            all = st.checkbox("Select All Districts", on_change=reset_districts())
            if all:
                district_list_selected = st.multiselect(
                    'Districts: ',
                    st.session_state.filter.district_list,
                    st.session_state.filter.district_list)
            else:
                district_list_selected = st.multiselect(
                    'Districts: ',
                    st.session_state.filter.district_list,
                    st.session_state['district_list'])


        date_filter, pre_filter2, confirm = st.columns([0.4, 0.4, 0.15])
        # st.columns([0.25, 0.3, 0.3, 0.15])
        with date_filter:
            d = st.date_input(
            "Select Transaction Time Range",
            (st.session_state['transaction_date_start'], st.session_state['transaction_date_end']),
            datetime.date(2019,1,1),
            datetime.datetime.now(),
            format="MM.DD.YYYY",
        )
            
        with pre_filter2:
            #floor range
            floor_range_selected  = st.slider(
                "Floor Range:",
                st.session_state.filter.floor_range[0],
                st.session_state.filter.floor_range[1],
                value=st.session_state.floor_range,
                step=1,
                on_change=reset_floor_range())            
        
        with confirm:    
            submitted = st.form_submit_button('Confirm')
            if submitted:
                set_session_states(room_types_selected, district_list_selected, d, floor_range_selected)

plot_graph()


# # ################################## TESTING ###########################
# # values to be put into filter
# st.write("TESTING")
# st.write(st.session_state['all_transactions'])
# st.write(st.session_state.district_list)
# # st.write(st.session_state.amenities_list)
# st.write(st.session_state.floor_range)
# # st.write(st.session_state.floor_range_min)
# # st.write(st.session_state.price_per_sqft_max)
# # st.write(st.session_state.price_per_sqft_min)
# st.write(sorted(st.session_state.room_type))

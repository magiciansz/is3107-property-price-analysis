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
        Page("Pages/District_Map.py", "District Level"),
        Page("Pages/Project_Map.py", "Project Level"),
    ]
)

############################################################################
############### Add different parts to the session state ###################
############################################################################
    
st.title("Singapore Property Price Trend")

    # #price list 
    # st.session_state.price_per_sqft_range  = st.slider(
    #     "Price per Sqft:",
    #     st.session_state.filter.price_per_sqft_range[0],
    #     st.session_state.filter.price_per_sqft_range[1],
    #     value=st.session_state.price_per_sqft_range)
    
    # #floor range
    # st.session_state.floor_range  = st.slider(
    #     "Floor Range:",
    #     st.session_state.filter.floor_range[0],
    #     st.session_state.filter.floor_range[1],
    #     value=st.session_state.floor_range)


def set_session_states(room_types_selected, district_list_selected, d):
    # TODO add on more session states from user selection
    # set session states from user selections
    st.session_state.room_type = room_types_selected
    st.session_state.district_list = district_list_selected
    st.session_state['transaction_date_start'], st.session_state['transaction_date_end'] = d[0],d[1]
    

def reset_room_types():
    st.session_state.room_type = []

def reset_districts():
    st.session_state.district_list = []


#plot the graph
def plot_graph():
    # plotting graph with selections
    try:
        st.pyplot(v.plot_price_over_time(st.session_state['all_transactions'],
                                        st.session_state['transaction_date_start'],
                                        st.session_state['transaction_date_end'],
                                        st.session_state['room_type'],
                                        st.session_state['district_list'],
                                        st.session_state['price_per_sqm_range']),
                # use_container_width=False
                )
    except: #avoid error during user selection
        st.pyplot(v.plot_price_over_time(st.session_state['all_transactions'],
                                        datetime.date(2019,1,1),
                                        datetime.datetime.now(),
                                        st.session_state.filter.room_type,
                                        st.session_state.filter.district_list,
                                        st.session_state.filter.price_per_sqm_range))


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
            # use_default_normal = st.button('Action 2', on_click=None, help="Preset sliders to default thresholds")
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
            
        # TODO price filters
        with pre_filter2:
            if True:
                o_threshold = st.slider("Slider 2:", 0, 100, (20,100), help="Drag sliders to desired values")
            
        with confirm:
            
            submitted = st.form_submit_button('Submit')
            if submitted:
                set_session_states(room_types_selected, district_list_selected, d)

            
            # TODO how to only refresh graph when users click confirm selection
            # plot_graph()



plot_graph()


# ################################## TESTING ###########################
# values to be put into filter
st.write("TESTING")
st.write(st.session_state['all_transactions'])
st.write(st.session_state.district_list)
# st.write(st.session_state.amenities_list)
# st.write(st.session_state.floor_range_max)
# st.write(st.session_state.floor_range_min)
# st.write(st.session_state.price_per_sqft_max)
# st.write(st.session_state.price_per_sqft_min)
# st.write(sorted(st.session_state.room_type))

from streamlit_folium import folium_static
import folium
import streamlit as st
import pandas as pd
from st_pages import show_pages, Page
import datetime

st.set_page_config(page_title = "Project_Map")
st.title("Project Map")

if 'project_info' not in st.session_state:
    projects = pd.DataFrame(st.session_state.cursor.get_project_info())
    st.session_state['project_info']  = projects
# showing project information columns
st.write(st.session_state.project_info)

# retrieve function for project click pop-up 
st.write(st.session_state.cursor.get_tx_under_proj(3095))

################################FILTER############################################
on = st.toggle('Show Filter')

if on:
    #date selector
    d = st.date_input(
        "Select Transaction Time range",
        (st.session_state['transaction_date_start'], st.session_state['transaction_date_end']),
        datetime.date(2019,1,1),
        datetime.datetime.now(),
        format="MM.DD.YYYY",
    )
    
    #amenity selector
    st.session_state['amenities_list'] = st.multiselect(
        'Show Amenities: ',
        st.session_state.filter.amenities_list,
        st.session_state['amenities_list'])
    
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


####################################set_icon########################################

Carpark_kw = {'prefix':'fa',"color": "blue", "icon":"square-parking"}
MRT_kw = {'prefix':'fa',"color": "blue", "icon":"train"}
Gym_kw = {'prefix':'fa',"color": "blue", "icon":"dumbbell"}
Hawker_kw = {'prefix':'fa',"color": "blue", "icon":"utensils"}
Kindergarten_kw = {'prefix':'fa',"color": "blue", "icon":"child-reaching"}
Park_kw = {'prefix':'fa',"color": "blue", "icon":"tree"}
Pharmacy_kw = {'prefix':'fa',"color": "blue", "icon":"capsules"}
Supermarket_kw = {'prefix':'fa',"color": "blue", "icon":"shop"}

amenity_kw_ref = {
    'MRT':MRT_kw,
    'LRT':MRT_kw,
    'Gym':Gym_kw,
    'Hawker':Hawker_kw,
    'Kindergarten':Kindergarten_kw,
    'Park':Park_kw,
    'Carpark':Carpark_kw,
    'Pharmacy':Pharmacy_kw,
    'Supermarket':Supermarket_kw,
}

Project_kw = {'prefix':'fa',"color": "green", "icon":"house"}

#####################################filter_data#######################################


############################################################################
m = folium.Map(location=[1.3521,103.8198], #center of singapore
            zoom_start = 11) #initialize the map in singapore
 
for _,row in st.session_state.amenities.iterrows():
    type = row['amenity_type']
    if type in st.session_state.amenities_list:
        icon = folium.Icon(**amenity_kw_ref[type])
        folium.Marker(location=[row['lat'], row['long']], icon=icon, tooltip=row['amenity_name']).add_to(m)


#TODO add project lat long when data is available
folium_static(m)
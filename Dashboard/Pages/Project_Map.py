from streamlit_folium import folium_static
import folium
import streamlit as st
import pandas as pd
from st_pages import show_pages, Page
import datetime
import branca

st.set_page_config(page_title = "Project_Map",    layout='wide')
st.title("Project Transaction Map")
st.write("This page provide information on the project and the amenities in the same district. Please select the district and amenities in interest.")

if 'project_info' not in st.session_state:
    projects = pd.DataFrame(st.session_state.cursor.get_project_info())
    st.session_state['project_info']  = projects

if "district_list_map" not in st.session_state:
    st.session_state['district_list_map']  = []

################################FILTER############################################

def reset_districts():
    st.session_state.district_list = []


with st.expander(label="Filter values", expanded=True):
    with st.form('test'):
        #Room type filter is removed as we are showing proj
        district_list_selected = []
        amenity_list_selected = []
        
        #----------------------------------------------------------------------------------------------
        st.write('Show Project for District:')
        cols = st.columns(4)
        district_list_value = []
        for d in st.session_state.filter.district_list:
            district_list_value.append(d in st.session_state['district_list_map'] )
        
        for i in range(len(district_list_value)):
            with cols[i%4]:
            # Display the news by markdown
                district_list_value[i] = st.checkbox(st.session_state.filter.district_list[i], value=district_list_value[i])

        for i in range(len(district_list_value)):
            if district_list_value[i]:
                district_list_selected.append(st.session_state.filter.district_list[i])
        #----------------------------------------------------------------------------------------------
        st.write('Amenities:')
        cols = st.columns(4)
    
        amenitiy_list_value = []
        for a in st.session_state.filter.amenities_list:
            amenitiy_list_value.append(a in st.session_state['amenities_list'] )
        
        for i in range(len(amenitiy_list_value)):
            with cols[i%4]:
            # Display the news by markdown
                amenitiy_list_value[i] = st.checkbox(st.session_state.filter.amenities_list[i], value=amenitiy_list_value[i])

        for i in range(len(amenitiy_list_value)):
            if amenitiy_list_value[i]:
                amenity_list_selected.append(st.session_state.filter.amenities_list[i])
        
        #----------------------------------------------------------------------------------------------
        _, submit_button = st.columns([0.8,0.2])
        with submit_button:
            submitted = st.form_submit_button('Confirm')
            if submitted:
                st.session_state['district_list_map'] = district_list_selected
                st.session_state['amenities_list'] = amenity_list_selected
            


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

#####################################popup#######################################
def get_popup(proj_id):
    transaction = st.session_state.cursor.get_tx_under_proj(proj_id)
    html = f'''
            <body>
            <b>Name:</b> {row['project_name']}<br>
            <b>Number of transaction:</b> {len(transaction)}<br>
            <b>Average Price:</b> {row['proj_avg_price_per_sqm']}<br>
            <br>
            <strong>Transaction History:</strong>
            <div class="scrollable-list">
            <ul>
        '''
        
    for trx in transaction:
        html += f"""
        <li class="project-item">
        <strong>Transaction Date:</strong> {trx['transaction_date']}<br>
        <strong>Type of Sale:</strong> {trx['type_of_sale']}<br>
        <strong>Foor Area (sqm):</strong> {trx['floor_area']}<br>
        <strong>Price:</strong> {trx['price']}<br>
    </li><br>
        """
    
    html += """</ul></div></body>
    """
    iframe = branca.element.IFrame(html=html, width=500, height=300)
    return folium.Popup(iframe, max_width=500)

############################################################################

m = folium.Map(location=[1.3521,103.8198], #center of singapore
        zoom_start = 11) #initialize the map in singapore


#plot project    
for _,row in st.session_state.project_info.iterrows():
    district = row['district_name']
    if district in st.session_state['district_list_map']:
        icon = folium.Icon(**Project_kw)

        folium.Marker(location=[row['lat'], row['long']], icon=icon, tooltip=row['project_name'],popup=get_popup(row['project_id']), lazy = True).add_to(m)

#plot amenities 
for _,row in st.session_state.amenities.iterrows():
    type = row['amenity_type']
    district = row['district_name']
    if type in st.session_state.amenities_list and district in st.session_state['district_list_map']:
        icon = folium.Icon(**amenity_kw_ref[type])
        folium.Marker(location=[row['lat'], row['long']], icon=icon, tooltip=row['amenity_name']).add_to(m)

#TODO add project lat long when data is available
folium_static(m)


        
        
# st.write('TESTING')
# # showing project information columns
# st.write(st.session_state.project_info)

# st.write(st.session_state.amenities)
# # retrieve function for project click pop-up 
# st.write(st.session_state.cursor.get_tx_under_proj(3095))
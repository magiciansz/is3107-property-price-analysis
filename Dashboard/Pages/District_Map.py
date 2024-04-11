from streamlit_folium import folium_static
import streamlit as st
import pandas as pd
import folium
import ast
from create_init import init_session_state
from branca.colormap import linear
import sys
from pathlib import Path
current_script_path = Path(__file__).resolve()
parent_directory = current_script_path.parent.parent
if str(parent_directory) not in sys.path:
    sys.path.append(str(parent_directory))
from etl.RetrieveDB import RetrieveDB

if 'cursor' not in st.session_state:
    try:
        # cursor = RetrieveDB(db_connect_type = 'LOCAL')
        cursor = RetrieveDB(db_connect_type = 'IAM')
        st.session_state.cursor = cursor
        init_session_state()
    except Exception as e:
        st.warning("Cursor is not established")
        st.write(e)
        st.stop()
    

st.set_page_config(page_title = "District_Map")
st.session_state.district_list = st.session_state.filter.district_list
# if 'district_info' not in st.session_state:
#     districts = pd.DataFrame(st.session_state.cursor.get_districts())
#     # format: ['id','district_name', 'coordinates']
#     st.session_state['district_info']  = districts

if 'district_popup' not in st.session_state:
    district_pop = pd.DataFrame(st.session_state.cursor.get_district_popup())
    st.session_state.district_popup = district_pop

def get_geojson_district(districts):
    features = []
    for idx, row in districts.iterrows():
        current_district = row['district_name']

        feature = {
            "type": "Feature",
            "properties": {
                "name": row['district_name'],
                'number of projects': row['no_of_projects'],
                'average price': row['avg_dist_price_per_sqm']
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [ast.literal_eval(row['coordinates'])]
            }
        }
        features.append(feature)

    geojson_district = {
        "type": "FeatureCollection",
        "features": features
    }

    return geojson_district

def plot_price_per_district(data):
    districts = get_geojson_district(data)
    
    colormap = linear.YlGn_09.scale(
    data.avg_dist_price_per_sqm.min(), data.avg_dist_price_per_sqm.max())



    m = folium.Map(location=[1.3521,103.8198], #center of singapore
               zoom_start = 11) #initialize the map in singapore
    

    folium.GeoJson(
        districts,
        style_function= lambda feature: {
            'fillColor' :(
                colormap(feature['properties']['average price']) if feature['properties']['name'] in st.session_state.district_list else 'white'),
            "color": "black",
            "fillOpacity": 0.5,
            "weight": 1,
        },
        highlight_function=lambda feature: {
            "fillColor": (
                "green" if feature['properties']['name'] in st.session_state.district_list else 'white'
        ),},
        popup= folium.GeoJsonPopup(fields=["name",'number of projects', 'average price']),
        popup_keep_highlighted= True,
    ).add_to(m)

    return m


st.title('Singapore District Map')
m = plot_price_per_district(st.session_state.district_popup)
# Display the map
folium_static(m)
    

colormap = linear.YlGn_09.scale(
    st.session_state.district_popup.avg_dist_price_per_sqm.min(), st.session_state.district_popup.avg_dist_price_per_sqm.max())
st.write(colormap)


st.write('TESTING')
# example showing info to be shown on district popup
st.write(st.session_state.district_popup.head(2))

# # example showing overall district transaction info for filters
st.write(pd.DataFrame(st.session_state.cursor.get_district_tx_info()).head())

# # example showing district df
# st.write(st.session_state.district_info)

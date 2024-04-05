from streamlit_folium import folium_static
import streamlit as st
import pandas as pd
import folium
import ast
from create_init import init_session_state

st.set_page_config(page_title = "District_Map")

# if 'district_info' not in st.session_state:
#     districts = pd.DataFrame(st.session_state.cursor.get_districts())
#     # format: ['id','district_name', 'coordinates']
#     st.session_state['district_info']  = districts

if 'cursor' not in st.session_state:
    init_session_state()
    
if 'district_popup' not in st.session_state:
    district_pop = pd.DataFrame(st.session_state.cursor.get_district_popup())
    st.session_state.district_popup = district_pop

def get_geojson_district(districts):
    features = []
    for idx, row in districts.iterrows():
        feature = {
            "type": "Feature",
            "properties": {
                "name": row['district_name'],
                'number of projects': row['no_of_projects'],
                'average price': row['avg_dist_price_per_sqft']
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
    #TODO colormap with average price 
    districts = get_geojson_district(data)
    
    # colormap = linear.YlGn_09.scale(
    #     320000, 900000000
    # )


    m = folium.Map(location=[1.3521,103.8198], #center of singapore
               zoom_start = 11) #initialize the map in singapore

    folium.GeoJson(
        districts,
        highlight_function=lambda feature: {
            "fillColor": 'green',
        },
        style_function= lambda feature: {
            'fillColor' :'grey',
            "color": "black",
            "weight": 1,
        },
        popup= folium.GeoJsonPopup(fields=["name",'number of projects', 'average price']),
        popup_keep_highlighted=True,
    ).add_to(m)

    return m


st.title('Singapore District Map')
m = plot_price_per_district(st.session_state.district_popup)
# Display the map
folium_static(m)
    


st.write('TESTING')
# example showing info to be shown on district popup
st.write(st.session_state.district_popup.head(2))

# # example showing overall district transaction info for filters
st.write(pd.DataFrame(st.session_state.cursor.get_district_tx_info()).head())

# # example showing district df
# st.write(st.session_state.district_info)

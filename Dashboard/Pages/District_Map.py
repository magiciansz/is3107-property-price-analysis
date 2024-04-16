import streamlit as st
import pandas as pd
import folium
from streamlit_folium import folium_static
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
import altair as alt
import json

st.set_page_config(page_title = "District_Map", layout='wide')

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
    

st.session_state.district_list = st.session_state.filter.district_list
district_tx_info = pd.DataFrame(st.session_state.cursor.get_district_tx_info())


if 'district_popup' not in st.session_state:
    district_pop = pd.DataFrame(st.session_state.cursor.get_district_popup())
    st.session_state.district_popup = district_pop

def district_avg_price_over_time(district_name, num_projects, avg_price):
    focal_district = district_tx_info.loc[district_tx_info['district_name'] ==  str(district_name)][['district_name', 'transaction_year', 'transaction_month', 'dist_price_per_sqm']]
    focal_district['transaction_year']= focal_district['transaction_year'].astype(str)
    focal_district['transaction_month'] = focal_district['transaction_month'].astype(str)
    focal_district['YearMonth'] = pd.to_datetime(focal_district['transaction_year']+ focal_district['transaction_month'], format='%Y%m')
    # print(focal_district)
    
    focal_district = focal_district.to_dict("records")    
    YearMonth = [record['YearMonth'] for record in focal_district]
    avg_prices = [record['dist_price_per_sqm'] for record in focal_district]
    # print(YearMonth)
    # print(avg_prices)
    x = YearMonth

    to_plot = pd.DataFrame({
                        'Time': x,
                        'Average Price per Sqm': avg_prices}
                        )

    title_str = f'{district_name}: {num_projects} Projects,    Average Price per sqm: {avg_price:.2f}'
    
    chart = alt.Chart(to_plot,title= title_str).mark_line().encode( alt.X('Time:T'),
                    y= 'Average Price per Sqm'
                ).properties(width = 300, height = 250)
    return chart

# TESTING
# st.write(district_avg_price_over_time('BEDOK'))


def get_geojson_district(districts):
    features = []
    for _, row in districts.iterrows():
        current_district = row['district_name']

        feature = {
            "type": "Feature",
            "properties": {
                "name": current_district,
                'number of projects': row['no_of_projects'],
                'average price': row['avg_dist_price_per_sqm'],
                'vega_lite_json': district_avg_price_over_time(current_district, row['no_of_projects'], row['avg_dist_price_per_sqm']).to_json()
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
    
    colormap = linear.YlOrRd_06.scale(data.avg_dist_price_per_sqm.min(), data.avg_dist_price_per_sqm.max())

    m = folium.Map(location=[1.3521, 103.8198], zoom_start=11)  # Center of Singapore
    
    for feature in districts['features']:
        popup_content = f"""
            <b>Name:</b> {feature['properties']['name']}  <br>
            <b>Number of Projects:</b> {feature['properties']['number of projects']}   <br>
            <b>Average Price:</b> {feature['properties']['average price']}   <br>
            <br>
            <div id='chart'></div>
        """
        
        popup = folium.Popup(popup_content, max_width=400)

        # Add VegaLite chart to the popup
        vega_lite_spec = json.loads(feature['properties']['vega_lite_json'])
        folium.VegaLite(vega_lite_spec, width=900, height=300).add_to(popup)

        # Add GeoJson layer to the map
        folium.GeoJson(
            feature,
            style_function=lambda feature: {
                'fillColor': colormap(feature['properties']['average price']) if feature['properties']['name'] in st.session_state.district_list else 'white',
                "color": "black",
                "fillOpacity": 0.5,
                "weight": 1,
            },
            highlight_function=lambda feature: {
                "fillColor": "green" if feature['properties']['name'] in st.session_state.district_list else 'white'
            },
            popup=popup,
            tooltip= feature['properties']['name'],
            popup_keep_highlighted=True,
        ).add_to(m)

    return m


st.title('Housing Price Breakdown by District')
st.write("This page provide information on the price trend per district.")
st.write("")
st.write("Average price per sqm scale:")

colormap = linear.YlOrRd_06.scale(
    st.session_state.district_popup.avg_dist_price_per_sqm.min(), st.session_state.district_popup.avg_dist_price_per_sqm.max())
st.write(colormap)
m = plot_price_per_district(st.session_state.district_popup)
# Display the map
folium_static(m)





# st.write('TESTING')
# # example showing info to be shown on district popup
# st.write(st.session_state.district_popup.head(2))

# # # example showing overall district transaction info for filters
# st.write(pd.DataFrame(st.session_state.cursor.get_district_tx_info()).head())

# # example showing district df
# st.write(st.session_state.district_info)

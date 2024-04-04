from streamlit_folium import folium_static
import streamlit as st
import pandas as pd
import visualizations as v

st.set_page_config(page_title = "District_Map")

def show_folium_map():
    st.title('Singapore District Map')
    m = v.plot_price_per_district(st.session_state.districts)
    # Display the map
    folium_static(m)
    
show_folium_map()
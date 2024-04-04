from streamlit_folium import folium_static
import folium
import streamlit as st
import pandas as pd
from st_pages import show_pages, Page


st.set_page_config(page_title = "Project_Map")

cursor.get_amenities()
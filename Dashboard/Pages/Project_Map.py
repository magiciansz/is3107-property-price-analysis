from streamlit_folium import folium_static
import streamlit as st
import pandas as pd
import numpy as np
from st_pages import show_pages, Page
import sys
from pathlib import Path
import visualizations as v

st.set_page_config(page_title = "Project_Map")
# st.session_state.district_ids = [1, 2, 3]
# st.write(st.session_state.cursor.get_proj_from_selection())
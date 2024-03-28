import streamlit as st
import pandas as pd
import numpy as np
from streamlit_folium import folium_static
import matplotlib.pyplot as plt
import visualizations as v

#py -m streamlit run Dashboard/app.py

# Function for Page 1 - Random Line Chart
def show_line_chart():
    st.title('Random Line Chart')
    # Generate random data for line chart
    data = pd.DataFrame(
        np.random.randn(20, 3),
        columns=['A', 'B', 'C']
    )

    st.line_chart(data)

# Function for Page 2 - Folium Map
def show_folium_map():
    st.title('Singapore District Map')
    m = v.plot_price_per_district()
    # Display the map
    folium_static(m)

# Sidebar navigation
st.sidebar.title('Navigation')
page = st.sidebar.radio("Choose a page", ['Random Line Chart', 'District Map', 'Project Map'])

if page == 'Random Line Chart':
    st.pyplot(v.plot_price_over_time())
elif page == 'District Map':
    show_folium_map()
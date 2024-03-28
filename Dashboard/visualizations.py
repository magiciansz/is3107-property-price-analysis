import folium
import json
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from branca.colormap import linear

def get_geojson_district():
    #TODO: change input to database
    f = open('districts_final.json') 
    districts = json.load(f)
    features = []
    count = 1
    for district_id, coords in districts['coord_list'].items():
        count += 1
        district_name = districts['pln_area_n'][district_id]
        feature = {
            "type": "Feature",
            "properties": {
                "name": district_name,
                'average_price': count
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [coords]
            }
        }
        features.append(feature)

    geojson_district = {
        "type": "FeatureCollection",
        "features": features
    }
    
    return geojson_district

def plot_price_per_district():
    #TODO colormap with average price 
    districts = get_geojson_district()
    
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
        popup= folium.GeoJsonPopup(fields=["name",'average_price']),
        popup_keep_highlighted=True,
    ).add_to(m)

    return m

def plot_price_over_time():
    #TODO: can provide district_list to add price line
    ura_data = pd.read_csv('ura_data.csv')
    ura_data['contractDate'] = ura_data['contractDate'].astype(str)
    ura_data['Year'], ura_data['Month'] = ura_data['contractDate'].str[-2:], ura_data['contractDate'].str[:-2]
    ura_data['YearMonth'] = pd.to_datetime(ura_data['Year'] + ura_data['Month'], format='%y%m')
    ura_data['YearMonth_str'] = ura_data['Year'] + '-' + ura_data['Month'].str.zfill(2)
    monthly_median = ura_data.groupby('YearMonth')['price'].median().reset_index()

    ura_data.sort_values('YearMonth', inplace=True)
    fig, ax = plt.subplots(figsize=(15, 10))
    
    sns.lineplot(data=ura_data, x='YearMonth', y='price',ax=ax)
    sns.lineplot(data=monthly_median, x='YearMonth', y='price',ax=ax)
    ax.tick_params(axis='x', labelrotation=45)
    
    return fig
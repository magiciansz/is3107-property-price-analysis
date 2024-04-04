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

def plot_price_over_time(data, start_date, end_date, room_type_list, district_list, price_per_sqft_range):
    #TODO: can provide district_list to add price line
    
    start_date,end_date = pd.Timestamp(start_date), pd.Timestamp(end_date)
    
    #filter with timerange
    data['transaction_year']= data['transaction_year'].astype(str)
    data['transaction_month'] = data['transaction_month'].astype(str)
    data['YearMonth'] = pd.to_datetime(data['transaction_year']+ data['transaction_month'], format='%Y%m')
    data = data[(data['YearMonth'] >= start_date) & (data['YearMonth'] <= end_date)]
    
    # filter with disctrict 
    #TODO
    
    #filter with room type
    data = data[data['property_type'].isin(room_type_list)]
    
    #filter with price_per_sqft_range
    data = data[(data['price_per_sqft'] >= price_per_sqft_range[0]) & (data['price_per_sqft'] <= price_per_sqft_range[1])]
    
    
    data['YearMonth_str'] = data['transaction_year'] + '-' + data['transaction_month'].str.zfill(2)
    monthly_median = data.groupby('YearMonth')['price'].median().reset_index()


    data.sort_values('YearMonth', inplace=True)
    fig, ax = plt.subplots(figsize=(15, 10))
    
    sns.lineplot(data=data, x='YearMonth', y='price',ax=ax)
    sns.lineplot(data=monthly_median, x='YearMonth', y='price',ax=ax)
    ax.tick_params(axis='x', labelrotation=45)
    
    return fig


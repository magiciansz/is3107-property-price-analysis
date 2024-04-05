import folium
import ast
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from branca.colormap import linear

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

def plot_price_over_time(data, start_date, end_date, room_type_list, district_list, price_per_sqft_range):
    #TODO: can provide district_list to add price line
    
    start_date,end_date = pd.Timestamp(start_date), pd.Timestamp(end_date)
    
    #filter with timerange
    data['transaction_year']= data['transaction_year'].astype(str)
    data['transaction_month'] = data['transaction_month'].astype(str)
    data['YearMonth'] = pd.to_datetime(data['transaction_year']+ data['transaction_month'], format='%Y%m')
    data = data[(data['YearMonth'] >= start_date) & (data['YearMonth'] <= end_date)]
    
    # filter with disctrict 
    data = data[data['district_name'].isin(district_list)]
    
    #filter with room type
    data = data[data['property_type'].isin(room_type_list)]
    
    #filter with price_per_sqft_range
    data = data[(data['price_per_sqft'] >= price_per_sqft_range[0]) & (data['price_per_sqft'] <= price_per_sqft_range[1])] 
    
    data['YearMonth_str'] = data['transaction_year'] + '-' + data['transaction_month'].str.zfill(2)
    monthly_median = data.groupby('YearMonth')['price'].median().reset_index()
    data.sort_values('YearMonth', inplace=True)

    fig, ax = plt.subplots(figsize=(15, 10))
    sns.lineplot(data=data, x='YearMonth', y='price',ax=ax, label = 'Monthly Prices')
    sns.lineplot(data=monthly_median, x='YearMonth', y='price',ax=ax, label = 'Monthly Median Prices')
    ax.set_xlabel('Date', fontsize='x-large')
    ax.set_ylabel('Price', fontsize='x-large')
    ax.set_title('Singapore Property Price Over Time', fontsize='xx-large')
    ax.tick_params(axis='x', labelrotation=45)
    ax.legend(fontsize='large')
    
    return fig


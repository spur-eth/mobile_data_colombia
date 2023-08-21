import os
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd
import dask.dataframe as dd
import folium as folium

def plot_frac_data_on_map(shapefile_path, ddf, frac=0.001):
    user_data = ddf.sample(frac=frac).compute()
    map_df=gpd.read_file(shapefile_path)

    # Create a folium map centered on the user's latitude and longitude
    center_lat = user_data.iloc[0]['lat']
    center_lng = user_data.iloc[0]['lng']
    map_obj = folium.Map(location=[center_lat, center_lng], zoom_start=10)

    # Add markers for each measurement
    for index, row in user_data.iterrows():
        folium.Circle(radius=0.1, location=[row['lat'], row['lng']], color="green", fill=False).add_to(map_obj)

    # Add the shapefile as an overlay
    style = {'fillColor': '#778899', 'color': '#778899', 'weight': 1.5, 'fillOpacity': 0.2} 
    folium.GeoJson(map_df, style_function = lambda x: style).add_to(map_obj)
    return map_obj, user_data

def plot_user_on_map(shapefile_path, df, lat_col, lng_col, user_id):
    if type(df) is dd.DataFrame: 
        user_data = df[df['uid']==user_id].copy().reset_index().compute() #need to add the compute if input is a ddf
    else: 
        user_data = df[df['uid']==user_id].copy().reset_index()
        
    map_df=gpd.read_file(shapefile_path)

    # Create a folium map centered on the user's latitude and longitude
    center_lat = user_data.iloc[0][lat_col]
    center_lng = user_data.iloc[0][lng_col]
    map_obj = folium.Map(location=[center_lat, center_lng], zoom_start=12)

    # Add markers for each measurement
    for index, row in user_data.iterrows():
        folium.Circle(radius=0.1, location=[row[lat_col], row[lng_col]], color="red", fill=False).add_to(map_obj)

    # Add the shapefile as an overlay
    style = {'fillColor': '#778899', 'color': '#778899', 'weight': 1.5, 'fillOpacity': 0.2} 
    folium.GeoJson(map_df, style_function = lambda x: style).add_to(map_obj)
    return map_obj, user_data

def plot_homes_in_zones(shapefile_path, df, zones_col, zones_of_interest, lat_col='lat', lng_col='lng',
                         colors=['green', 'yellow', 'crimson', 'blue', 'purple', 'orange']):
    user_data = df 
    map_df=gpd.read_file(shapefile_path)

    # Create a folium map centered on the user's latitude and longitude
    center_lat = user_data.iloc[0][lat_col]
    center_lng = user_data.iloc[0][lng_col]
    map_obj = folium.Map(location=[center_lat, center_lng], zoom_start=12)

    # Add markers for each measurement
    for index, row in user_data.iterrows():
        #folium.Marker([row[lat_col], row[lng_col]]).add_to(map_obj)
        for i in range(0, len(zones_of_interest)):
            if row[zones_col] == zones_of_interest[i]:
                folium.Circle(radius=0.5, location=[row[lat_col], row[lng_col]], color=colors[i], fill=False).add_to(map_obj)
            else: 
                #print('localidad not found')
                pass
            
    # Add the shapefile as an overlay
    style = {'fillColor': '#778899', 'color': '#778899', 'weight': 1.5, 'fillOpacity': 0.2} 
    folium.GeoJson(map_df, style_function = lambda x: style).add_to(map_obj)
    return map_obj, user_data
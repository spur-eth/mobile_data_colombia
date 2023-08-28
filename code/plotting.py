import os
os.environ['USE_PYGEOS'] = '0'
import geopandas as gpd
import numpy as np
import pandas as pd
import dask.dataframe as dd
import folium as folium
import matplotlib.pyplot as plt

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

def plot_stacked_bar_from_csv(filename, out_file, colormap='viridis'):
    # Read the csv data
    df = pd.read_csv(filename)
    
    # Separate the data into control and treatment groups
    control = df[df['Group'] == 'Control'].sort_values('category')
    treatment = df[df['Group'] == 'Treatment'].sort_values('category')
    
    # Extract category and proportion data
    categories = control['category'].values
    control_props = control['proportion'].values
    treatment_props = treatment['proportion'].values
    
    # Get colors from the specified colormap
    colors = plt.get_cmap(colormap)(np.linspace(0, 1, len(categories)))
    
    # Plotting
    plt.figure(figsize=(4, 6))
    
    # Plot for Control
    plt.bar(['Control'], control_props[0], label=categories[0], color=colors[0], alpha=0.7)
    bottom_control = control_props[0]
    for i in range(1, len(categories)):
        plt.bar(['Control'], control_props[i], bottom=bottom_control, label=categories[i], color=colors[i], alpha=0.7)
        bottom_control += control_props[i]
        
    # Plot for Treatment
    plt.bar(['Treatment'], treatment_props[0], color=colors[0], alpha=0.7)
    bottom_treatment = treatment_props[0]
    for i in range(1, len(categories)):
        plt.bar(['Treatment'], treatment_props[i], bottom=bottom_treatment, color=colors[i], alpha=0.7)
        bottom_treatment += treatment_props[i]
    
    # Draw lines connecting the segments
    cum_control = 0
    cum_treatment = 0
    for c_prop, t_prop in zip(control_props, treatment_props):
        plt.plot(['Control', 'Treatment'], [cum_control + c_prop, cum_treatment + t_prop], 'k-', alpha=0.5)
        cum_control += c_prop
        cum_treatment += t_prop
    
    # Tufte-style aesthetics
    plt.gca().spines['right'].set_visible(False)
    plt.gca().spines['top'].set_visible(False)
    plt.gca().spines['left'].set_visible(False)
    plt.gca().yaxis.set_ticks_position('none') 
    plt.gca().xaxis.set_ticks_position('bottom') 
    plt.yticks([])
    plt.title('Proportions by Group and Category', fontsize=12)
    plt.legend(bbox_to_anchor=(1.05, 0.95), loc='upper left', frameon=False)
    plt.tight_layout()
    plt.savefig(f'{out_file}.pdf', dpi=200, bbox_inches='tight')
    plt.show()
    
from geopy.distance import geodesic
from skmob.preprocessing import detection

from tqdm.notebook import tqdm

def geodesic_distance(row):
    """
    Calculate the geodesic distance between two sets of GPS coordinates using geopy.
    :param row: dataframe row with columns 'lat_home', 'lng_home', 'lat_work', 'lng_work'
    :return: distance in kilometers
    """
    home_coords = (row['lat_home'], row['lng_home'])
    work_coords = (row['lat_work'], row['lng_work'])
    return geodesic(home_coords, work_coords).kilometers

def calculate_visits_min_minutes(tdf, visit_durations, out_dir, stop_radius_factor=0.5, spatial_radius_km=0.2, no_data_for_minutes=1e12): 
    i = 0
    expected_iter = len(visit_durations)
    pbar_process = tqdm(total=(expected_iter))
    pbar_write = tqdm(total=(expected_iter))

    while i < expected_iter:
        number_min = visit_durations[i]
        pbar_process.set_description(f"Computing visits where users spent at least {number_min} minutes")
        visit_df = detection.stay_locations(tdf, stop_radius_factor=0.5, minutes_for_a_stop=number_min, no_data_for_minutes=no_data_for_minutes, spatial_radius_km=0.2, leaving_time=True)
        print(f'The number of stops for {number_min} minutes in the dataset is {len(visit_df)}')
        pbar_process.update(1)
        outfilename = f'users_living_in_sel_zat_visits_atleast_{number_min}min_nodatafor_{no_data_for_minutes}_minutes'
        pbar_write.set_description(f"Writing user data for {number_min} minute visits")
        write_to_pq(visit_df, out_dir, filename=outfilename)
        visit_df.to_csv(f'{out_dir}{outfilename}.csv', index=False)
        pbar_write.update(1)
        i += 1

    pbar_process.close()
    pbar_write.close()

    return 

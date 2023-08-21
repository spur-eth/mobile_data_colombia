from geopy.distance import geodesic

def geodesic_distance(row):
    """
    Calculate the geodesic distance between two sets of GPS coordinates using geopy.
    :param row: dataframe row with columns 'lat_home', 'lng_home', 'lat_work', 'lng_work'
    :return: distance in kilometers
    """
    home_coords = (row['lat_home'], row['lng_home'])
    work_coords = (row['lat_work'], row['lng_work'])
    return geodesic(home_coords, work_coords).kilometers

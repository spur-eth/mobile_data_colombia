import glob
import os
import math
import pandas as pd 
import dask.dataframe as dd
import geopandas as gpd
import dask_geopandas as ddgpd
from mobilkit.loader import crop_spatial as mk_crop_spatial
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from skmob import TrajDataFrame
from skmob.measures.individual import home_location

from tqdm.notebook import tqdm

#### VARIABLES FOR DATA LOADING AND PREPROCESSING ####
# These are based on the data we have and would have to be modified if the format of the files changed/adapted for other data 

initial_cols=['device_id', 'id_type', 'latitude', 'longitude', 'horizontal_accuracy', 'timestamp',  'ip_address', 'device_os', 'country', 'unknown_2', 'geohash']
sel_cols = ['device_id','latitude','longitude','timestamp','geohash','horizontal_accuracy']
final_cols = ['uid','lat','lng','datetime','geohash','horizontal_accuracy']

datatypes = {'device_id': 'object', 'id_type': 'object', 'latitude': 'float64', 'longitude': 'float64', 
             'horizontal_accuracy': 'float32', 'timestamp': 'int64', 'ip_address': 'object', 
             'device_os': 'object', 'country': 'object', 'unknown_2': 'int64', 'geohash': 'object'}

# boundary box that roughly captures the larger county of Bogota
minlon = -74.453
maxlon = -73.992
minlat = 3.727
maxlat = 4.835

#### FUNCTIONS FOR DATA PREPROCESSING AND FILTERING ####

def get_days(data_folder):
    """Assuming a directory organized as a month's worth of days with files in each directory like "day=01", etc """
    day_dirs = glob.glob((data_folder + '*/'))
    return day_dirs

def get_files(data_folder, day_dir):
    """Assuming a dir corresponding to and named for a day day_dir, (e.g. "day=01") within the data_folder with that day's mobile data files."""
    day = day_dir.split(data_folder)[1]
    filepaths = [file_path for file_path in glob.glob(os.path.join(day_dir, '*')) if not file_path.endswith('.gz')] # select all the non-zipped mobile data files
    return filepaths, day

def load_data(filepaths, initial_cols, sel_cols, final_cols, datatypes=datatypes): 
    """Load in the mobile data and specify the columns"""
    ddf = dd.read_csv(filepaths, dtype=datatypes, names=initial_cols)
    ddf = ddf[sel_cols]
    ddf.columns = final_cols
    return ddf 

def convert_datetime(ddf: dd.DataFrame): #needs work
    """Process timestamp to datetime for dataframe with a "datatime" column with timestamp values. """
    ddf["datetime"] = dd.to_datetime(ddf["datetime"], unit='ms', errors='coerce')
    ddf["datetime"] = ddf["datetime"].dt.tz_localize('UTC').dt.tz_convert('America/Bogota')
    return ddf

def find_within_box(ddf, minlon, maxlon, minlat, maxlat):
    """Quick way to filter out points not in a particular rectangular region."""
    box=[minlon,minlat,maxlon,maxlat]
    filtered_ddf = mk_crop_spatial(ddf, box).reset_index()
    return filtered_ddf

def find_within_regions(ddf, gdf, lng_col='lng', lat_col='lat'): 
    ddf = ddgpd.from_dask_dataframe(ddf, geometry=ddgpd.points_from_xy(ddf, lng_col, lat_col))
    ddf = ddf.set_crs(gdf.crs)
    ddf_in_regions = ddf.sjoin(gdf, predicate="within")
    return ddf_in_regions

def filter_data_for_day(filepaths, gdf):
    ddf = load_data(filepaths, initial_cols, sel_cols, final_cols)
    within_box_ddf = find_within_box(ddf, minlon, maxlon, minlat, maxlat)
    ddf_in_regions = find_within_regions(within_box_ddf, gdf=gdf)
    # cols must be in in the ddf/shapefile
    cols = ['uid', 'lat', 'lng', 'datetime', 'geohash', 'horizontal_accuracy', 'index_right', 'LOCNombre', 'UTAMNombre']
    ddf_in_regions = ddf_in_regions[cols]
    final_colnames = ['uid', 'lat', 'lng', 'datetime', 'geohash', 'horizontal_accuracy', 'index_from_shp', 'LOCNombre', 'UTAMNombre']
    ddf_in_regions.columns = final_colnames
    ddf_in_regions = convert_datetime(ddf_in_regions)
    return ddf_in_regions

def write_day(ddf_in_regions, out_dir, year, month, day):
    filename = f'{out_dir}{year}_{month}_{day}.parquet'
    ddf_in_regions.to_parquet(path=filename, write_index=False) 
    return 

def write_to_pq(df, out_dir, filename, write_subdir='', write_csv=False): 
    if len(write_subdir) > 0: 
        out_dir = f'{out_dir}{write_subdir}/'
        try: 
            os.makedirs(out_dir)
        except FileExistsError: 
            pass
    table_name = f'{out_dir}{filename}.parquet'
    table = pa.Table.from_pandas(df)
    pq.write_table(table, table_name)
    if write_csv == True: 
        df.to_csv(f'{out_dir}{filename}.csv', index=False)
    return

def from_month_write_filter_days_to_pq(data_folder: str, gdf, data_year: str, year: str, out_dir:str):
    month = data_folder.split(data_year)[1].split('/')[0]
    day_dirs = get_days(data_folder)
    for i in tqdm(range(0,len(day_dirs)), desc=f'Files from {year} {month} processed'): 
        filepaths, day = get_files(data_folder, day_dirs[i])
        ddf_in_regions = filter_data_for_day(filepaths, gdf=gdf)
        day_name = day.split('/')[0]
        filename = f"{year}_{month}_{day_name}.parquet"
        write_to_pq(df=ddf_in_regions.compute(), out_dir=out_dir, filename=filename)
    return

def get_df_for_sel_users(dataset, sel_users, cols):
    table = dataset.to_table(columns=cols, filter=ds.field('uid').isin(sel_users))
    df = table.to_pandas().reset_index()
    df = df.drop(['index'], axis=1)
    return df

def find_home_lat_lng(df, start_night='22:00', end_night='06:00'):
    traj_df = TrajDataFrame(df, user_id='uid', latitude='lat', longitude='lng', datetime='datetime')
    hl_df = home_location(traj_df, start_night=start_night, end_night=end_night)
    return hl_df 

def assign_points_to_regions(points_df, regions_gdf, cols_to_keep, point_lat_col='lat', point_lng_col='lng'):
    geometry = gpd.points_from_xy(points_df[point_lng_col], points_df[point_lat_col])
    points_gdf = gpd.GeoDataFrame(points_df, geometry=geometry, crs=regions_gdf.crs)
    # Perform spatial join to assign points to regions
    joined_gdf = gpd.sjoin(points_gdf, regions_gdf, how="left", op="within")
    joined_gdf = joined_gdf[cols_to_keep]
    return joined_gdf

def compute_home_lat_lngs_for_users(uids_pass_qc, pq_dir, out_dir, regions_gdf, num_users=20000, 
                                    cols = ['uid', 'datetime', 'lat', 'lng'], 
                                    gdf_cols=['Area', 'MUNCod', 'NOMMun', 'ZAT', 'UTAM', 'stratum'], 
                                    start_time='22:00', end_time='06:00', goal='home'):
    
    pings_paths = glob.glob((pq_dir + '*.parquet'))
    dataset = ds.dataset(pings_paths, format="parquet")
    total_users = len(uids_pass_qc)
    user_count = 0

    expected_iter = math.ceil(len(uids_pass_qc)/num_users)
    pbar_load = tqdm(total=(expected_iter))
    pbar_process = tqdm(total=(expected_iter))
    pbar_write = tqdm(total=(expected_iter))

    while (total_users - user_count) > 0:
        user_count_updated = user_count + num_users
        sel_users = uids_pass_qc[user_count:user_count_updated]
        pbar_load.set_description(f"Loading user data from users {user_count} to {user_count_updated}")
        df = get_df_for_sel_users(dataset, sel_users, cols)
        pbar_load.update(1)
        pbar_process.set_description(f"Computing {goal} location user data from users {user_count} to {user_count_updated}")
        hl_df = find_home_lat_lng(df, start_night=start_time, end_night=end_time)
        pbar_process.update(1)
        joined_df = assign_points_to_regions(points_df=hl_df, regions_gdf=regions_gdf, 
                                     cols_to_keep=(['uid', 'lat', 'lng'] + gdf_cols))
        outfilename = f'{goal}_locs_for_{user_count}_{user_count_updated}_passqc_users'
        print(outfilename)
        pbar_write.set_description(f"Writing {goal} location user data from users {user_count} to {user_count_updated}")
        write_to_pq(joined_df, out_dir, filename=outfilename)
        pbar_write.update(1)
        user_count = user_count_updated

    pbar_load.close()
    pbar_process.close()
    pbar_write.close()

    return

def read_visits(visits_fp, uid_treat_group_info=None):
    # note the optional uid_treat_group_info needs to be a pandas df with 'uid', 'ZAT_home', 'Group' columns
    visit_df = pd.read_csv(visits_fp)
    visit_df = visit_df.rename(columns={'lat': 'lat_visit', 'lng': 'lng_visit'})
    if uid_treat_group_info is not None: 
        visit_df = visit_df.merge(uid_treat_group_info[['uid', 'ZAT_home', 'Group']], on='uid')
    return visit_df 

def calc_write_visit_pois(visit_df, regions_gdf, cols_to_keep, out_dir, subdir_name, outfilename, 
    point_lat_col='lat_visit', point_lng_col='lng_visit'
    ):
    visits_w_poi_df = assign_points_to_regions(points_df=visit_df, regions_gdf=regions_gdf,
        cols_to_keep=cols_to_keep, point_lat_col=point_lat_col, point_lng_col=point_lng_col)
    num_mapped_visits = len(visits_w_poi_df) - sum(visits_w_poi_df.name.isna())
    print(f'There were {len(visit_df)} visits in the file.')
    print(f'{num_mapped_visits} instances occured where visits mapped to named POIs (some visits may map to multiple POIs).')
    write_to_pq(visits_w_poi_df, out_dir, filename=outfilename, write_subdir=subdir_name, write_csv=True)
    print(f'Wrote data to {outfilename}')
    return visits_w_poi_df

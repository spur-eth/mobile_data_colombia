import glob
import os
import dask.dataframe as dd
import dask_geopandas as ddgpd
from mobilkit.loader import crop_spatial as mk_crop_spatial
import pyarrow as pa
import pyarrow.parquet as pq

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

def write_to_pq(df, out_dir, filename): 
    table_name = f'{out_dir}{filename}.parquet'
    table = pa.Table.from_pandas(df)
    pq.write_table(table, table_name)

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
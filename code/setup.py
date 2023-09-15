import yaml
from pathlib import Path
import glob
from dataclasses import dataclass
import geopandas as gpd

@dataclass
class Where:
    data_dir: str
    data_year: str
    meta_dir: str
    out_dir: str
    study_area_dir: str
    pass_qc_dir: str
    plot_dir: str
    data_folders: list

def read_config(path: str='configs/config.yml'): 
    with open(path, 'r') as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return config

def get_config_vars(c: dict, mode='preprocess'): 
    """Takes in a config dict c and returns the variables needed for the mode"""
    if mode == 'preprocess':
        year=c['data']['year']
        datatypes = c['data']['datatypes']
        initial_cols, sel_cols, final_cols = c['data']['initial_cols'], c['data']['sel_cols'], c['data']['final_cols']
        minlon, maxlon, minlat, maxlat = (c['filter']['boundary_box']['minlon'], 
                                        c['filter']['boundary_box']['maxlon'], 
                                        c['filter']['boundary_box']['minlat'], 
                                        c['filter']['boundary_box']['maxlat'])
        return year, datatypes, initial_cols, sel_cols, final_cols, minlon, maxlon, minlat, maxlat, 
    elif mode == 'user_qc':
        min_days=c['filter']['qc']['min_days']
        min_pings=c['filter']['qc']['min_pings']
        return min_days, min_pings
    else: 
        print(mode, 'mode not implemented yet')   


def ensure_directory_exists(dir_path: str) -> None:
    """Ensure that the given directory exists, and if not, create it."""
    path = Path(dir_path)
    path.mkdir(parents=True, exist_ok=True)

def get_dirs(working_dir, year: str="year=2018", min_days: int=10, min_pings: int=60): 
    data_dir = f'{working_dir}data/'
    data_year = f'{data_dir}{year}/'
    meta_dir = f'{working_dir}metadata/'
    out_dir = f'{working_dir}out/{year}/'
    study_area_dir = f'{out_dir}in_study_area/'
    pass_qc_dir = f'{out_dir}pass_qc_{min_days}days_{min_pings}pings/'
    plot_dir = f'{out_dir}figures/'
    for folder in [data_dir, data_year, meta_dir, out_dir, study_area_dir, pass_qc_dir, plot_dir]: 
        ensure_directory_exists(folder)
    data_folders = glob.glob((data_year + '*/')) # get the month folders from the year folder
    where =  Where(data_dir, data_year, meta_dir, out_dir, study_area_dir, pass_qc_dir, plot_dir, data_folders)
    return where
    
def get_shp(meta_dir: str, shp_name: str, load: bool = False):
    shapefile = f'{meta_dir}{shp_name}.shp'
    if load: 
        gdf_regions = gpd.read_file(shapefile)
        gdf_regions.plot()
        return shapefile, gdf_regions
    return shapefile

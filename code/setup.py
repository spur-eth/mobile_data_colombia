import yaml
from pathlib import Path
import glob
from dataclasses import dataclass
import pandas as pd
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
    elif mode == 'user_locs':
        start_time_home = c['user_locs']['home']['start_time']
        end_time_home = c['user_locs']['home']['end_time']
        start_time_work = c['user_locs']['work']['start_time']
        end_time_work = c['user_locs']['work']['end_time']
        return start_time_home, end_time_home, start_time_work, end_time_work
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

def get_shp_to_assign_poi(shp_dir: str, config: dict, radius:int=15, plot: bool=False): 
    shp_name= config['meta']['shp']['visits'][f'{radius}m']
    shapefile = f'{shp_dir}{shp_name}.shp'
    regions_gdf = gpd.read_file(shapefile)
    if plot: 
        regions_gdf.plot(column='category')
    return shp_name, regions_gdf

def adapt_ZATs_file(meta_dir:str, 
                    in_filename: str='ZAT_treat_control.csv', 
                    out_filename: str='ZAT_selected_txt_control.csv'):
    in_fp = f'{meta_dir}{in_filename}'
    out_fp=f'{meta_dir}{out_filename}'
    zats_tc = pd.read_csv(in_fp).astype('float64') 
    ztreat = [i for i in list(zats_tc['ZATs Treatment group']) if str(i) != "nan"] 
    zcontrol = [i for i in list(zats_tc['ZAT Control group']) if str(i) != "nan"] 
    ztreat_df = pd.DataFrame({'Group': 'Treatment', 'ZATs': ztreat})
    zcontrol_df = pd.DataFrame({'Group': 'Control', 'ZATs': zcontrol})
    zat_treat_control_df = pd.concat([ztreat_df, zcontrol_df])
    zat_treat_control_df.to_csv(out_fp, index=False)
    return zat_treat_control_df 

import glob
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union

import geopandas as gpd
import pandas as pd
import yaml


@dataclass
class Where:
    data_dir: str
    data_year: str
    meta_dir: str
    out_dir: str
    study_area_dir: str
    pass_qc_dir: str
    plot_dir: str
    user_stats_dir: str
    data_folders: List[str]


def read_config(path: str = "configs/config.yml") -> Dict:
    """Reads the configuration file and returns its content.

    Args:
        path (str, optional): Path to the configuration file.

    Returns:
        Dict: Parsed configuration.
    """
    with open(path, "r") as stream:
        try:
            config = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return config


def get_config_vars(c: dict, mode: str = "preprocess") -> Tuple[Any, ...]:
    """Gets configuration variables based on the mode.

    Args:
        c (Dict): The configuration dictionary.
        mode (str, optional): Mode to get the variables for.

    Returns:
        Tuple: The necessary configuration variables.
    """
    if mode == "preprocess":
        year = c["data"]["year"]
        datatypes = c["data"]["datatypes"]
        initial_cols, sel_cols, final_cols = (
            c["data"]["initial_cols"],
            c["data"]["sel_cols"],
            c["data"]["final_cols"],
        )
        minlon, maxlon, minlat, maxlat = (
            c["filter"]["boundary_box"]["minlon"],
            c["filter"]["boundary_box"]["maxlon"],
            c["filter"]["boundary_box"]["minlat"],
            c["filter"]["boundary_box"]["maxlat"],
        )
        return (
            year,
            datatypes,
            initial_cols,
            sel_cols,
            final_cols,
            minlon,
            maxlon,
            minlat,
            maxlat,
        )
    elif mode == "user_qc":
        min_days = c["filter"]["qc"]["min_days"]
        min_pings = c["filter"]["qc"]["min_pings"]
        return min_days, min_pings
    elif mode == "user_locs":
        start_time_home = c["user_locs"]["home"]["start_time"]
        end_time_home = c["user_locs"]["home"]["end_time"]
        start_time_work = c["user_locs"]["work"]["start_time"]
        end_time_work = c["user_locs"]["work"]["end_time"]
        return start_time_home, end_time_home, start_time_work, end_time_work
    else:
        print(mode, "mode not implemented yet")


def ensure_directory_exists(dir_path: str) -> None:
    """Ensures that the directory exists; if not, it creates it.

    Args:
        dir_path (str): Path of the directory to ensure.
    """
    path = Path(dir_path)
    path.mkdir(parents=True, exist_ok=True)


def get_dirs(
    working_dir: str, year: str = "year=2018", min_days: int = 10, min_pings: int = 60
) -> Where:
    """Gets the necessary directories based on the working directory and year.

    Args:
        working_dir (str): The root working directory.
        year (str, optional): The data year.
        min_days (int, optional): Minimum days.
        min_pings (int, optional): Minimum pings.

    Returns:
        Where: An instance of Where dataclass.
    """
    data_dir = f"{working_dir}data/"
    data_year = f"{data_dir}{year}/"
    meta_dir = f"{working_dir}metadata/"
    out_dir = f"{working_dir}out/{year}/"
    study_area_dir = f"{out_dir}in_study_area/"
    pass_qc_dir = f"{out_dir}pass_qc_{min_days}days_{min_pings}pings/"
    user_stats_dir = f"{out_dir}user_stats/"
    plot_dir = f"{out_dir}figures/"
    for folder in [
        data_dir,
        data_year,
        meta_dir,
        out_dir,
        study_area_dir,
        pass_qc_dir,
        user_stats_dir,
        plot_dir,
    ]:
        ensure_directory_exists(folder)
    data_folders = glob.glob(
        (data_year + "*/")
    )  # get the month folders from the year folder
    where = Where(
        data_dir,
        data_year,
        meta_dir,
        out_dir,
        study_area_dir,
        pass_qc_dir,
        plot_dir,
        user_stats_dir,
        data_folders,
    )
    return where


def get_shp(
    meta_dir: str, shp_name: str, load: bool = False
) -> Union[str, tuple[str, gpd.GeoDataFrame]]:
    """Gets the shapefile path and optionally loads it.

    Args:
        meta_dir (str): Directory containing metadata.
        shp_name (str): Name of the shapefile without extension.
        load (bool, optional): If True, loads the shapefile.
    Returns:
        str: The shapefile path if load is False.
        Tuple[str, GeoDataFrame]: The shapefile path and the loaded shapefile if load is True.
    """
    shapefile = f"{meta_dir}{shp_name}.shp"
    if load:
        gdf_regions = gpd.read_file(shapefile)
        gdf_regions.plot()
        return shapefile, gdf_regions
    return shapefile


def get_shp_to_assign_poi(
    shp_dir: str, config: Dict, radius: int = 15, plot: bool = False
) -> Tuple[str, gpd.GeoDataFrame]:
    """Gets the shapefile path for assigning points of interest.

    Args:
        shp_dir (str): Directory containing the shapefile.
        config (Dict): Configuration dictionary.
        radius (int, optional): Radius in meters.
        plot (bool, optional): If True, plots the shapefile.

    Returns:
        Tuple[str, GeoDataFrame]: The shapefile name and the loaded shapefile.
    """
    shp_name = config["meta"]["shp"]["visits"][f"{radius}m"]
    shapefile = f"{shp_dir}{shp_name}.shp"
    regions_gdf = gpd.read_file(shapefile)
    if plot:
        regions_gdf.plot(column="category")
    return shp_name, regions_gdf


def adapt_ZATs_file(
    meta_dir: str,
    in_filename: str = "ZAT_treat_control.csv",
    out_filename: str = "ZAT_selected_txt_control.csv",
) -> pd.DataFrame:
    """Adapts the ZATs file to contain the selected treatment and control groups for the ZATs.

    Args:
        meta_dir (str): Directory containing the metadata file with the ZAT information.
        in_filename (str, optional): Input filename.
        out_filename (str, optional): Output filename.

    Returns:
        pd.DataFrame: Modified DataFrame.
    """
    in_fp = f"{meta_dir}{in_filename}"
    out_fp = f"{meta_dir}{out_filename}"
    zats_tc = pd.read_csv(in_fp).astype("float64")
    ztreat = [i for i in list(zats_tc["ZATs Treatment group"]) if str(i) != "nan"]
    zcontrol = [i for i in list(zats_tc["ZAT Control group"]) if str(i) != "nan"]
    ztreat_df = pd.DataFrame({"Group": "Treatment", "ZATs": ztreat})
    zcontrol_df = pd.DataFrame({"Group": "Control", "ZATs": zcontrol})
    zat_treat_control_df = pd.concat([ztreat_df, zcontrol_df])
    zat_treat_control_df.to_csv(out_fp, index=False)
    return zat_treat_control_df

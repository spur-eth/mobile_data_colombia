import glob
import math
import os
from typing import Optional

import dask.dataframe as dd
import dask_geopandas as ddgpd
import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from mobilkit.loader import crop_spatial as mk_crop_spatial
from mobilkit.stats import userStats
from skmob import TrajDataFrame
from skmob.measures.individual import home_location
from tqdm.notebook import tqdm

from setup import Where, get_config_vars, get_shp, read_config

#### VARIABLES FOR DATA LOADING AND PREPROCESSING ####
# These are based on the data we have and would have to be modified if the format of the files changed/adapted for other data
# Load config file to see them (initial_cols, sel_cols, final_cols, datatypes, boundary_box, etc)

#### FUNCTIONS FOR DATA PREPROCESSING AND FILTERING ####


def get_days(data_folder: str):
    """
    Get the list of day directories within a data folder with a month's worth of data in individual day folders like "day=01", ...

    Args:
        data_folder (str): Path to the main data folder.

    Returns:
        list: A list of paths corresponding to day directories.
    """
    day_dirs = glob.glob((data_folder + "*/"))
    return day_dirs


def get_files(data_folder: str, day_dir: str):
    """
    Retrieve the file paths for a specific day directory.

    Args:
        data_folder (str): Path to the main data folder.
        day_dir (str): Specific day directory like "day=01".

    Returns:
        list, str: A list of file paths and the specific day string.
    """
    day = day_dir.split(data_folder)[1]
    filepaths = [
        file_path
        for file_path in glob.glob(os.path.join(day_dir, "*"))
        if not file_path.endswith(".gz")
    ]  # select all the non-zipped mobile data files
    return filepaths, day


def load_data(
    filepaths, initial_cols: list, sel_cols: list, final_cols: list, datatypes: dict
):
    """
    Load mobile data from given file paths and specify columns.

    Args:
        filepaths (list): List of file paths.
        initial_cols (list): Initial columns in the data.
        sel_cols (list): Columns to select.
        final_cols (list): Final column names to set.
        datatypes (dict): Data types for each column.

    Returns:
        dd.DataFrame: The loaded and processed data.
    """
    ddf = dd.read_csv(filepaths, dtype=datatypes, names=initial_cols)
    ddf = ddf[sel_cols]
    ddf.columns = final_cols
    return ddf


def convert_datetime(ddf: dd.DataFrame):
    """
    Convert timestamp to datetime for a dataframe.

    Args:
        ddf (dd.DataFrame): Dataframe with a "datetime" column containing timestamp values.

    Returns:
        dd.DataFrame: The dataframe with processed datetime values.
    """
    ddf["datetime"] = dd.to_datetime(ddf["datetime"], unit="ms", errors="coerce")
    ddf["datetime"] = (
        ddf["datetime"].dt.tz_localize("UTC").dt.tz_convert("America/Bogota")
    )
    return ddf


def find_within_box(
    ddf: dd.DataFrame, minlon: float, maxlon: float, minlat: float, maxlat: float
) -> dd.DataFrame:
    """
    Filters out points from a Dask DataFrame that are not within a specified rectangular region.

    Parameters:
    ddf (dd.DataFrame): Input Dask DataFrame containing points to filter.
    minlon (float): Minimum longitude of the rectangular region.
    maxlon (float): Maximum longitude of the rectangular region.
    minlat (float): Minimum latitude of the rectangular region.
    maxlat (float): Maximum latitude of the rectangular region.

    Returns:
    dd.DataFrame: Filtered Dask DataFrame containing only points within the specified rectangular region.
    """
    box = [minlon, minlat, maxlon, maxlat]
    filtered_ddf = mk_crop_spatial(ddf, box).reset_index()
    return filtered_ddf


def find_within_regions(
    ddf: dd.DataFrame, gdf: gpd.GeoDataFrame, lng_col: str = "lng", lat_col: str = "lat"
) -> dd.DataFrame:
    """
    Filters a Dask DataFrame based on whether points are within specified regions.

    Parameters:
    ddf (dd.DataFrame): Input Dask DataFrame containing points to filter.
    gdf (gpd.GeoDataFrame): GeoDataFrame containing the geometry of regions to filter points against.
    lng_col (str): Name of the column in ddf representing longitude.
    lat_col (str): Name of the column in ddf representing latitude.

    Returns:
    dd.DataFrame: Filtered Dask DataFrame containing only points within the specified regions.
    """
    ddf = ddgpd.from_dask_dataframe(
        ddf, geometry=ddgpd.points_from_xy(ddf, lng_col, lat_col)
    )
    ddf = ddf.set_crs(gdf.crs)
    ddf_in_regions = ddf.sjoin(gdf, predicate="within")
    return ddf_in_regions


def filter_data_for_day(
    filepaths,
    gdf: gpd.GeoDataFrame,
    initial_cols: list,
    sel_cols: list,
    final_cols: list,
    datatypes: dict,
    minlon: float,
    maxlon: float,
    minlat: float,
    maxlat: float,
) -> dd.DataFrame:
    """
    Loads data for a given day, filters it to a study area and regions of interest, and transforms the columns as specified.

    Parameters:
    filepaths (list): List of filepaths for the data to load.
    gdf (gpd.GeoDataFrame): GeoDataFrame containing the geometry of regions to filter points against.
    initial_cols (list): List of initial columns to select from the data.
    sel_cols (list): List of columns to be selected after initial filtering.
    final_cols (list): List of final columns to keep after all transformations.
    datatypes (dict): Dictionary specifying the datatypes for each column.
    minlon (float): Minimum longitude of the rectangular region for filtering.
    maxlon (float): Maximum longitude of the rectangular region for filtering.
    minlat (float): Minimum latitude of the rectangular region for filtering.
    maxlat (float): Maximum latitude of the rectangular region for filtering.

    Returns:
    dd.DataFrame: Filtered and transformed Dask DataFrame.
    """
    ddf = load_data(filepaths, initial_cols, sel_cols, final_cols, datatypes)
    within_box_ddf = find_within_box(ddf, minlon, maxlon, minlat, maxlat)
    ddf_in_regions = find_within_regions(within_box_ddf, gdf=gdf)
    # cols must be in in the ddf/shapefile
    cols = [
        "uid",
        "lat",
        "lng",
        "datetime",
        "geohash",
        "horizontal_accuracy",
        "index_right",
        "LOCNombre",
        "UTAMNombre",
    ]
    ddf_in_regions = ddf_in_regions[cols]
    final_colnames = [
        "uid",
        "lat",
        "lng",
        "datetime",
        "geohash",
        "horizontal_accuracy",
        "index_from_shp",
        "LOCNombre",
        "UTAMNombre",
    ]
    ddf_in_regions.columns = final_colnames
    ddf_in_regions = convert_datetime(ddf_in_regions)
    return ddf_in_regions


def write_day(ddf_in_regions: dd.DataFrame, out_dir: str, year, month, day) -> None:
    """
    Writes the filtered Dask DataFrame to a parquet file.

    Parameters:
    ddf_in_regions (dd.DataFrame): Filtered Dask DataFrame to write.
    out_dir (str): Output directory where the parquet file will be written.
    year: Year of the data.
    month: Month of the data.
    day: Day of the data.
    """
    filename = f"{out_dir}{year}_{month}_{day}.parquet"
    ddf_in_regions.to_parquet(path=filename, write_index=False)
    return


def write_to_pq(
    df, out_dir: str, filename: str, write_subdir="", write_csv=False
) -> None:
    """
    Writes a DataFrame to a parquet file and optionally to a CSV file.

    Parameters:
    df (pd.DataFrame/dd.DataFrame): DataFrame to write.
    out_dir (str): Output directory where the files will be written.
    filename (str): Name of the output file without the extension.
    write_subdir (str): Subdirectory within out_dir where the file will be written.
    write_csv (bool): If True, also writes the DataFrame to a CSV file.
    """
    if len(write_subdir) > 0:
        out_dir = f"{out_dir}{write_subdir}/"
        try:
            os.makedirs(out_dir)
        except FileExistsError:
            pass
    table_name = f"{out_dir}{filename}.parquet"
    table = pa.Table.from_pandas(df)
    pq.write_table(table, table_name)
    if write_csv == True:
        df.to_csv(f"{out_dir}{filename}.csv", index=False)
    return


def from_month_write_filter_days_to_pq(
    data_folder: str,
    gdf,
    data_year: str,
    year: str,
    out_dir: str,
    initial_cols: list,
    sel_cols: list,
    final_cols: list,
    datatypes: dict,
    minlon: float,
    maxlon: float,
    minlat: float,
    maxlat: float,
) -> None:
    """
    For a given month, filters the data for each day and writes the filtered data to parquet files.

    Parameters:
    data_folder (str): Path to the folder containing the data.
    gdf (gpd.GeoDataFrame): GeoDataFrame containing the geometry of regions to filter points against.
    data_year (str): The year of the data being processed.
    year (str): Year to be used for the output directory.
    out_dir (str): Output directory where the parquet files will be written.
    initial_cols (list): List of initial columns to select from the data.
    sel_cols (list): List of columns to be selected after initial filtering.
    final_cols (list): List of final columns to keep after all transformations.
    datatypes (dict): Dictionary specifying the datatypes for each column.
    minlon (float): Minimum longitude of the rectangular region for filtering.
    maxlon (float): Maximum longitude of the rectangular region for filtering.
    minlat (float): Minimum latitude of the rectangular region for filtering.
    maxlat (float): Maximum latitude of the rectangular region for filtering.
    """
    month = data_folder.split(data_year)[1].split("/")[0]
    day_dirs = get_days(data_folder)
    for i in tqdm(range(0, len(day_dirs)), desc=f"Files from {year} {month} processed"):
        filepaths, day = get_files(data_folder, day_dirs[i])
        ddf_in_regions = filter_data_for_day(
            filepaths,
            gdf,
            initial_cols,
            sel_cols,
            final_cols,
            datatypes,
            minlon,
            maxlon,
            minlat,
            maxlat,
        )
        day_name = day.split("/")[0]
        filename = f"{year}_{month}_{day_name}"
        write_to_pq(df=ddf_in_regions.compute(), out_dir=out_dir, filename=filename)
    return


def write_data_in_study_area(where: Where, config_path: str) -> None:
    """
    Filters and writes data located in the study area specified in the configuration file.

    Parameters:
    where (Where): Contains data folders and other information to locate the data.
    config_path (str): Path to the configuration file containing filtering parameters and other settings.
    """
    print(config_path)
    c = read_config(path=config_path)
    shapefile, gdf_regions = get_shp(
        meta_dir=where.meta_dir, shp_name=c["meta"]["shp"]["study_area"], load=True
    )
    (
        year,
        datatypes,
        initial_cols,
        sel_cols,
        final_cols,
        minlon,
        maxlon,
        minlat,
        maxlat,
    ) = get_config_vars(c=c, mode="preprocess")
    for i in range(0, len(where.data_folders)):
        data_folder = where.data_folders[i]
        from_month_write_filter_days_to_pq(
            data_folder,
            gdf=gdf_regions,
            out_dir=where.study_area_dir,
            data_year=where.data_year,
            year=year,
            initial_cols=initial_cols,
            sel_cols=sel_cols,
            final_cols=final_cols,
            datatypes=datatypes,
            minlon=minlon,
            maxlon=maxlon,
            minlat=minlat,
            maxlat=maxlat,
        )


def compute_user_stats_from_pq(pq_dir: str) -> pd.DataFrame:
    """
    Computes user statistics from parquet files located in the specified directory.

    Parameters:
    pq_dir (str): Directory containing the parquet files.

    Returns:
    DataFrame: A DataFrame containing computed user statistics.
    """
    table_dd = dd.read_parquet(pq_dir, columns=["uid", "datetime"])
    user_stats = userStats(table_dd).compute()
    return user_stats


def get_df_for_sel_users(
    dataset: ds.dataset, sel_users: list, cols: list
) -> pd.DataFrame:
    """
    Extracts a DataFrame containing only selected users and columns from a given dataset.

    Parameters:
    dataset: Dataset from which to extract the DataFrame.
    sel_users (list): List of user IDs to select from the dataset.
    cols (list): List of columns to select from the dataset.

    Returns:
    pd.DataFrame: Extracted DataFrame containing only the selected users and columns.
    """
    table = dataset.to_table(columns=cols, filter=ds.field("uid").isin(sel_users))
    df = table.to_pandas().reset_index()
    df = df.drop(["index"], axis=1)
    return df


def find_goal_lat_lng(
    df: pd.DataFrame, start_night: str = "22:00", end_night: str = "06:00"
) -> pd.DataFrame:
    """
    Determine home location based on nighttime trajectories. (Alternatively can compute work locations by setting the work times as 'night times')

    Args:
        df (pd.DataFrame): Input data frame containing trajectory information.
        start_night (str, optional): Start time for the night period.
        end_night (str, optional): End time for the night period.

    Returns:
        pd.DataFrame: DataFrame containing home location data.
    """
    traj_df = TrajDataFrame(
        df, user_id="uid", latitude="lat", longitude="lng", datetime="datetime"
    )
    hl_df = home_location(traj_df, start_night=start_night, end_night=end_night)
    return hl_df


def assign_points_to_regions(
    points_df: pd.DataFrame,
    regions_gdf: gpd.GeoDataFrame,
    cols_to_keep: list,
    point_lat_col: str = "lat",
    point_lng_col: str = "lng",
):
    """
    Assign points to specified regions based on spatial location.

    Args:
        points_df: DataFrame containing points data.
        regions_gdf (gpd.GeoDataFrame): GeoDataFrame of regions to assign points.
        cols_to_keep (list): List of columns to keep in the output.
        point_lat_col (str, optional): Latitude column of points.
        point_lng_col (str, optional): Longitude column of points.

    Returns:
        gpd.GeoDataFrame: GeoDataFrame with points assigned to regions.
    """
    geometry = gpd.points_from_xy(points_df[point_lng_col], points_df[point_lat_col])
    points_gdf = gpd.GeoDataFrame(points_df, geometry=geometry, crs=regions_gdf.crs)
    # Perform spatial join to assign points to regions
    joined_gdf = gpd.sjoin(points_gdf, regions_gdf, how="left", op="within")
    joined_gdf = joined_gdf[cols_to_keep]
    return joined_gdf


def compute_goal_lat_lngs_for_users(
    uids_pass_qc: list,
    pq_dir: str,
    out_dir: str,
    regions_gdf: gpd.GeoDataFrame,
    num_users: int = 20000,
    cols: list = ["uid", "datetime", "lat", "lng"],
    gdf_cols: list = ["Area", "MUNCod", "NOMMun", "ZAT", "UTAM", "stratum"],
    start_time: str = "22:00",
    end_time: str = "06:00",
    goal: str = "home",
) -> None:
    """
    Compute goal latitude and longitude for a batch of users and write results to parquet files.

    Args:
        uids_pass_qc (list): List of user IDs that passed the quality check.
        pq_dir (str): Directory containing input parquet files.
        out_dir (str): Directory to store the resulting data.
        regions_gdf (gpd.GeoDataFrame): GeoDataFrame of regions to assign points.
        num_users (int): Number of users to process in each batch.
        cols (list): List of columns to consider from the input dataset.
        gdf_cols (list): List of columns from the regions GeoDataFrame to retain in the results.
        start_time (str): The start time to define the range of interest.
        end_time (str): The end time to define the range of interest.
        goal (str): The goal location type (e.g., 'home') to be computed.
    """
    pings_paths = glob.glob((pq_dir + "*.parquet"))
    dataset = ds.dataset(pings_paths, format="parquet")
    total_users = len(uids_pass_qc)
    user_count = 0

    expected_iter = math.ceil(len(uids_pass_qc) / num_users)
    pbar_load = tqdm(total=(expected_iter))
    pbar_process = tqdm(total=(expected_iter))
    pbar_write = tqdm(total=(expected_iter))

    while (total_users - user_count) > 0:
        user_count_updated = user_count + num_users
        sel_users = uids_pass_qc[user_count:user_count_updated]
        pbar_load.set_description(
            f"Loading user data from users {user_count} to {user_count_updated}"
        )
        df = get_df_for_sel_users(dataset, sel_users, cols)
        pbar_load.update(1)
        pbar_process.set_description(
            f"Computing {goal} location user data from users {user_count} to {user_count_updated}"
        )
        hl_df = find_goal_lat_lng(df, start_night=start_time, end_night=end_time)
        pbar_process.update(1)
        joined_df = assign_points_to_regions(
            points_df=hl_df,
            regions_gdf=regions_gdf,
            cols_to_keep=(["uid", "lat", "lng"] + gdf_cols),
        )
        outfilename = f"{goal}_locs_for_{user_count}_{user_count_updated}_passqc_users"
        print(outfilename)
        pbar_write.set_description(
            f"Writing {goal} location user data from users {user_count} to {user_count_updated}"
        )
        write_to_pq(joined_df, out_dir, filename=outfilename)
        pbar_write.update(1)
        user_count = user_count_updated

    pbar_load.close()
    pbar_process.close()
    pbar_write.close()

    return


def read_visits(visits_fp: str, uid_treat_group_info=None):
    """
    Read visits from a file and optionally merge with treatment group info.

    Args:
        visits_fp (str): File path to the visits data.
        uid_treat_group_info (pd.DataFrame, optional): DataFrame with 'uid', 'ZAT_home', 'Group' columns.

    Returns:
        pd.DataFrame: DataFrame containing visit data.
    """
    visit_df = pd.read_csv(visits_fp)
    visit_df = visit_df.rename(columns={"lat": "lat_visit", "lng": "lng_visit"})
    if uid_treat_group_info is not None:
        visit_df = visit_df.merge(
            uid_treat_group_info[["uid", "ZAT_home", "Group"]], on="uid"
        )
    return visit_df


def calc_write_visit_pois(
    visit_df: pd.DataFrame,
    regions_gdf: gpd.GeoDataFrame,
    cols_to_keep: list,
    out_dir: str,
    subdir_name: str,
    outfilename: str,
    point_lat_col: str = "lat_visit",
    point_lng_col: str = "lng_visit",
):
    """
    Calculate visit points of interest and write results to output directory.

    Args:
        visit_df: DataFrame containing visit data.
        regions_gdf: GeoDataFrame of regions to assign points.
        cols_to_keep (list): List of columns to keep in the output.
        out_dir (str): Output directory for the resulting data.
        subdir_name (str): Subdirectory name inside the output directory.
        outfilename (str): Output file name.
        point_lat_col (str, optional): Latitude column of visit points.
        point_lng_col (str, optional): Longitude column of visit points.

    Returns:
        pd.DataFrame: DataFrame containing visit points of interest.
    """
    visits_w_poi_df = assign_points_to_regions(
        points_df=visit_df,
        regions_gdf=regions_gdf,
        cols_to_keep=cols_to_keep,
        point_lat_col=point_lat_col,
        point_lng_col=point_lng_col,
    )
    num_mapped_visits = len(visits_w_poi_df) - sum(visits_w_poi_df.name.isna())
    print(f"There were {len(visit_df)} visits in the file.")
    print(
        f"{num_mapped_visits} instances occured where visits mapped to named POIs (some visits may map to multiple POIs)."
    )
    write_to_pq(
        visits_w_poi_df,
        out_dir,
        filename=outfilename,
        write_subdir=subdir_name,
        write_csv=True,
    )
    print(f"Wrote data to {outfilename}")
    return visits_w_poi_df

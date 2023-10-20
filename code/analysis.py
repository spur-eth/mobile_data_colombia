from typing import List, Tuple

import pandas as pd
from geopy.distance import geodesic
from skmob.core.trajectorydataframe import TrajDataFrame
from skmob.preprocessing import detection
from tqdm.notebook import tqdm

from preprocess import write_to_pq


def geodesic_distance(row: pd.DataFrame) -> float:
    """Calculate the geodesic distance between two sets of GPS coordinates.

    Args:
        row (DataFrame): Row of a dataframe with columns 'lat_home', 'lng_home', 'lat_work', and 'lng_work'.

    Returns:
        float: The distance between the two coordinates in kilometers.
    """
    home_coords = (row["lat_home"], row["lng_home"])
    work_coords = (row["lat_work"], row["lng_work"])
    return geodesic(home_coords, work_coords).kilometers


def calculate_visits_min_minutes(
    tdf: TrajDataFrame,
    visit_durations: List[int],
    out_dir: str,
    stop_radius_factor: float = 0.5,
    spatial_radius_km: float = 0.2,
    no_data_for_minutes: float = 1e12,
) -> None:
    """Calculate and write visits based on minimum visit durations.

    Args:
        tdf (DataFrame): The input dataframe.
        visit_durations (List[int]): List of minimum visit durations to compute.
        out_dir (str): Output directory to write results.
        stop_radius_factor (float, optional): Factor to apply on the stop radius.
        spatial_radius_km (float, optional): Spatial radius in kilometers.
        no_data_for_minutes (float, optional): Threshold for missing data in minutes.
    """
    i = 0
    expected_iter = len(visit_durations)
    pbar_process = tqdm(total=(expected_iter))
    pbar_write = tqdm(total=(expected_iter))

    while i < expected_iter:
        number_min = visit_durations[i]
        pbar_process.set_description(
            f"Computing visits where users spent at least {number_min} minutes"
        )
        visit_df = detection.stay_locations(
            tdf,
            stop_radius_factor=0.5,
            minutes_for_a_stop=number_min,
            no_data_for_minutes=no_data_for_minutes,
            spatial_radius_km=0.2,
            leaving_time=True,
        )
        print(
            f"The number of stops for {number_min} minutes in the dataset is {len(visit_df)}"
        )
        pbar_process.update(1)
        outfilename = f"users_living_in_sel_zat_visits_atleast_{number_min}min_nodatafor_{no_data_for_minutes}_minutes"
        pbar_write.set_description(f"Writing user data for {number_min} minute visits")
        write_to_pq(visit_df, out_dir, filename=outfilename)
        visit_df.to_csv(f"{out_dir}{outfilename}.csv", index=False)
        pbar_write.update(1)
        i += 1

    pbar_process.close()
    pbar_write.close()

    return


def calc_group_poi_visits(
    visits_w_poi_df: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.Series, pd.DataFrame]:
    """Calculate the number and groupings of point of interest (POI) visits.

    Args:
        visits_w_poi_df (DataFrame): DataFrame containing visits with POIs.

    Returns:
        DataFrame: Visits with named POIs.
        Series: Visits with more than one named POI.
        DataFrame: Proportions of grouped categories.
    """
    visits_w_named_pois = visits_w_poi_df.dropna(subset="name")
    print(
        f"The number of total visits is {len(visits_w_poi_df)}, with {len(visits_w_named_pois)} POIs mapped."
    )
    grouped_visits = visits_w_named_pois.groupby(["uid", "datetime"])["name"].count()
    visits_w_more_than_one_named_poi = grouped_visits[grouped_visits > 1]
    print(
        f"The number of visits that map to at least one POI is {len(grouped_visits)}, and {len(visits_w_more_than_one_named_poi)} map to multiple POIs."
    )
    per_visits_mult_name = (
        len(visits_w_more_than_one_named_poi) / len(grouped_visits)
    ) * 100
    print(
        f"The percentage of visits with more than one assigned POI is: {per_visits_mult_name}"
    )
    grouped_category_proportions = (
        visits_w_named_pois.groupby(["Group"])["category"]
        .value_counts(normalize=True)
        .to_frame()
    )
    return (
        visits_w_named_pois,
        visits_w_more_than_one_named_poi,
        grouped_category_proportions,
    )

def count_visits_by_month(visit_df: pd.DataFrame, cols: List[str], as_proportion: bool=False, 
                          normalize: bool=False, dt_col: str='datetime') -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Counts and possibly normalizes the visits by month, and can calculate them as proportions. This function 
    adds month information to the 'visit_df', groups the data by month and optionally normalizes the visit counts 
    or calculates them as proportions.

    Args:
        visit_df: DataFrame containing visit records with a datetime column.
        cols: List of column names to consider from visit_df for plotting.
        as_proportion: If True, returns the count of visits as a proportion of the total.
        normalize: If True, normalizes the visit counts by the number of unique users.
        dt_col: Name of the column in visit_df that contains datetime information.

    Returns:
        A tuple containing two DataFrames: the first one is the modified visit_df with added month information, 
        and the second one is the grouped DataFrame with counts or proportions of visits per month.
    """
    visits_to_plot = visit_df.reset_index()[cols]
    print(f'The total number of visits considered is {len(visits_to_plot)}')
    visits_to_plot["datetime"] = pd.to_datetime(visits_to_plot["datetime"], format='mixed', dayfirst=True)
    visits_to_plot['month_name'] = visits_to_plot["datetime"].dt.month_name()
    visits_to_plot['month'] = visits_to_plot["datetime"].dt.month
    if as_proportion == False: 
        visits_grouped = visits_to_plot.groupby('month')['Group'].value_counts(dropna=False).to_frame().reset_index()
    else: 
        visits_grouped = visits_to_plot.groupby('month')['Group'].value_counts(dropna=False, normalize=True).to_frame().reset_index()
        visits_grouped['percentage'] = round(visits_grouped['proportion']*100, 2)
    if normalize == True: 
        num_users_per_month = visits_to_plot.groupby(['month', 'Group'])['uid'].nunique().to_frame().reset_index()
        normalized_visits_grouped = num_users_per_month.merge(visits_grouped['count'], left_index=True, right_index=True)
        normalized_visits_grouped['count_normalized_nusers'] = normalized_visits_grouped['count']/normalized_visits_grouped['uid']
        visits_grouped = normalized_visits_grouped 
    return visits_to_plot, visits_grouped

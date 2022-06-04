from pathlib import Path
from prefect import task
import pandas as pd


@task()
def set_station_as_index(df: pd.DataFrame) -> pd.DataFrame:
    df[['LATITUDE', 'LONGITUDE', 'ELEVATION']] = df[['LATITUDE', 'LONGITUDE', 'ELEVATION']].fillna('missing')
    return df.set_index('STATION')


@task()
def remove_missing_spatial(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes Records with Missing Spatial Data
    - If 'NaN' exists, replaces with 'missing'
    - Returns dataframe with all 'missing' spatial elements removed
    - Why: This is 100x (guestimate) faster than saving a separate csv with this information removed. Quicker to just
           run this function when a dataframe with 100% clean spatial data is required
    """
    df = df[['LATITUDE', 'LONGITUDE', 'ELEVATION']] = df[['LATITUDE', 'LONGITUDE', 'ELEVATION']].fillna('missing')
    return df[(df['LATITUDE'] != 'missing') & (df['LONGITUDE'] != 'missing') & (df['ELEVATION'] != 'missing')]


@task()
def find_missing_lat_long(station_indexed_df: pd.DataFrame, year: str, data_dir: str) -> pd.DataFrame:
    """
    NOTE: Assumes dataframe has had 'nan' replaced with 'missing' string already.
    """
    # FIND RECORDS MISSING LATITUDE OR LONGITUDE (or both)
    df = station_indexed_df
    df = df[(df['LATITUDE']=='missing') | (df['LONGITUDE']=='missing')]
    df = df.reset_index()
    df.to_csv(Path(data_dir) / year / f"{year}_missing_lat_long.csv")
    return


@task()
def find_missing_elevation(station_indexed_df: pd.DataFrame, year: str, data_dir: str) -> pd.DataFrame:
    """
    NOTE: Assumes dataframe has had 'nan' replaced with 'missing' string already.
    """
    # FIND RECORDS MISSING ONLY ELEVATION
    # - these could likely still be used
    # - may also be able to pull elevation from else where based on latitude and longitude
    df = station_indexed_df
    df = df[(df['LATITUDE'] != 'missing') & (df['LONGITUDE'] != 'missing') & (df['ELEVATION'] == 'missing')]
    df = df.reset_index()
    df.to_csv(Path(data_dir) / year / f"{year}_missing_only_elevation.csv")
    return


@task()
def confirm_consistent_spatial(station_indexed_df: pd.DataFrame, year: str, data_dir: str) -> pd.DataFrame:
    """
    NOTE: Assumes dataframe has had 'nan' replaced with 'missing' string already.
    """
    # Confirm all stations each has consistent spatial data
    # - *drop* records where spatial data is missing
    # - only runs after previous tasks for missing spatial data are complete
    df = station_indexed_df
    station_grouped_df = df.groupby('STATION')[['LATITUDE', 'LONGITUDE', 'ELEVATION']].value_counts() #dropna=False)
    if len(station_grouped_df) == len(df.index.unique()):
        # run again, but drop records with 'nan' spatial values
        # df = df[(df['LATITUDE'] != 'missing') & (df['LONGITUDE'] != 'missing') & (df['ELEVATION'] != 'missing')]
        return df.reset_index()
    else:
        # TODO: This part doesn't work yet; also haven't had an inconsistencies above either (i.e., "else" has never been triggered).
        raise ValueError(f'Spatial data for one or more station ids is not consistent in: {Path(data_dir) / f"{year}_full.csv"}')

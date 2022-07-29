##############################################################################
# Author: Ben Hammond
# Last Changed: 5/16/22
#
# REQUIREMENTS
# - Pip: use requirements.txt
# - Poetry: use pyproject.toml
#
# DESCRIPTION
# - (1) Uses requests and bs4 to webscrape a NOAA web page containing temperature data (goes back to 1929)
#   - Source: "https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive"
# - (2) Uses httpx to download the tar.gz files to folders named after the year representing the file
# - (3) Extracts and merges individual csv files in tarfile into a single large csv file
# - (4) Cleans/Verifies data by:
#   - extracts records with lat/long, but missing elevation into a separate csv
#   - extracts records missing lat/long into another separate csv
#   - extracts sites with inconsistent spatial data and [haven't defined this yet, since the issue hasn't been encountered]
# - Features:
#   - New/missing archive files are detected and downloaded
#   - Updated archive files are detected and downloaded (does't replace original tar.gz, but does replace extracted
#     files in "data" folder
#   - _missing_lat_long csv is created
#   - _missing_only_elevation csv is created
##############################################################################
from prefect import flow, get_run_logger
from prefect.task_runners import SequentialTaskRunner
from pathlib import Path
from noaa_file_download.tasks.download import query_cloud_archives, query_local_archives, archives_difference, download_and_merge, save_to_file
from noaa_file_download.tasks.clean import set_station_as_index, find_missing_lat_long, find_missing_elevation, confirm_consistent_spatial


@flow(name="NOAA-files-download", task_runner=SequentialTaskRunner())
def noaa_file_download():
    logger = get_run_logger()
    base_url = "https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive"
    data_dir = str(Path("./local_data/global-summary-of-the-day-archive"))

    cloud_df = query_cloud_archives(base_url)
    local_df = query_local_archives(data_dir)
    diff_l = archives_difference(cloud_df, local_df)
    for file_ in diff_l:
        year = Path(file_[1]).name[:4]
        tarfile_name = file_[0]
        
        # DOWNLOAD DATA FILE
        logger.info(f'Download started for {year}')
        s4_download_df = download_and_merge(file_, base_url, data_dir)
        
        # CLEAN/VERIFY DATA
        logger.info(f'Clean step started for {year}')
        clean1_data = set_station_as_index(s4_download_df)
        logger.info(f'Check for missing lat/long for {year}')
        clean2_lat_long = find_missing_lat_long(clean1_data, year, data_dir)
        logger.info(f'Check for missing elevation for {year}')
        clean3_elevation = find_missing_elevation(clean1_data, year, data_dir)
        logger.info(f'Check for consistent spatial data for {year}')
        final_data_df = confirm_consistent_spatial(clean1_data, year, data_dir, wait_for=[clean2_lat_long, clean3_elevation])
        logger.info(f'Clean step completed for {year}')

        # SAVE DATA
        s6_save = save_to_file(final_data_df, year, data_dir, tarfile_name)
        logger.info(f'Download completed for {year}')


if __name__ == "__main__":
    noaa_file_download()

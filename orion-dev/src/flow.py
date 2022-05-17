##############################################################################
# Author: Ben Hammond
# Last Changed: 5/16/22
#
# REQUIREMENTS
# - Pip: use requirements.txt
# - Poetry: use pyproject.toml
#
# DESCRIPTION
# - Uses requests and bs4 to webscrape a NOAA web page containing temperature data (goes back to 1929)
#   - Source: "https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive"
# - Uses httpx to download the tar.gz files to folders named after the year representing the file
#   - tar.gz files are immediately extracted upload download to a folder named "data" in the same year-named directory
# - Features:
#   - New/missing archive files are detected and downloaded
#   - Updated archive files are detected and downloaded (does't replace original tar.gz, but does replace extracted
#     files in "data" folder
##############################################################################

from prefect import flow
from prefect.task_runners import DaskTaskRunner, SequentialTaskRunner
from pathlib import Path
from src.tasks import query_cloud_archives, query_local_archives, archives_difference, download


n_workers = 1

@flow(name="NOAA-files-download", 
    task_runner=SequentialTaskRunner()
    # task_runner=DaskTaskRunner(cluster_kwargs={"n_workers": 2})
)
def file_download():
    base_url = "https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive"
    data_dir = str(Path("./local_data/global-summary-of-the-day-archive"))

    cloud_df = query_cloud_archives(base_url)
    local_df = query_local_archives(data_dir)
    diff_l = archives_difference(cloud_df, local_df)
    for file_ in diff_l.result():
        download(file_, base_url, data_dir)


if __name__ == "__main__":
    file_download()

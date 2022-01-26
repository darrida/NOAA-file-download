##############################################################################
# Author: Ben Hammond
# Last Changed: 5/7/21
#
# REQUIREMENTS
# - Detailed dependencies in requirements.txt
# - Directly referenced:
#   - prefect, bs4, requests
#
# - Infrastructure:
#   - Prefect: Script is registered as a Prefect flow with api.prefect.io
#     - Source: https://prefect.io
#
# DESCRIPTION
# - Uses requests and bs4 to webscrape a NOAA web page containing temperature data (goes back to 1929)
#   - Source: https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/
# - Uses requests to download the files and store them on a local server
#   - Local files are in the same directory structure as the website (individual site files
#     stored in directories for each year)
# - Includes the following features (to assist with handling the download of 538,000 [34gb] csv files):
#   - Continue Downloading: If the download is interrupted, the script can pick up where it left off
#   - If caught up with downloads it downloads the current year again (refresh files that are updated everyday)
##############################################################################

# PyPI
import csv
from prefect import flow
from prefect.task_runners import DaskTaskRunner

# Standard
from pathlib import Path
from datetime import timedelta

from tasks import tasks_calc as calc
from tasks import tasks_local as local
from tasks import tasks_cloud as cloud


n_workers = 4

@flow(name="NOAD-files-download", task_runner=DaskTaskRunner(cluster_kwargs={'n_workers': n_workers}))
def file_download(n_workers: int):
    n_workers = 4
    base_url = "https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/"
    data_dir = str(Path("./local_data/noaa_temp_downloads"))
    download_chunk_size = 100
    t1_year = local.find_highest_year(url=base_url, data_dir=data_dir)
    t2_url = local.build_url(base_url=base_url, year=t1_year)
    t3_cset = cloud.query_cloud_csvs(url=t2_url, year=t1_year)
    t4_lset = local.query_local_csvs(year=t1_year, data_dir=data_dir)
    t5_diff_l = calc.query_diff_local_cloud(
        local_set=t4_lset, cloud_set=t3_cset, chunk_size=download_chunk_size, workers=n_workers
    )
    # t6_next = []
    csv_l = t5_diff_l.wait()
    print(f'Files to download: {len([i for sub in csv_l.result() for i in sub])}')
    for csv_set in csv_l.result():
        t6_next = cloud.download_new_csvs(url=t2_url, year=t1_year, diff_set=csv_set, data_dir=data_dir)
        # t6_next.append(result)
    # else:

    t7_task = cloud.find_new_year(url=base_url, next_year=t6_next, year=t1_year, data_dir=data_dir)


if __name__ == "__main__":
    file_download(n_workers)

# schedule = IntervalSchedule(interval=timedelta(seconds=5))

# n_workers = 13
# executor = LocalDaskExecutor(scheduler="processes", num_workers=n_workers)
# with Flow("NOAA files: Download All", executor=executor, schedule=schedule) as flow:
#     base_url = Parameter("base_url", default="https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/")
#     data_dir = Parameter("data_dir", default=str(Path("./local_data/noaa_temp_downloads")))
#     download_chunk_size = Parameter("download_map_lists", default=100)

#     t1_year = local.find_highest_year(url=base_url, data_dir=data_dir)
#     t2_url = local.build_url(base_url=base_url, year=t1_year)
#     t3_cset = cloud.query_cloud_csvs(url=t2_url, year=t1_year)
#     t4_lset = local.query_local_csvs(year=t1_year, data_dir=data_dir)
#     t5_diff_l = calc.query_diff_local_cloud(
#         local_set=t4_lset, cloud_set=t3_cset, chunk_size=download_chunk_size, workers=n_workers
#     )
#     t6_next = cloud.download_new_csvs(
#         url=unmapped(t2_url), year=unmapped(t1_year), diff_set=mapped(t5_diff_l), data_dir=unmapped(data_dir)
#     )
#     t7_task = cloud.find_new_year(url=base_url, next_year=t6_next, year=t1_year, data_dir=data_dir)

# # flow.run_config = LocalRun(working_dir="/home/share/github/1-NOAA-Data-Download-Cleaning-Verification/")


# if __name__ == "__main__":
#     flow.run()
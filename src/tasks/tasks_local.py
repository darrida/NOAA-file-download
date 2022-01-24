import os
from pathlib import Path

import icecream as ic
from prefect import task


@task(log_stdout=True)
def find_highest_year(url: str, data_dir):
    year_folders = os.listdir(path=data_dir)
    print(sorted(year_folders))
    if year_folders:
        return max(year_folders)
    else:
        return 0


@task(log_stdout=True)
def build_url(base_url, year=""):
    return f"{base_url}/{year}"


@task(log_stdout=True)
def query_local_csvs(year: int, data_dir: str) -> set:
    csv_local_set = set()
    data_dir = Path(data_dir)
    csv_folder = (data_dir / str(year)).rglob("*.csv")
    csv_local_list = [x for x in csv_folder]
    for i in csv_local_list:
        csv_local_set.add(str(i).split("/")[-1])
    return csv_local_set

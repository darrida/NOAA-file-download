import os
from pathlib import Path

from icecream import ic
from prefect import task


@task(name='Find Highest Year Directory')
def find_highest_year(data_dir):
    year_folders = os.listdir(path=data_dir)
    print(sorted(year_folders))
    if year_folders:
        return max(year_folders)
    else:
        return 0


@task(name='Build File Download URL')
def build_url(base_url, year=""):
    return f"{base_url}/{year}"


@task(name='Query Local Dir CSVs')
def query_local_csvs(year: int, data_dir: str) -> set:
    csv_local_set = set()
    data_dir = Path(data_dir)
    csv_folder = (data_dir / str(year)).rglob("*.csv")
    csv_local_list = [x for x in csv_folder]
    for i in csv_local_list:
        csv_local_set.add(str(i).split("/")[-1])
    return csv_local_set

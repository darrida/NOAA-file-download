from pathlib import Path
from datetime import timedelta
import re
import os

from prefect import task
import requests
from bs4 import BeautifulSoup as BS
from tqdm import tqdm


@task() # log_stdout=True)
def query_cloud_csvs(url: str, year: int) -> set:
    response = requests.get(url)
    parsed_html = BS(response.content, "html.parser")
    csv_cloud_set = set()
    for item in parsed_html.find_all("a"):
        if ".csv" in item.get_text():
            csv_cloud_set.add(item.get_text())
    return csv_cloud_set


@task(retries=5, retry_delay_seconds=3)
def download_new_csvs(url: str, year: int, diff_set: set, data_dir: str) -> bool:
    if int(year) > 0:
        count = 0
        data_dir = Path(data_dir)
        download_path = data_dir / str(year)
        if os.path.exists(download_path) == False:
            Path(download_path).mkdir(parents=True, exist_ok=True)
        for i in tqdm(diff_set):
            if count <= 1000:
                try:
                    download_url = url + "/" + i
                    # print(download_url)
                    result = requests.get(download_url)
                    file_path = Path(data_dir / year / i)
                    open(file_path, "wb").write(result.content)
                except requests.exceptions.InvalidURL:
                    print("Bad url", i)
            count += 1
        if count <= 2000:
            return True
    elif year == 0:
        return True


@task() # log_stdout=True)
def find_new_year(url: str, next_year: bool, year: int, data_dir: str):
    if next_year:
        response = requests.get(url)
        parsed_html = BS(response.content, "html.parser")
        cloud_year_set = set()
        for item in parsed_html.find_all("a"):
            cloud_year = item.get_text().replace("/", "")
            cloud_year_set.add(cloud_year)
        cloud_year_set = [x for x in cloud_year_set if re.search(r"\d\d\d\d", x)]
        cloud_year_set = sorted(cloud_year_set, reverse=True)
        if year == 0:
            year = cloud_year_set[-1]
        else:
            for i in sorted(cloud_year_set):
                if int(i) > int(year):
                    year = i
                    break
        data_dir = Path(data_dir)
        download_path = data_dir / str(year)
        if os.path.exists(download_path) == False:
            Path(download_path).mkdir(parents=True, exist_ok=True)
        print("STATUS => new year:", year)
        return year
    print("STATUS => current year not finished.")
import re
from pprint import pprint
from datetime import datetime, timedelta
from pathlib import Path
from tqdm import *
from prefect import Flow, Parameter, task, unmapped, mapped
from prefect.executors.dask import LocalDaskExecutor
from loguru import logger as local_logger
import pandas as pd
import numpy as np
import httpx


@task()
def query_cloud_archives(url: str) -> set:
    table = pd.read_html(url, skiprows=2)
    table[0].columns = ['name', 'date', 'size', 'drop']
    table = table[0].drop(['drop'], axis = 1)
    table['date'] =  pd.to_datetime(table['date'], format='%Y-%m-%d %H:%M')#  %d%b%Y:%H:%M:%S.%f')
    pprint(table)
    return table


@task()
def query_local_archives(data_dir: str) -> set:
    data_path = Path(data_dir)
    local_d = []
    for item in data_path.rglob('*.tar.gz'):
        if re.match(r'.*_archive.tar.gz', item.name):
            continue
        dt_m = datetime.fromtimestamp(item.stat().st_mtime)
        size = item.stat().st_size
        local_d.append({'name': item.name, 'date': dt_m, 'size': str(round(size / 1000))})
    local_df = pd.DataFrame(local_d)
    print(local_df)
    return local_df


@task()
def archives_difference(cloud, local):
    diff_df = pd.concat([cloud['name'], local['name']]).drop_duplicates(keep=False)
    if len(diff_df) > 0:
        diff_df = diff_df.replace({np.nan: None})
        return diff_df.to_list()


@task(log_stdout=True, max_retries=3, retry_delay=timedelta(seconds=5))
def download(filename, url, data_dir):
    try:
        print(filename)
        if filename is None:
            return
        download_url = f'{url}/{filename}'
        local_logger.info(f'Starting Download: {download_url}')
        file_path = Path(data_dir) / filename.replace(".tar.gz", "")
        file_path.mkdir(parents=True, exist_ok=True)
        with open(file_path / filename, 'wb') as download_file:
            with httpx.stream("GET", download_url) as response:
                total = int(response.headers["Content-Length"])
                with tqdm(total=total, unit_scale=True, unit_divisor=1024, unit="B") as progress:
                    num_bytes_downloaded = response.num_bytes_downloaded
                    for chunk in response.iter_bytes():
                        download_file.write(chunk)
                        progress.update(response.num_bytes_downloaded - num_bytes_downloaded)
                        num_bytes_downloaded = response.num_bytes_downloaded
    except httpx.ConnectTimeout as e:
        local_logger.error(f'Error message: {e}')
        raise httpx.ConnectTimeout(e)
    except ConnectionError as e:
        local_logger.error(f'Connection error on request for {download_url}')
        local_logger.error(f'Error message: {e}')
    except AttributeError as e:
        local_logger.error(f'{filename} not found.')
        local_logger.error(f'Error message: {e}')


n_workers = 1
executor = LocalDaskExecutor(scheduler="processes", num_workers=n_workers)
with Flow("NOAA files: Download All", executor=executor) as flow:
    base_url = Parameter("base_url", default="https://www.ncei.noaa.gov/data/global-summary-of-the-day/archive")
    data_dir = Parameter("data_dir", default=str(Path("./local_data/global-summary-of-the-day-archive")))

    cloud_df = query_cloud_archives(base_url)
    local_df = query_local_archives(data_dir)
    diff_l = archives_difference(cloud_df, local_df)
    downloads = download(mapped(diff_l), unmapped(base_url), unmapped(data_dir))


if __name__ == "__main__":
    flow.run()
import tarfile, shutil, re
from pprint import pprint
from datetime import datetime
from pathlib import Path
from pendulum import local
from tqdm import *
from prefect import task
from loguru import logger as local_logger
import pandas as pd
import httpx


@task()
def query_cloud_archives(url: str) -> set:
    table = pd.read_html(url, skiprows=2)
    table[0].columns = ['name', 'date', 'size', 'drop']
    table = table[0].drop(['drop'], axis = 1)
    table['date'] =  pd.to_datetime(table['date'], format='%Y-%m-%d %H:%M')
    pprint(table)
    return table


@task()
def query_local_archives(data_dir: str) -> set:
    data_path = Path(data_dir)
    local_d = []
    for item in data_path.rglob('*.tar.gz'):
        # if re.match(r'.*_archive.tar.gz', item.name):
        #     continue
        # if re.match(r'\d\d\d\d.tar.gz', item.name):
        #     continue
        dt_m = datetime.fromtimestamp(item.stat().st_mtime)
        size = item.stat().st_size

        local_d.append({'name': item.name, 'date': dt_m, 'size': str(round(size / 1000))})
    if not local_d:
        return pd.DataFrame()
    else:
        return pd.DataFrame(local_d)


@task()
def archives_difference(cloud, local):
    cloud['date_str'] = cloud['date'].dt.strftime('%Y%m%d_%H%M')
    cloud_set = set([(f"{str(row['name']).replace('.tar.gz', '')}_ts_{row['date_str']}.tar.gz", row['name']) 
                     for index, row in cloud.iterrows()])
    local_set = set([(row['name'], f"{row['name'][:4]}.tar.gz") for index, row in local.iterrows()])
    diff_set = cloud_set.difference(local_set)
    pprint(diff_set)
    diff_l = list(diff_set)
    diff_l.sort()
    return diff_l


@task(retries=3, retry_delay_seconds=5)
async def download(file_item: tuple, url: str, data_dir: str):
    try:
        ts_filename, filename = file_item
        if filename is None:
            return
        if pd.isnull(filename):
            local_logger.warning(f'{filename} == pd.NaT')
            return
        download_url = f'{url}/{filename}'
        file_path = Path(data_dir) / filename.replace(".tar.gz", "")
        file_path.mkdir(parents=True, exist_ok=True)
        local_logger.info(f'Download Starting: {download_url}')
        with open(file_path / ts_filename, 'wb') as download_file:
            with httpx.stream("GET", download_url) as response:
                total = int(response.headers["Content-Length"])
                with tqdm(total=total, unit_scale=True, unit_divisor=1024, unit="B") as progress:
                    num_bytes_downloaded = response.num_bytes_downloaded
                    for chunk in response.iter_bytes():
                        download_file.write(chunk)
                        progress.update(response.num_bytes_downloaded - num_bytes_downloaded)
                        num_bytes_downloaded = response.num_bytes_downloaded
        local_logger.info(f'Download Complete: {download_url}')
        local_logger.info(f'Extract Starting: {ts_filename}')
        extract_dir = Path(file_path) / 'data'
        if extract_dir.exists():
            shutil.rmtree(extract_dir)
        extract_dir.mkdir(parents=True, exist_ok=True)
        with open(str(extract_dir / ts_filename).replace('.tar.gz', ''), 'w') as f:
            pass
        with tarfile.open(file_path / ts_filename) as tar:
            tar.extractall(extract_dir)
        local_logger.info(f'Extract Complete: {ts_filename}')
    except httpx.ConnectTimeout as e:
        local_logger.error(f'Error message: {e}')
        raise httpx.ConnectTimeout(e)
    except ConnectionError as e:
        local_logger.error(f'Connection error on request for {download_url}')
        local_logger.error(f'Error message: {e}')
    except AttributeError as e:
        local_logger.error(f'{filename} not found.')
        local_logger.error(f'Error message: {e}')
    except (Exception) as e:
        ts_filename = file_path / ts_filename
        if Path(ts_filename).exists():
            Path(ts_filename).unlink()
        raise Exception(e)
    except KeyboardInterrupt as e:
        ts_filename = file_path = Path(data_dir) / filename.replace(".tar.gz", "") / ts_filename
        if Path(ts_filename).exists():
            Path(ts_filename).unlink()
        raise KeyboardInterrupt(e)
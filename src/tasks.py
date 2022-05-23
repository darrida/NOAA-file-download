import csv
from csv import QUOTE_MINIMAL
import io
import tarfile, shutil
from datetime import datetime
from pathlib import Path
from tqdm import *
from fs import open_fs
from fs import copy as copy_fs
from prefect import task, get_run_logger
import pandas as pd
import httpx


@task()
def query_cloud_archives(url: str) -> set:
    table = pd.read_html(url, skiprows=2)
    table[0].columns = ['name', 'date', 'size', 'drop']
    table = table[0].drop(['drop'], axis = 1)
    table['date'] =  pd.to_datetime(table['date'], format='%Y-%m-%d %H:%M')
    return table


@task()
def query_local_archives(data_dir: str) -> set:
    data_path = Path(data_dir)
    local_d = []
    for item in data_path.rglob('*.tar.gz'):
        dt_m = datetime.fromtimestamp(item.stat().st_mtime)
        size = item.stat().st_size
        local_d.append({'name': item.name, 'date': dt_m, 'size': str(round(size / 1000))})
    if not local_d:
        return pd.DataFrame()
    else:
        return pd.DataFrame(local_d)


@task()
def archives_difference(cloud, local) -> list:
    logger = get_run_logger()
    cloud['date_str'] = cloud['date'].dt.strftime('%Y%m%d_%H%M')
    cloud_set = set([(f"{str(row['name']).replace('.tar.gz', '')}_ts_{row['date_str']}.tar.gz", row['name']) 
                     for index, row in cloud.iterrows()])
    local_set = set([(row['name'], f"{row['name'][:4]}.tar.gz") for index, row in local.iterrows()])
    diff_set = cloud_set.difference(local_set)
    diff_set.remove(('NaT_ts_NaT.tar.gz', pd.NaT))  # remove set element created by empty pandas dataframe row
    logger.info(f"New or changed year archives: {diff_set or None}")
    diff_l = list(diff_set)
    diff_l.sort()
    return diff_l


@task(retries=3, retry_delay_seconds=5)
async def download_process(file_item: tuple, url: str, data_dir: str):
    logger = get_run_logger()
    try:
        ts_filename, filename = file_item
        logger.info(f"(1) BEGIN Processing {filename}")
        if filename is None:
            return
        if pd.isnull(filename):
            logger.warning(f'{filename} == pd.NaT')
            return
        download_url = f'{url}/{filename}'
        file_path = Path(data_dir) / filename.replace(".tar.gz", "")
        file_path.mkdir(parents=True, exist_ok=True)
        logger.info(f'DOWNLOAD Starting: {download_url}')
        with open(file_path / ts_filename, 'wb') as download_file:
            with httpx.stream("GET", download_url) as response:
                total = int(response.headers["Content-Length"])
                with tqdm(total=total, unit_scale=True, unit_divisor=1024, unit="B") as progress:
                    num_bytes_downloaded = response.num_bytes_downloaded
                    for chunk in response.iter_bytes():
                        download_file.write(chunk)
                        progress.update(response.num_bytes_downloaded - num_bytes_downloaded)
                        num_bytes_downloaded = response.num_bytes_downloaded
        logger.info(f'DOWNLOAD Complete: {download_url}')
        
        # return filename
        
        # logger.info(f'Extract Starting: {ts_filename}')
        # extract_dir = Path(file_path) / 'data'
        # if extract_dir.exists():
        #     shutil.rmtree(extract_dir)
        # extract_dir.mkdir(parents=True, exist_ok=True)

        # with open(str(extract_dir / ts_filename).replace('.tar.gz', ''), 'w') as f:
        #     pass
        extract_and_merge(filename, logger)
        
        # with tarfile.open(file_path / ts_filename) as tar:
        #     tar.extractall(extract_dir)
        logger.info(f"(1) COMPLETED Processing: {filename}")
    except httpx.ConnectTimeout as e:
        logger.error(f'Error message: {e}')
        raise httpx.ConnectTimeout(e)
    except ConnectionError as e:
        logger.error(f'Connection error on request for {download_url}')
        logger.error(f'Error message: {e}')
    except AttributeError as e:
        logger.error(f'{filename} not found.')
        logger.error(f'Error message: {e}')
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






def extract_and_merge(filename: Path, logger):
    year = filename.name[:4]

    exclude_filename = f"{year}_ts_"
    save_to = Path(filename).parent / f"{year}_full.csv"

    mem_fs = open_fs('mem://')
    copy_fs.copy_file(str(filename.parent), filename.name, mem_fs, filename.name)
    
    logger.info(f"(1) BEGIN Extract: {filename}")
    with mem_fs.open(filename.name, 'rb') as fd:
        tar = tarfile.open(fileobj=fd)
        for member in tqdm(tar.getmembers()):
            if member.isreg():  # regular file
                if exclude_filename in member.name:
                    continue
                with mem_fs.open(member.name, 'wb') as fd_file:
                    with tar.extractfile(member.path) as csv_file:
                        fd_file.write(csv_file.read())

        # print(mem_fs.listdir('/'))

        total_lines = 0
        csv_lines = []
        for file_ in tqdm(mem_fs.listdir('/')):
            lines = None
            if exclude_filename in file_:
                continue
            with mem_fs.open(file_, 'rb') as fd_file:
                csv_file = io.TextIOWrapper(fd_file, encoding='utf-8')
                csv_reader = csv.reader(csv_file)
                lines = list(csv_reader)

                total_lines += len(lines[1:])
                
                csv_lines += lines[1:]

        if total_lines == len(csv_lines):
            logger.info(f'(1) COMPLETED Extract: {filename}')
            logger.info(f'(2) BEGINNING Merge: {filename}')
            df = pd.DataFrame(csv_lines, columns=["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","TEMP","TEMP_ATTRIBUTES","DEWP","DEWP_ATTRIBUTES","SLP","SLP_ATTRIBUTES","STP","STP_ATTRIBUTES","VISIB","VISIB_ATTRIBUTES","WDSP","WDSP_ATTRIBUTES","MXSPD","GUST","MAX","MAX_ATTRIBUTES","MIN","MIN_ATTRIBUTES","PRCP","PRCP_ATTRIBUTES","SNDP","FRSHTT"])
            df.to_csv(save_to, index=False, quoting=QUOTE_MINIMAL)
            logger.info(f"(2) COMPLETED Merge: {filename}")

    with open(str(filename).replace('.tar.gz', ''), 'w') as f:
        pass


if __name__ == '__main__':
    data_dir = str(Path("./local_data/global-summary-of-the-day-archive"))
    filename = Path(data_dir) / '2020' / '2020_ts_20210331_0838.tar.gz'
    # filename = Path(data_dir) / '1929' / '1929_ts_20190221_0305.tar.gz'
    from loguru import logger
    extract_and_merge(filename, logger)

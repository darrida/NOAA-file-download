import csv
import gc
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
    logger = get_run_logger()
    logger.info(f"HTTP Request Starting: NOAA Global Summary Archives")
    table = pd.read_html(url, skiprows=2)
    table[0].columns = ['name', 'date', 'size', 'drop']
    table = table[0].drop(['drop'], axis = 1)
    table['date'] =  pd.to_datetime(table['date'], format='%Y-%m-%d %H:%M')
    return table


@task()
def query_local_archives(data_dir: str) -> set:
    data_path = Path(data_dir)
    local_d = []
    # "_ts_" is part of the string for all stored "complete" timestamps (these match NOAA's archive modified dates)
    for item in data_path.rglob('*_ts_*'):
        if ".tar.gz" in item.name:  # the actual tarballs also contain "_ts_"; this removes them from the comparison list
            continue
        dt_m = datetime.fromtimestamp(item.stat().st_mtime)
        size = item.stat().st_size
        local_d.append({'name': f"{item.name.replace('___complete', '')}.tar.gz", 'date': dt_m, 'size': str(round(size / 1000))})
    if not local_d:
        return pd.DataFrame()
    else:
        return pd.DataFrame(local_d)


@task()
def archives_difference(cloud, local) -> list:
    logger = get_run_logger()
    # massage cloud and local sets to match for comparison
    cloud['date_str'] = cloud['date'].dt.strftime('%Y%m%d_%H%M')  # update NOAA archive timestamp to match local formatting
    cloud_set = set([(f"{str(row['name']).replace('.tar.gz', '')}_ts_{row['date_str']}.tar.gz", row['name']) 
                     for index, row in cloud.iterrows()])
    local_set = set([(row['name'], f"{row['name'][:4]}.tar.gz") for index, row in local.iterrows()])
    # diff set is any year archive that is missing or newer than local data
    diff_set = cloud_set.difference(local_set)
    diff_set.remove(('NaT_ts_NaT.tar.gz', pd.NaT))  # remove set element created by empty pandas dataframe row
    logger.info(f"New or changed year archives count: {len(diff_set) or None}")
    diff_l = list(diff_set)
    diff_l.sort()
    return diff_l


@task(retries=3, retry_delay_seconds=5)
async def download_and_merge(file_item: tuple, url: str, data_dir: str):
    logger = get_run_logger()
    try:
        ts_filename, filename = file_item
        logger.info(f"BEGIN Downlard/Merge Process: {filename}")
        if filename is None:
            return
        if pd.isnull(filename):
            logger.warning(f'{filename} == pd.NaT')
            return
        
        # TODO: move download code to separate support function
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
        
        merged_df = extract_and_merge(file_path / ts_filename, logger)
        
        if not isinstance(merged_df, pd.DataFrame):
            logger.info(f"FAILED Download/Merge Process: {filename}")
            return
        
        logger.info(f"COMPLETED Download/Merge Process: {filename}")
        return merged_df
        with open(f"{str(file_path / ts_filename).replace('.tar.gz', '')}___complete", 'w') as f:
            pass
            
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


def extract_and_merge(filename: Path, logger: get_run_logger):
    """
    Extracts Tarfile Multiple Records Files into Single CSV
    - (1) copies tarfile into memory (ram) filesystem (root)
    - (2) extracts all files (csv) from tarfile into same memory filesystem location
    - (3) opens and merges all csvs into a single list of lists (includes original filename in last index of each list)
    - (4) loads list of lists into a pandas dataframe, then exports using `.to_csv()`
    - (5) returns bool indicator of success or failure

    Args:
        filename (pathlib.Path): full filepath + filename of tarfile to process
        logger (get_run_logger): initialized Prefect 2.0 `get_run_logger`
    
    Returns:
        bool: Success (True) or failure (False)
    """
    year = filename.name[:4]
    exclude_filename = f"{year}_ts_"
    save_to = Path(filename).parent / f"{year}_full.csv"

    # (1) copy tarfile into memory filesystem
    mem_fs = open_fs('mem://')
    copy_fs.copy_file(str(filename.parent), filename.name, mem_fs, filename.name)
    
    logger.info(f"BEGIN Extract: {filename}")
    with mem_fs.open(filename.name, 'rb') as fd:
        tar = tarfile.open(fileobj=fd)
         # (2) extract files from tarball into same memory filesystem
        for member in tqdm(tar.getmembers()):
            if member.isreg():  # regular file
                if exclude_filename in member.name:
                    continue
                with mem_fs.open(member.name, 'wb') as fd_file:
                    with tar.extractfile(member.path) as csv_file:
                        fd_file.write(csv_file.read())
        logger.info(f'COMPLETED Extract: {filename}')

        logger.info(f'BEGINNING Merge: {filename}')
        # (3) open and merge all csv records into same list of lists (`csv_lines`)
        gc.disable()
        print('GC Enabled:', gc.isenabled(), '(temp disable)')
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

                lines = [x.append(file_) or x for x in lines]  # add original filename to last column of every record
                total_lines += len(lines[1:])  # counter that used to compare totals below
                csv_lines += lines[1:]  # merge into unified records set for year
        gc.collect()
        gc.enable()
        print('GC Enabled:', gc.isenabled())
        if not gc.isenabled() == True:
            raise Exception('FAILED to re-enable GARBAGE COLLECTION')

        if total_lines == len(csv_lines):
            # (4) load `csv_lines` into dataframe and export to csv
            logger.info(f"COMPLETED Merge: {filename}")
            df = pd.DataFrame(csv_lines, columns=["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","TEMP","TEMP_ATTRIBUTES","DEWP","DEWP_ATTRIBUTES","SLP","SLP_ATTRIBUTES","STP","STP_ATTRIBUTES","VISIB","VISIB_ATTRIBUTES","WDSP","WDSP_ATTRIBUTES","MXSPD","GUST","MAX","MAX_ATTRIBUTES","MIN","MIN_ATTRIBUTES","PRCP","PRCP_ATTRIBUTES","SNDP","FRSHTT","SOURCE_FILE"])
            return df
            # df.to_csv(save_to, index=False, quoting=QUOTE_MINIMAL)

            logger.info(f"COMPLETED Merge: {filename}")
            # (5) return bool indicator of success
            success = True
        else:
            logger.info(f"FAILED to process: {filename}\nLines in Files: {total_lines}; Lines from Merge: {len(csv_lines)}")
            return False

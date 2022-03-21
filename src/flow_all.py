from pathlib import Path
from datetime import timedelta
from prefect import flow
from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import IntervalSchedule
from prefect.flow_runners import SubprocessFlowRunner
from pydantic import BaseModel, DirectoryPath, HttpUrl
from src.tasks import calc, local, cloud


class Parameters(BaseModel):
    url: HttpUrl = "https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/"
    data_dir: DirectoryPath = Path("./local_data/noaa_temp_downloads")
    download_chunk: int = 100
    workers: int = 8


@flow(name='Loop Files to Download')
def loop_downloads(file_l, url, year, dir):
    result_l = []
    for f in file_l:
        result = cloud.download_new_csvs(url=url, year=year, diff_set=f, data_dir=dir)
        result_l.append(result)
    return result_l


@flow(name='noaa-files-all')
def main(): #url, data_dir, download_chunk, workers):# p: Parameters):
    p = Parameters()
    t1_year = local.find_highest_year(data_dir=p.data_dir)
    t2_url = local.build_url(base_url=p.url, year=t1_year)
    t3_cset = cloud.query_cloud_csvs(url=t2_url, year=t1_year)
    t4_lset = local.query_local_csvs(year=t1_year, data_dir=p.data_dir)
    t5_diff_l = calc.query_diff_local_cloud(
        local_set=t4_lset, cloud_set=t3_cset, chunk_size=p.download_chunk, workers=p.workers)
    t6_next = loop_downloads(t5_diff_l, t2_url, t1_year, p.data_dir)
    t7_task = cloud.find_new_year(url=p.url, next_year=t6_next, year=t1_year, data_dir=p.data_dir)


DeploymentSpec(
    name='download-deployment2',
    flow_location='./src/flow_all.py',
    flow=main,
    schedule=IntervalSchedule(interval=timedelta(days=1)),
    # flow_runner=SubprocessFlowRunner(virtualenv='./.venv', stream_output=True),
    tags=['noaa', 'heroku-postgres'],
    # parameters={
    #     'url': "https://www.ncei.noaa.gov/data/global-summary-of-the-day/access/",
    #     'data_dir': Path("./local_data/noaa_temp_downloads"),
    #     'download_chunk': 100,
    #     'workers': 8,
    # }
)


if __name__ == '__main__':
    # p = Parameters()
    main()

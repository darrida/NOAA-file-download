from prefect import task
from icecream import ic


@task() # log_stdout=True)
def query_diff_local_cloud(local_set: set, cloud_set: set, chunk_size: int, workers: int) -> set:
    diff_set = cloud_set.difference(local_set)
    if diff_set:
        print(f"{len(diff_set)} new data files available for download.")
    else:
        print(f"No new data files for this run.")
    diff_l = list(diff_set)
    if chunk_size > 1000:
        chunk_size = 1000
        print("CHANGED TO DEFAULT CHUCK SIZE: 1000")
    if len(diff_l) < workers * chunk_size:
        ic(len(diff_l), workers, chunk_size)
        chunk_size = int(len(diff_l) / workers) + 1
        print(f"LESS THAN {workers * 200} RECORS: Chunk size: {chunk_size}")
    diff_l = [diff_l[x : x + chunk_size] for x in range(0, len(diff_l), chunk_size)]
    return diff_l
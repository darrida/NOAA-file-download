import csv
from csv import QUOTE_MINIMAL
from pathlib import Path
import os

import pandas

list1 = [
    [1, 2, 3, 4, 5],
    [5, 4, 3, 2, 1]
]

list2 = [
    [11, 22, 33, 44, 55],
    [55, 44, 33, 22, 11]
]

print(list1 + list2)


data_dir = str(Path("./local_data/global-summary-of-the-day-archive") / '2014' / 'data')

file_l = os.listdir(data_dir)

# csv_lines = []

from tqdm import tqdm

first_file = True
total_lines = 0
csv_lines = []
for count, f in tqdm(enumerate(file_l, start=1)):
    f = Path(data_dir) / f
    with open(f, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)

        lines = list(csv_reader)
        # print(len(lines))
        total_lines += len(lines)
        # if count == 1:
        #     csv_lines += lines[0]
        csv_lines += lines[1:]

df = pandas.DataFrame(csv_lines, columns=["STATION","DATE","LATITUDE","LONGITUDE","ELEVATION","NAME","TEMP","TEMP_ATTRIBUTES","DEWP","DEWP_ATTRIBUTES","SLP","SLP_ATTRIBUTES","STP","STP_ATTRIBUTES","VISIB","VISIB_ATTRIBUTES","WDSP","WDSP_ATTRIBUTES","MXSPD","GUST","MAX","MAX_ATTRIBUTES","MIN","MIN_ATTRIBUTES","PRCP","PRCP_ATTRIBUTES","SNDP","FRSHTT"])
df.to_csv(Path(data_dir) / '2014_full_test.csv', 
    index=False, 
    quoting=QUOTE_MINIMAL)

from pprint import pprint
print(total_lines)
print(len(csv_lines))
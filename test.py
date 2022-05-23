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

print([x.append('test') or x for x in list2])
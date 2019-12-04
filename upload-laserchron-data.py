#!/usr/bin/env python

from pathlib import Path
from itertools import chain

data_path = "/Users/Daven/Projects/EarthCube-Geochron/Sparrow-instances/Sparrow-LaserChron/Data"

path = Path(data_path)

file_list = chain(path.glob("**/*.xls"), path.glob("**/*.xls[xm]"))

for fn in file_list:
    print(fn)

# Loop through extract files and jam them all together

import csv
import pandas as pd
import os

extract_files = [fn for fn in os.listdir("/home/ubuntu/sharded_input_TOTAL/") if fn.startswith("AUTOGRAPH_201")]

base_output = extract_files[0]

to_append = extract_files[1:]

with open(base_output, "a") as base_reader:
    for ef in to_append:
        for chunk in pd.read_csv(ef, sep=";", quotechar='"', quoting=csv.QUOTE_ALL, chunksize=10000):
            chunk.to_csv(base_reader, header=False, sep=";", quotechar='"', quoting=csv.QUOTE_ALL, index=False)

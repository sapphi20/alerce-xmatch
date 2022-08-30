import sys

import pandas as pd
from PyAstronomy import pyasl


file_name = sys.argv[1]
df = pd.read_csv(file_name)

new_arr = []

for elem in df.to_numpy():
    join = elem[0] + ' ' + elem[1]
    join = pyasl.coordsSexaToDeg(join)
    new_arr.append(join)

new_df = pd.DataFrame(new_arr, columns=['ra', 'dec'])
new_df.to_csv('data_deg.csv', sep=',', index=False)
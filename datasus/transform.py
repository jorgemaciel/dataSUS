# %%
from datasus import read_dbc
import pandas as pd
import os


def dbc_csv(path, csv_path):
    for filename in os.listdir(path):
        if filename.endswith('.dbc'):
            filepath = os.path.join(path, filename)
            df = pd.DataFrame(read_dbc(filepath))
            csv_filename = filename.replace('.dbc', '.csv')
            csv_filepath = os.path.join(path, csv_filename)
            df.to_csv(csv_filepath, index=False)
            destination_filepath = os.path.join(csv_path, csv_filename)
            os.rename(csv_filepath, destination_filepath)
            print(f"Converted {filename} to {csv_filename} and moved to {csv_path}")

# %%
dbc_csv('../data/dbc', '../data/csv')
# %%

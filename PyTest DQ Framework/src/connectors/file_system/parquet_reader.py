import pandas as pd
import os

class ParquetReader:
    def process(self, path, include_subfolders=False):
        dataframes = []
        if include_subfolders:
            for root, _, files in os.walk(path):
                for file in files:
                    if file.endswith('.parquet'):
                        df = pd.read_parquet(os.path.join(root, file))
                        dataframes.append(df)
        else:
            for file in os.listdir(path):
                if file.endswith('.parquet'):
                    df = pd.read_parquet(os.path.join(path, file))
                    dataframes.append(df)
        if dataframes:
            return pd.concat(dataframes, ignore_index=True)
        else:
            return pd.DataFrame()
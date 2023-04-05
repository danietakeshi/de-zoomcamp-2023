FROM prefecthq/prefect:2.6.5-python3.10
RUN ["pip", "install", "gcsfs", "pandas", "requests", "prefect_gcp", "pyarrow", "fastparquet", "pandas-gbq"]
RUN mkdir ./parquet_files
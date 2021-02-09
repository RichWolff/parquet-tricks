import pandas as pd
import pyarrow as pa
from pyarrow import parquet as pq
import fsspec
import os

fs = fsspec.filesystem(
    protocol='abfs',
    account_name=os.getenv('azure_account_name'),
    account_key=os.getenv('azure_sas_key')
)

ds = pa.Table.from_pandas(parquet_df)
pq.write_to_dataset(
    table=ds,
    root_path=f'{os.getenv('azure_container')}/prefix',
    filesystem=fs,
    partition_cols=['site', 'asset', 'date'],
    row_group_size=10000,
    coerce_timestamps='ms',
    allow_truncated_timestamps=True,
    flavor='spark'
)

import pyarrow.parquet as pq
import gc
import pandas as pd

batch_size = 2000
parquet_file = pq.ParquetFile('altscore_data/mobility_data.parquet')

for batch in parquet_file.iter_batches(batch_size=batch_size):
    df_batch = batch.to_pandas()
    print(len(df_batch))
    del df_batch
    gc.collect()
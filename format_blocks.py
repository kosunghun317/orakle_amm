import os
import polars as pl
import pandas as pd

data_path = "./data/blocks/ethereum__blocks__*.parquet"
data_path = os.path.expanduser(data_path)

timestamps_blocks_fees = (
    pl.scan_parquet(data_path)
    .select(pl.col("block_number", "timestamp", "base_fee_per_gas"))
    .collect(streaming=True)
    .to_numpy()
)

df = pd.DataFrame(
    timestamps_blocks_fees, columns=["blockNumber", "timestamp", "baseFeePerGas"]
)

df.to_csv("./data/blocks/blockNumber_timestamp_baseFeePerGas.csv", index=False)

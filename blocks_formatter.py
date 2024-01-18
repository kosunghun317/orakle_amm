import os
import polars as pl
import pandas as pd
from utils import *
import glob

# ETHEREUM blocks
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

# ARBITRUM blocks
data_path = "./data/arbitrum_blocks/arbitrum_blocks_*.csv"
data_list = glob.glob(data_path)
dataframes = [pl.read_csv(data) for data in data_list]

for i in range(len(dataframes)):
    dataframes[i] = dataframes[i].with_columns(
        [
            (
                pl.col("timestamp").str.strptime(
                    pl.Datetime, "%Y-%m-%d %H:%M:%S%.f UTC", strict=False
                )
                / 1000000000
            ).cast(pl.Int64)
        ]
    )

df = pl.concat(dataframes)
df = df.sort("block_number")
df = df.rename({"block_number": "blockNumber"})
df.write_csv("./data/arbitrum_blocks/blockNumber_timestamp_baseFeePerGas.csv")

df = pd.read_csv("./data/arbitrum_blocks/blockNumber_timestamp_baseFeePerGas.csv")
df["baseFeePerGas"] = 10**8
df.to_csv("./data/arbitrum_blocks/blockNumber_timestamp_baseFeePerGas.csv", index=False)

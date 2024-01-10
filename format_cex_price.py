import polars as pl

"""
extract timestamp and opens from csv file downloaded at https://www.binance.com/en/landing/data
put original data into raw/ and add the path to .gitignore to avoid the upload error.
"""

# ETHUSD
ETHUSD_oct = pl.read_csv(
    "./data/cex_price/raw/ETHUSD-1s-2023-10.csv", has_header=False
).select(["column_1", "column_2"])
ETHUSD_oct.columns = ["timestamp", "price"]
ETHUSD_oct = ETHUSD_oct.with_columns(pl.col("timestamp") // 1000)

ETHUSD_nov = pl.read_csv(
    "./data/cex_price/raw/ETHUSD-1s-2023-11.csv", has_header=False
).select(["column_1", "column_2"])
ETHUSD_nov.columns = ["timestamp", "price"]
ETHUSD_nov = ETHUSD_nov.with_columns(pl.col("timestamp") // 1000)

ETHUSD_total = pl.concat([ETHUSD_oct, ETHUSD_nov])
ETHUSD_total.write_csv("./data/cex_price/ETHUSD_total.csv")

###################################################################################
#                              repeat for new pairs                               #
###################################################################################

# ETHBTC
ETHBTC_oct = pl.read_csv(
    "./data/cex_price/raw/ETHBTC-1s-2023-10.csv", has_header=False
).select(["column_1", "column_2"])
ETHBTC_oct.columns = ["timestamp", "price"]
ETHBTC_oct = ETHBTC_oct.with_columns(pl.col("timestamp") // 1000)

ETHBTC_nov = pl.read_csv(
    "./data/cex_price/raw/ETHBTC-1s-2023-11.csv", has_header=False
).select(["column_1", "column_2"])
ETHBTC_nov.columns = ["timestamp", "price"]
ETHBTC_nov = ETHBTC_nov.with_columns(pl.col("timestamp") // 1000)

ETHBTC_total = pl.concat([ETHBTC_oct, ETHBTC_nov])
ETHBTC_total.write_csv("./data/cex_price/ETHBTC_total.csv")

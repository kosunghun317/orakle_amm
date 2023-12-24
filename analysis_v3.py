"""
Analysis on Uniswap V3
"""
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
import web3
from decimal import Decimal
import time
import pandas as pd
import polars as pl
import numpy as np
from utils import *

load_dotenv()


def analyze_v3_data(network, dex, base_token, quote_token, fee, use_instant_volatility):
    """
    Calculate the theoretical predictions, compare with historical data.
    """

    #################### read files ####################
    events_df = pd.read_csv(
        f"data/onchain_events/{network}_{dex}_{base_token}_{quote_token}_events.csv"
    )
    blocks_df = pd.read_csv("data/blocks/blockNumber_timestamp_baseFeePerGas.csv")
    cex_price_df = pd.read_csv(
        f"data/cex_price/{token_to_ticker(base_token)}{token_to_ticker(quote_token)}_total.csv"
    )

    #################### bind the price data to each block ####################
    """
    We will refer the price 4 seconds before the block timestamp. 
    This is when the block building auction ends usually.
    It turned out that arbitrageurs get maximal profit if they execute the order at that moment.
    See https://ethresear.ch/t/empirical-analysis-of-cross-domain-cex-dex-arbitrage-on-ethereum/17620
    """
    cex_price_df["refPrice"] = cex_price_df["price"].shift(4)
    blocks_price = pd.merge(blocks_df, cex_price_df, on="timestamp", how="left")

    # estimating volatility is tricky part, so we use one of following:
    if use_instant_volatility:
        """
        Instantaneous volatility from 1-minute return.
        For the derivation see https://blog.naver.com/chunjein/100209878606
        """
        blocks_price["VolSquared"] = (
            2
            * (
                (blocks_price["refPrice"] - blocks_price["refPrice"].shift(5))
                / blocks_price["refPrice"].shift(5)
                - np.log(blocks_price["refPrice"] / blocks_price["refPrice"].shift(5))
            )  # difference in arithmetic and logarithmic (approx.) 1 minute return
            * (60 * 60 * 24)
            / (blocks_price["timestamp"] - blocks_price["timestamp"].shift(5))
        )  # converted to daily volatility.
    else:
        """
        Rolling volatility from 1-minute return.
        We use 1 hour rolling window. Can be freely modified.
        """
        blocks_price["VolSquared"] = (
            np.log(
                blocks_price["refPrice"] / blocks_price["refPrice"].shift(5)
            )  # (approx.) 1 minute return
            .rolling(window=5 * 60)  # samples within (approx.) 1 hour
            .std()
            ** 2
            * 60
            * 24
        )  # converted to daily volatility.

    # parameter lambda from MMR23 paper.
    blocks_price["lambda"] = (
        60 * 60 * 24 / (blocks_price["timestamp"] - blocks_price["timestamp"].shift(1))
    )

    # fee rate.
    gamma = np.log(1 + fee / 10000)

    # composite parameter from MMR23 paper.
    blocks_price["eta"] = (
        np.sqrt(2 * blocks_price["lambda"])
        * gamma
        / np.sqrt(blocks_price["VolSquared"])
    )

    # theoretical prediction from MMR23 paper.
    blocks_price["tradeProbability"] = 1 / (1 + blocks_price["eta"])


if __name__ == "__main__":
    network = "MAINNET"
    dex = "UNI_V3"
    base_token = "WETH"
    quote_token = "USDC"
    fee = 30  # in bps
    use_instant_volatility = False
    analyze_v3_data(network, dex, base_token, quote_token, fee, use_instant_volatility)

"""
processing data on Uniswap V2 and its forks. 
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
import matplotlib.pyplot as plt

load_dotenv()


def v2_swaps_and_arbitrages(
    network, dex, base_token, quote_token, fee, use_instant_volatility, interval, window
):
    """
    Read files, compute the parameters, theoretical predictions, then
    compare them against realized data.
    """
    ############################################################
    #                        read files                        #
    ############################################################

    events_df = pd.read_csv(
        f"data/onchain_events/{network}_{dex}_{base_token}_{quote_token}_events.csv"
    )
    blocks_df = pd.read_csv(
        f"data/{network}_blocks/blockNumber_timestamp_baseFeePerGas.csv"
    )
    cex_price_df = pd.read_csv(
        f"data/cex_price/{token_to_ticker(base_token)}{token_to_ticker(quote_token)}_total.csv"
    )
    """
    For the case of Mainnet, we will refer the price 4 seconds before the block timestamp. 
    This is when the block building auction ends usually.
    It is also turned out that arbitrageurs get maximal profit if they execute 
    the order at that moment. See 
    https://ethresear.ch/t/empirical-analysis-of-cross-domain-cex-dex-arbitrage-on-ethereum/17620
    """
    if network == "MAINNET":
        cex_price_df["price"] = (
            cex_price_df["price"].shift(4).fillna(cex_price_df["price"][0])
        )

    ############################################################
    #                    compute parameters                    #
    ############################################################

    if use_instant_volatility:
        """
        Instantaneous volatility from return.
        For the derivation see http://dx.doi.org/10.3905/jpm.1994.409478
        """
        cex_price_df["volSquared"] = (
            2
            * (
                (
                    cex_price_df["price"]
                    - cex_price_df["price"]
                    .shift(interval)
                    .fillna(cex_price_df["price"][0])
                )
                / cex_price_df["price"]
                .shift(interval)
                .fillna(
                    cex_price_df["price"][0]
                )  # use the first element as proxy for the previous price
                - np.log(
                    cex_price_df["price"]
                    / cex_price_df["price"]
                    .shift(interval)
                    .fillna(cex_price_df["price"][0])
                )
            )  # difference in arithmetic and logarithmic return
            * 60
            * 60
            * 24
            / interval
        )  # converted into daily timeframe.
    else:
        """
        Rolling volatility from logarithmic return.
        We use rolling window. Can be freely modified.

        TODO: completely rewrite this part.
        We first get logarithmic (interval) return,
        then rollover (window) samples with step size being (interval).
        We don't want the std of 1-hour returns from 00:00:00 to 00:00:23 :(
        Instead we want the std of 1-hour returns from 00:00:00 to 23:00:00.
        """
        # cex_price_df["volSquared"] = (
        #     np.log(
        #         cex_price_df["price"]
        #         / cex_price_df["price"].shift(interval).fillna(cex_price_df["price"][0])
        #     )  # logarithmic return
        #     .rolling(window=window)  # samples within interval
        #     .std()
        #     ** 2
        #     * 60
        #     * 60
        #     * 24
        #     / interval
        # )  # converted into daily timeframe.
    cex_price_df.fillna(0, inplace=True)

    blocks_price = pd.merge(blocks_df, cex_price_df, on="timestamp", how="left")
    blocks_price["lambda"] = (60 * 60 * 24) * len(blocks_df) / len(cex_price_df)
    """
    ^-------parameter lambda for Poisson process, which can be thought 
    as inverse of mean block time normalized in daily time.
    """
    gamma = np.log(1 + fee / 10000)  # fee rate.
    blocks_price["eta"] = (
        np.sqrt(2 * blocks_price["lambda"])
        * gamma
        / np.sqrt(blocks_price["volSquared"])
    )  # composite parameter.
    blocks_price["tradeProbability"] = 1 / (1 + blocks_price["eta"])

    ############################################################
    #                       Predictions                        #
    ############################################################

    blocks_price["LVRperPoolValueRate"] = (
        blocks_price["volSquared"] / 8 / blocks_price["lambda"]
    )  # from MMRZ22
    blocks_price["ARBperPoolValueRate"] = (
        blocks_price["volSquared"]
        / 8
        * blocks_price["tradeProbability"]
        * (
            (np.exp(gamma / 2) + np.exp(-gamma / 2))
            / (2 * (1 - blocks_price["volSquared"] / (8 * blocks_price["lambda"])))
        )
        / blocks_price["lambda"]
    )  # from MMR23

    blocks_price["expLVRperPoolValue"] = blocks_price["LVRperPoolValueRate"].cumsum()
    blocks_price["expARBperPoolValue"] = blocks_price["ARBperPoolValueRate"].cumsum()

    ############################################################
    #                     Historical Data                      #
    ############################################################

    blocks_price_events = pd.merge(
        blocks_price, events_df, on="blockNumber", how="outer"
    )
    blocks_price_events.fillna(0, inplace=True)

    blocks_price_events["poolValue"] = 2 * np.sqrt(
        blocks_price_events["quoteReserve"] * blocks_price_events["baseReserve"] * blocks_price_events["price"]
    ) # immune to flashloan and sandwich attack
    blocks_price_events["LVR"] = -(10000 - fee) / 10000 * (
        blocks_price_events["baseIn"] * blocks_price_events["price"]
        + blocks_price_events["quoteIn"]
    ) + (
        blocks_price_events["baseOut"] * blocks_price_events["price"]
        + blocks_price_events["quoteOut"]
    )  # LVR, which is equal to trader's PnL without swap fee and gas cost
    blocks_price_events["FEE"] = (
        fee
        / 10000
        * (
            blocks_price_events["baseIn"] * blocks_price_events["price"]
            + blocks_price_events["quoteIn"]
        )
    )  # Fee income
    if token_to_ticker(base_token) == "ETH":
        """
        (potential) arbitrage profit after swap fee and gas cost.
        gas fee 140k (120k for V3) is selected from 5% percentile of gas
        cost distribution, assuming that the arbitrageurs optimized their codes.
        See https://twitter.com/atiselsts_eth/status/1719693946375258507
        """
        blocks_price_events["ARB"] = (
            blocks_price_events["LVR"]
            - blocks_price_events["FEE"]
            - blocks_price_events["baseFeePerGas"]
            * 140000
            / 10**18
            * blocks_price_events["price"]
        )
    else:
        blocks_price_events["ARB"] = (
            blocks_price_events["LVR"]
            - blocks_price_events["FEE"]
            - blocks_price_events["baseFeePerGas"] * 140000 / 10**18
        )
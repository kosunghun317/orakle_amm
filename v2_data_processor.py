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

    cex_price_df["return"] = (
        cex_price_df["price"]
        - cex_price_df["price"].shift(interval).fillna(cex_price_df["price"][0])
    ) / cex_price_df["price"].shift(interval).fillna(
        cex_price_df["price"][0]
    )  # arithmetic return
    cex_price_df["logReturn"] = np.log(
        cex_price_df["price"]
        / cex_price_df["price"].shift(interval).fillna(cex_price_df["price"][0])
    )  # logarithmic return

    if use_instant_volatility:
        """
        Instantaneous volatility from return.
        For the derivation see http://dx.doi.org/10.3905/jpm.1994.409478
        """
        cex_price_df["volSquared"] = 2 * (
            cex_price_df["return"] - cex_price_df["logReturn"]
        )  # difference in arithmetic and logarithmic return
    else:
        """
        Rolling volatility from logarithmic return.
        We rollover (window) samples with step size being (interval).
        """
        cex_price_df["modulus"] = (
            cex_price_df["timestamp"] - cex_price_df["timestamp"].min()
        ) % interval

        for i in range(interval):
            # filter the indexes
            mask = cex_price_df["modulus"] == i
            masked_logReturns = cex_price_df.loc[mask, "logReturn"]

            # j >= window
            std_squared = masked_logReturns.rolling(window).std() ** 2
            cex_price_df.loc[mask, "volSquared"] = std_squared

            # 2 <= j < window
            for j in range(2, window):
                cex_price_df.loc[i + interval * (j - 1), "volSquared"] = (
                    masked_logReturns[:j].std() ** 2
                )

            # j = 1: we use instantaneous volatility
            cex_price_df.loc[i, "volSquared"] = 2 * (
                cex_price_df.loc[i, "return"] - cex_price_df.loc[i, "logReturn"]
            )
    cex_price_df["volSquared"] *= (
        60 * 60 * 24 / interval
    )  # convert into daily timeframe.

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
    )  # from MMRZ22, multiplied by avg block time (= inverse of lambda)
    blocks_price["ARBperPoolValueRate"] = (
        blocks_price["volSquared"]
        / 8
        * blocks_price["tradeProbability"]
        * (
            (np.exp(gamma / 2) + np.exp(-gamma / 2))
            / (2 * (1 - blocks_price["volSquared"] / (8 * blocks_price["lambda"])))
        )
        / blocks_price["lambda"]
    )  # from MMR23, multiplied by avg block time (= inverse of lambda)

    blocks_price["expLVRperPoolValue"] = blocks_price["LVRperPoolValueRate"].cumsum()
    blocks_price["expARBperPoolValue"] = blocks_price["ARBperPoolValueRate"].cumsum()

    ############################################################
    #                     Historical Data                      #
    ############################################################

    blocks_price_events = pd.merge(
        blocks_price, events_df, on="blockNumber", how="left"
    )

    # fill the missing values of blocks without swaps
    # forward fill
    blocks_price_events["totalSupply"].ffill(inplace=True)
    blocks_price_events["baseReserve"].ffill(inplace=True)
    blocks_price_events["quoteReserve"].ffill(inplace=True)
    # backward fill
    blocks_price_events["totalSupply"].bfill(inplace=True)
    blocks_price_events["baseReserve"].bfill(inplace=True)
    blocks_price_events["quoteReserve"].bfill(inplace=True)
    # empty swap values are filled with 0
    blocks_price_events.fillna(0, inplace=True)

    blocks_price_events["poolValue"] = 2 * np.sqrt(
        blocks_price_events["quoteReserve"]
        * blocks_price_events["baseReserve"]
        * blocks_price_events["price"]
    )  # this is immune to pool value manipulation from flashloan and sandwich attack
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

    ############################################################
    #                      Process data                        #
    ############################################################
    """
    entire swap record for profit analysis. This contains retail orderflow too.
    """
    swaps = blocks_price_events[blocks_price_events["FEE"] > 0].copy()
    swaps["swapSize"] = swaps["baseIn"] * swaps["price"] + swaps["quoteIn"]

    """
    arbitrage-only record. This is for error analysis between theory and real.
    """
    total_arb_per_block = blocks_price_events.groupby("blockNumber").sum()["ARB"]
    mask = blocks_price_events["blockNumber"].isin(
        list(total_arb_per_block[total_arb_per_block > 0.0].index)
    )
    arbitrages = blocks_price_events[mask]

    df = pd.DataFrame(
        columns=[
            "timestamp",
            "meanVolSquared",
            "meanPoolValue",
            "meanBaseFeePerGas",
            "expectedLVRperPoolValue",  # for error analysis
            "realizedLVRperPoolValue",  # for error analysis
            "expectedARBperPoolValue",  # for error analysis
            "realizedARBperPoolValueWithoutGas",  # for error analysis
            "realizedARBperPoolValueWithGas",  # for error analysis
        ]
    )

    start_time = int(datetime(2023, 10, 1, tzinfo=timezone.utc).timestamp())
    end_time = int(datetime(2023, 12, 1, tzinfo=timezone.utc).timestamp())

    while start_time < end_time:
        blocks_price_events_in_interval = blocks_price_events[
            (start_time <= blocks_price_events["timestamp"])
            & (blocks_price_events["timestamp"] < start_time + interval)
        ]
        arbitrages_in_interval = arbitrages[
            (start_time <= arbitrages["timestamp"])
            & (arbitrages["timestamp"] < start_time + interval)
        ]

        new_row = pd.DataFrame(
            [
                [
                    start_time,
                    blocks_price_events_in_interval["volSquared"].mean(),
                    blocks_price_events_in_interval["poolValue"].mean(),
                    blocks_price_events_in_interval["baseFeePerGas"].mean(),
                    blocks_price_events_in_interval["LVRperPoolValueRate"].sum(),
                    (
                        arbitrages_in_interval["LVR"]
                        / arbitrages_in_interval["poolValue"]
                    ).sum(),
                    blocks_price_events_in_interval["ARBperPoolValueRate"].sum(),
                    (
                        (arbitrages_in_interval["LVR"] - arbitrages_in_interval["FEE"])
                        / arbitrages_in_interval["poolValue"]
                    ).sum(),
                    (
                        arbitrages_in_interval["ARB"]
                        / arbitrages_in_interval["poolValue"]
                    ).sum(),
                ]
            ],
            columns=[
                "timestamp",
                "meanVolSquared",
                "meanPoolValue",
                "meanBaseFeePerGas",
                "expectedLVRperPoolValue",
                "realizedLVRperPoolValue",
                "expectedARBperPoolValue",
                "realizedARBperPoolValueWithoutGas",
                "realizedARBperPoolValueWithGas",
            ],
        )

        df = pd.concat([df, new_row], ignore_index=True)

        start_time += interval

    return (swaps, df)

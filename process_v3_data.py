"""
processing data on Uniswap V3
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


def v3_swaps_and_arbitrages(
    network, dex, base_token, quote_token, fee, use_instant_volatility
):
    """
    Read files, compute the parameters, theoretical predictions, then
    compare them against realized data.
    """
    ############################################################
    #                        read files                        #
    ############################################################

    events_df = pd.read_csv(
        f"data/onchain_events/{network}_{dex}_{base_token}_{quote_token}_{fee}bps_events.csv"
    )
    events_df.rename(columns={"price": "ammPrice"}, inplace=True)
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

    interval = 60 * 60
    if use_instant_volatility:  # volatility.
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
                / cex_price_df["price"].shift(interval).fillna(cex_price_df["price"][0])
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
        )  # converted to daily timeframe.
    else:
        """
        Rolling volatility from logarithmic return.
        We use rolling window. Can be freely modified.
        """
        cex_price_df["volSquared"] = (
            np.log(
                cex_price_df["price"]
                / cex_price_df["price"].shift(interval).fillna(cex_price_df["price"][0])
            )  # logarithmic return
            .rolling(window=interval)  # samples within interval
            .std()
            ** 2
            * 60
            * 60
            * 24
            / interval
        )  # converted to daily timeframe.
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

    blocks_price["instLVRperPoolValue"] = (
        blocks_price["volSquared"] / 8 / blocks_price["lambda"]
    )  # from MMRZ22
    blocks_price["instARBperPoolValue"] = (
        blocks_price["volSquared"]
        * blocks_price["tradeProbability"]
        * (
            (np.exp(gamma / 2) + np.exp(-gamma / 2))
            / (2 * (1 - blocks_price["volSquared"] / (8 * blocks_price["lambda"])))
        )
        / 8
        / blocks_price["lambda"]
    )  # from MMR23
    blocks_price["instFEEperPoolValue"] = (
        blocks_price["instLVRperPoolValue"] - blocks_price["instARBperPoolValue"]
    )  # from MMRZ22

    blocks_price["expLVRperPoolValue"] = blocks_price["instLVRperPoolValue"].cumsum()
    blocks_price["expARBperPoolValue"] = blocks_price["instARBperPoolValue"].cumsum()

    ############################################################
    #                     Historical Data                      #
    ############################################################

    blocks_price_events = pd.merge(
        blocks_price, events_df, on="blockNumber", how="left"
    )
    blocks_price_events.fillna(0, inplace=True)

    blocks_price_events["poolValue"] = (
        blocks_price_events["liquidity"] * np.sqrt(blocks_price_events["ammPrice"])
        + blocks_price_events["liquidity"]
        / np.sqrt(blocks_price_events["ammPrice"])
        * blocks_price_events["price"]
    )
    blocks_price_events["LVR"] = -(10000 - fee) / 10000 * (
        blocks_price_events["baseAmount"].clip(lower=0.0) * blocks_price_events["price"]
        + blocks_price_events["quoteAmount"].clip(lower=0.0)
    ) - (
        blocks_price_events["baseAmount"].clip(upper=0.0) * blocks_price_events["price"]
        + blocks_price_events["quoteAmount"].clip(upper=0.0)
    )  # LVR, which is equal to trader's PnL without swap fee and gas cost
    blocks_price_events["FEE"] = (
        fee
        / 10000
        * (
            blocks_price_events["baseAmount"].clip(lower=0.0)
            * blocks_price_events["price"]
            + blocks_price_events["quoteAmount"].clip(lower=0.0)
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
            * 120000
            / 10**18
            * blocks_price_events["price"]
        )
    else:
        blocks_price_events["ARB"] = (
            blocks_price_events["LVR"]
            - blocks_price_events["FEE"]
            - blocks_price_events["baseFeePerGas"] * 120000 / 10**18
        )

    ############################################################
    #                     Processing data                      #
    ############################################################
    """
    entire swap record for profit analysis. This contains retail orderflow too.
    """
    swaps = blocks_price_events[blocks_price_events["FEE"] > 0]
    swaps["swapSize"] = swaps["baseAmount"].clip(lower=0.0) * swaps["price"] + swaps[
        "quoteAmount"
    ].clip(lower=0.0)

    """
    arbitrage-only record. This is for error analysis between theory and real.
    """
    arbitrages = blocks_price_events[blocks_price_events["ARB"] > 0]

    """
    predicted numbers are instantaneous RATE of LVR.
    They should be summed up in specific time interval 
    and be compared against realized value within that interval.
    Here we set the interval 1 hour by default.
    """
    df = pd.DataFrame(
        columns=[
            "timestamp",
            "meanVolSquared",
            "meanPoolValue",
            "meanBaseFeePerGas",
            "expectedLVRperPoolValue",  # for error analysis
            "realizedLVRperPoolValue",  # for error analysis
            "expectedARBperPoolValue",  # for error analysis
            "realizedARBperPoolValue",  # for error analysis
            "realizedARBperPoolValueWithGas",  # for error analysis
        ]
    )

    start_time = int(datetime(2023, 10, 1, tzinfo=timezone.utc).timestamp())
    end_time = int(datetime(2023, 12, 1, tzinfo=timezone.utc).timestamp())

    while start_time < end_time:
        BPE_in_interval = blocks_price_events[
            (start_time <= blocks_price_events["timestamp"])
            & (blocks_price_events["timestamp"] < start_time + interval)
        ]
        ARB_in_interval = arbitrages[
            (start_time <= arbitrages["timestamp"])
            & (arbitrages["timestamp"] < start_time + interval)
        ]

        new_row = pd.DataFrame(
            [
                [
                    start_time,
                    BPE_in_interval["volSquared"].mean(),
                    BPE_in_interval["poolValue"].mean(),
                    BPE_in_interval["baseFeePerGas"].mean(),
                    BPE_in_interval["instLVRperPoolValue"].sum(),
                    (ARB_in_interval["LVR"] / ARB_in_interval["poolValue"]).sum(),
                    BPE_in_interval["instARBperPoolValue"].sum(),
                    (
                        (ARB_in_interval["LVR"] - ARB_in_interval["FEE"])
                        / ARB_in_interval["poolValue"]
                    ).sum(),
                    (ARB_in_interval["ARB"] / ARB_in_interval["poolValue"]).sum(),
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
                "realizedARBperPoolValue",
                "realizedARBperPoolValueWithGas",
            ],
        )

        df = pd.concat([df, new_row], ignore_index=True)

        start_time += interval

    percentile_limit = (
        abs(df["realizedLVRperPoolValue"] - df["expectedLVRperPoolValue"])
    ).quantile(
        0.99
    )  # filter the outliers. mostly sandwich attacks.
    filtered_arbitrages = df[
        abs(df["realizedLVRperPoolValue"] - df["expectedLVRperPoolValue"])
        <= percentile_limit
    ]

    """
    save into csv files
    """
    swaps.to_csv(
        f"results/swaps/{network}_{dex}_{base_token}_{quote_token}_{fee}bps.csv",
        index=False,
        header=True,
    )
    filtered_arbitrages.to_csv(
        f"results/arbitrages/{network}_{dex}_{base_token}_{quote_token}_{fee}bps.csv",
        index=False,
        header=True,
    )


if __name__ == "__main__":
    network = "ARBITRUM"
    dex = "UNI_V3"
    base_token = "WETH"
    for quote_token in ["DAI", "USDC", "USDT"]:
        for fee in [5, 30, 100]:
            use_instant_volatility = True
            v3_swaps_and_arbitrages(
                network, dex, base_token, quote_token, fee, use_instant_volatility
            )

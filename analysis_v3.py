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
    Read files, compute the parameters, theoretical predictions, then
    compare them against realized data.
    """
    ############################################################
    #                        read files                        #
    ############################################################

    events_df = pd.read_csv(
        f"data/onchain_events/{network}_{dex}_{base_token}_{quote_token}_{fee}bps_events.csv"
    )
    events_df.rename({"price": "ammPrice"}, inplace=True)
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
        cex_price_df["price"] = cex_price_df["price"].shift(4)

    ############################################################
    #                    compute parameters                    #
    ############################################################

    if use_instant_volatility:  # volatility.
        """
        Instantaneous volatility from 1 minute return.
        For the derivation see https://blog.naver.com/chunjein/100209878606
        """
        cex_price_df["volSquared"] = (
            2
            * (
                (cex_price_df["price"] - cex_price_df["price"].shift(60))
                / cex_price_df["price"].shift(60)
                - np.log(cex_price_df["price"] / cex_price_df["price"].shift(60))
            )  # difference in arithmetic and logarithmic 1 minute return
            * (60 * 60 * 24)
            / (cex_price_df["timestamp"] - cex_price_df["timestamp"].shift(60))
        )  # converted to daily timeframe.
    else:
        """
        Rolling volatility from 1 minute return.
        We use 10 minutes rolling window. Can be freely modified.
        """
        cex_price_df["volSquared"] = (
            np.log(
                cex_price_df["price"] / cex_price_df["price"].shift(60)
            )  # (approx.) 1 minute return
            .rolling(window=60 * 10)  # samples within 10 minutes
            .std()
            ** 2
            * 60
            * 24
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

    blocks_price["preLVRperVP"] = blocks_price["volSquared"] / 8  # from MMRZ23
    blocks_price["preARBperVP"] = (
        blocks_price["volSquared"]
        * blocks_price["tradeProbability"]
        * (
            (np.exp(gamma / 2) + np.exp(-gamma / 2))
            / (2 * (1 - blocks_price["volSquared"] / (8 * blocks_price["lambda"])))
        )
        / 8
    )  # from MMR23
    blocks_price["preFEEperVP"] = (
        blocks_price["preLVRperVP"] - blocks_price["preARBperVP"]
    )  # from MMRZ22

    ############################################################
    #                     Historical Data                      #
    ############################################################
    blocks_price_events = pd.merge(
        blocks_price, events_df, on="blockNumber", how="left"
    )
    blocks_price_events.fillna(0, inplace=True)

    blocks_price_events["poolValue"] = (
        2 * np.sqrt(blocks_price_events["ammPrice"]) * blocks_price_events["liquidity"]
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

    arbitrages = blocks_price_events[blocks_price_events["ARB"] > 0]


if __name__ == "__main__":
    network = "MAINNET"
    dex = "UNI_V3"
    base_token = "WETH"
    quote_token = "USDC"
    fee = 30  # in bps
    use_instant_volatility = False
    analyze_v3_data(network, dex, base_token, quote_token, fee, use_instant_volatility)

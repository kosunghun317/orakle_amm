import v2_data_processor
import v3_data_processor
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


def v2_and_v3_pnl():
    """
    compare pnl of v2 and v3.
    plot pnl graph for the fixed pair.
    then show the result on many pairs.
    """
    pass


def mainnet_and_arbitrum_pnl():
    """
    compare pnl of mainnet and arbitrum.
    plot pnl graph for the fixed pair.
    then show the result on many pairs.
    """
    pass


def v3_fee_and_pnl():
    """
    show the effect of fee on pnl.
    plot pnl graph for the fixed pair.
    then show the result on many pairs.
    """
    pass


def v3_pnl_and_vol(
    network,
    base_token,
    quote_token,
    fee,
    use_instant_volatility,
    interval,
    window,
):
    """
    show the effect of high volatility on pnl.
    plot pnl graph for the fixed pair.
    then show the result on many pairs.
    """
    (v3_swaps, v3_arbs) = v3_data_processor.v3_swaps_and_arbitrages(
        network,
        "UNI_V3",
        base_token,
        quote_token,
        fee,
        use_instant_volatility,
        interval,
        window,
    )
    v3_swaps_lowVol = v3_swaps[
        v3_swaps["volSquared"] < v3_swaps["volSquared"].quantile(0.25)
    ]
    v3_swaps_highVol = v3_swaps[
        v3_swaps["volSquared"] >= v3_swaps["volSquared"].quantile(0.75)
    ]
    # Create the plot
    plt.figure(figsize=(10, 6))

    # plot the lines
    plt.plot(
        v3_swaps["timestamp"],
        ((v3_swaps["FEE"] - v3_swaps["LVR"]) / v3_swaps["poolValue"]).cumsum() * 100,
        label="v3 all",
    )
    plt.plot(
        v3_swaps_lowVol["timestamp"],
        (
            (v3_swaps_lowVol["FEE"] - v3_swaps_lowVol["LVR"])
            / v3_swaps_lowVol["poolValue"]
        ).cumsum()
        * 100,
        label="v3 low vol",
    )
    plt.plot(
        v3_swaps_highVol["timestamp"],
        (
            (v3_swaps_highVol["FEE"] - v3_swaps_highVol["LVR"])
            / v3_swaps_highVol["poolValue"]
        ).cumsum()
        * 100,
        label="v3 high vol",
    )

    # Label the axes
    plt.xlabel("timestamp")
    plt.ylabel("per unit pool value (in %)")
    plt.legend()
    plt.title("V3 Fee - LVR")

    # Save & Show the plot
    plt.savefig(
        f"results/vol_and_pnl_{network}_v3_{base_token}_{quote_token}_{fee}.png",
        dpi=300,
    )
    plt.show()


if __name__ == "__main__":
    v3_pnl_and_vol(
        "MAINNET",
        "WETH",
        "USDC",
        30,
        False,
        1800,
        24,
    )

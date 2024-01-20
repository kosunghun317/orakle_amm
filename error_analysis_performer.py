"""
perform error analysis.
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
import matplotlib.pyplot as plt
from utils import *
import data_processor

load_dotenv()


def compare_lvr_theory_real(
    network,
    v2_dex,
    use_instant_volatility,
    interval,
    window,
):
    """
    Compare LVR & ARB theory and reality.
    """

    ############################################################
    #                         load data                        #
    ############################################################

    if network == "MAINNET":
        quote_tokens = ["USDC", "USDT", "DAI"]
        v2_xticks = [1, 2, 3]
        v3_xticks = [1, 2, 3, 4, 5, 6, 7, 8, 9]
    else:
        quote_tokens = ["USDC", "USDCe", "USDT", "DAI"]
        v2_xticks = [1, 2, 3, 4]
        v3_xticks = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
    fees = [5, 30, 100]

    v3_arbs_list = []
    v2_arbs_list = []
    xticks = []
    for quote_token in quote_tokens:
        for fee in fees:
            if fee == 30:
                (v2_swaps, v2_arbs) = data_processor.v2_swaps_and_arbitrages(
                    network,
                    v2_dex,
                    "WETH",
                    quote_token,
                    fee,
                    use_instant_volatility,
                    interval,
                    window,
                )
                v2_arbs_list.append(v2_arbs)

            (v3_swaps, v3_arbs) = data_processor.v3_swaps_and_arbitrages(
                network,
                "UNI_V3",
                "WETH",
                quote_token,
                fee,
                use_instant_volatility,
                interval,
                window,
            )
            v3_arbs_list.append(v3_arbs)
            xticks.append(f"{quote_token}-{fee}")

    ############################################################
    #                         plot data                        #
    ############################################################

    # LVR error boxplot: V2
    plt.figure(figsize=(len(quote_tokens), 10))
    plt.scatter(
        v2_xticks,
        [
            (
                v2_arbs["realizedLVRperPoolValue"] / v2_arbs["expectedLVRperPoolValue"]
                - 1
            ).mean()
            * 100
            for v2_arbs in v2_arbs_list
        ],
        label="mean",
        marker="x",
    )
    plt.boxplot(
        [
            (
                v2_arbs["realizedLVRperPoolValue"] / v2_arbs["expectedLVRperPoolValue"]
                - 1
            )
            * 100
            for v2_arbs in v2_arbs_list
        ],
        showfliers=False,
    )
    plt.xticks(v2_xticks, quote_tokens)
    plt.axhline(y=0, color="gray", linestyle="--")
    plt.title("expected vs. realized LVR on V2 pairs")
    plt.ylabel("%")
    plt.legend()
    plt.savefig(
        f"results/lvr_err_{network}_{v2_dex}_WETH_{fee}bps_boxplot.png",
    )

    # LVR error boxplot: V3
    plt.figure(figsize=(len(v3_xticks), 10))
    plt.scatter(
        v3_xticks,
        [
            (
                v3_arbs["realizedLVRperPoolValue"] / v3_arbs["expectedLVRperPoolValue"]
                - 1
            ).mean()
            * 100
            for v3_arbs in v3_arbs_list
        ],
        label="mean",
        marker="x",
    )
    plt.boxplot(
        [
            (
                v3_arbs["realizedLVRperPoolValue"] / v3_arbs["expectedLVRperPoolValue"]
                - 1
            )
            * 100
            for v3_arbs in v3_arbs_list
        ],
        showfliers=False,
    )
    plt.xticks(v3_xticks, xticks, rotation=45)
    plt.axhline(y=0, color="gray", linestyle="--")

    plt.title("expected vs. realized LVR on V3 pools")
    plt.ylabel("%")
    plt.legend()
    plt.savefig(
        f"results/lvr_err_{network}_UNI_V3_WETH_{fee}bps_boxplot.png",
    )

    # ARB error boxplot: V2
    plt.figure(figsize=(len(quote_tokens), 10))
    plt.scatter(
        v2_xticks,
        [
            (
                v2_arbs["realizedARBperPoolValueWithoutGas"]
                / v2_arbs["expectedARBperPoolValue"]
                - 1
            ).mean()
            * 100
            for v2_arbs in v2_arbs_list
        ],
        label="mean",
        marker="x",
    )
    plt.boxplot(
        [
            (
                v2_arbs["realizedARBperPoolValueWithoutGas"]
                / v2_arbs["expectedARBperPoolValue"]
                - 1
            )
            * 100
            for v2_arbs in v2_arbs_list
        ],
        showfliers=False,
    )
    plt.xticks(v2_xticks, quote_tokens)
    plt.axhline(y=0, color="gray", linestyle="--")
    plt.title("expected vs. realized ARB on V2 pairs")
    plt.ylabel("%")
    plt.legend()
    plt.savefig(
        f"results/arb_err_{network}_{v2_dex}_WETH_{fee}bps_boxplot.png",
    )

    # ARB error boxplot: V3
    plt.figure(figsize=(len(v3_xticks), 10))
    plt.scatter(
        v3_xticks,
        [
            (
                v3_arbs["realizedARBperPoolValueWithoutGas"]
                / v3_arbs["expectedARBperPoolValue"]
                - 1
            ).mean()
            * 100
            for v3_arbs in v3_arbs_list
        ],
        label="mean",
        marker="x",
    )
    plt.boxplot(
        [
            (
                v3_arbs["realizedARBperPoolValueWithoutGas"]
                / v3_arbs["expectedARBperPoolValue"]
                - 1
            )
            * 100
            for v3_arbs in v3_arbs_list
        ],
        showfliers=False,
    )
    plt.xticks(v3_xticks, xticks, rotation=45)
    plt.axhline(y=0, color="gray", linestyle="--")

    plt.title("expected vs. realized ARB on V3 pools")
    plt.ylabel("%")
    plt.legend()
    plt.savefig(
        f"results/arb_err_{network}_UNI_V3_WETH_{fee}bps_boxplot.png",
    )


def compare_volatility():
    """
    This will be added in appendix.
    show that volatility is consistent across the various combinations of interval and window.
    """
    pass


if __name__ == "__main__":
    compare_lvr_theory_real(
        "MAINNET",
        "UNI_V2",
        False,
        1800,
        24,
    )
    compare_lvr_theory_real(
        "ARBITRUM",
        "SUSHI",
        False,
        1800,
        24,
    )

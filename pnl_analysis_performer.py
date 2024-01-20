"""
perform the analysis related to pnl.
"""
import data_processor
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


def v2_and_v3_pnl(
    network,
    v2_dex,
    base_token,
    quote_token,
    fee,
    use_instant_volatility,
    interval,
    window,
):
    ############################################################
    #                         load data                        #
    ############################################################

    if network == "MAINNET":
        quote_tokens = ["USDC", "USDT", "DAI", "WBTC"]
    else:
        quote_tokens = ["USDC", "USDCe", "USDT", "DAI", "WBTC"]

    v2_swaps_list = []
    v3_swaps_list = []
    v2_arbs_list = []
    v3_arbs_list = []
    for quote_token in quote_tokens:
        (v2_swaps, v2_arbs) = data_processor.v2_swaps_and_arbitrages(
            network,
            v2_dex,
            base_token,
            quote_token,
            fee,
            use_instant_volatility,
            interval,
            window,
        )
        (v3_swaps, v3_arbs) = data_processor.v3_swaps_and_arbitrages(
            network,
            "UNI_V3",
            base_token,
            quote_token,
            fee,
            use_instant_volatility,
            interval,
            window,
        )
        v2_swaps_list.append(v2_swaps)
        v3_swaps_list.append(v3_swaps)
        v2_arbs_list.append(v2_arbs)
        v3_arbs_list.append(v3_arbs)

    ############################################################
    #           plot the cumulative fee - lvr graph            #
    ############################################################

    # Create the plot
    plt.figure(figsize=(10, 6))

    # plot the lines
    plt.plot(
        v2_swaps_list[0]["timestamp"],
        (
            (v2_swaps_list[0]["FEE"] - v2_swaps_list[0]["LVR"])
            / v2_swaps_list[0]["poolValue"]
        ).cumsum()
        * 100,
        label=f"V2 WETH-{quote_tokens[0]} {fee}bps",
    )

    # Label the axes
    plt.xlabel("timestamp")
    plt.ylabel("per unit pool value (%)")
    plt.legend()
    plt.title(f"{network} V2: (FEE - LVR) per unit pool value from entire orderflow")

    # Save & Show the plot
    plt.savefig(
        f"results/{network}_pnl_{v2_dex}_WETH_{quote_tokens[0]}_{fee}bps.png", dpi=300
    )

    # Create the plot
    plt.figure(figsize=(10, 6))

    # plot the lines
    plt.plot(
        v3_swaps_list[0]["timestamp"],
        (
            (v3_swaps_list[0]["FEE"] - v3_swaps_list[0]["LVR"])
            / v3_swaps_list[0]["poolValue"]
        ).cumsum()
        * 100,
        label=f"V3 WETH-{quote_tokens[0]} {fee}bps",
    )

    # Label the axes
    plt.xlabel("timestamp")
    plt.ylabel("per unit pool value (%)")
    plt.legend()
    plt.title(f"{network} V3: (FEE - LVR) per unit pool value from entire orderflow")

    # Save & Show the plot
    plt.savefig(
        f"results/{network}_pnl_UNI_V3_WETH_{quote_tokens[0]}_{fee}bps.png", dpi=300
    )

    ############################################################
    #             plot boxplots across many pools              #
    ############################################################

    v2_pnls = [
        ((df["FEE"] - df["LVR"]) / df["poolValue"]) * 100 for df in v2_swaps_list
    ]
    v3_pnls = [
        ((df["FEE"] - df["LVR"]) / df["poolValue"]) * 100 for df in v3_swaps_list
    ]
    xticks = [1, 2, 3, 4] if network == "MAINNET" else [1, 2, 3, 4, 5]

    # Create the plot
    plt.figure(figsize=(10, 6))

    plt.scatter(xticks, [v2_pnl.mean() for v2_pnl in v2_pnls], zorder=3, label="mean")
    plt.boxplot(v2_pnls, showfliers=False)
    plt.xticks(xticks, quote_tokens)
    plt.axhline(y=0, color="gray", linestyle="--")
    plt.title("(Fee - LVR) across various V2 pairs")
    plt.ylabel("per unit pool value (%)")
    plt.legend()
    plt.savefig(
        f"results/{network}_pnl_{v2_dex}_WETH_{fee}bps_boxplot.png",
    )

    # Create the plot
    plt.figure(figsize=(10, 6))

    plt.scatter(xticks, [v3_pnl.mean() for v3_pnl in v3_pnls], zorder=3, label="mean")
    plt.boxplot(v3_pnls, showfliers=False)
    plt.xticks(xticks, quote_tokens)
    plt.axhline(y=0, color="gray", linestyle="--")
    plt.title("(Fee - LVR) across various V3 pools")
    plt.ylabel("per unit pool value (%)")
    plt.legend()
    plt.savefig(
        f"results/{network}_pnl_UNI_V3_WETH_{fee}bps_boxplot.png",
    )


def v3_fee_and_pnl(
    network,
    use_instant_volatility,
    interval,
    window,
):
    """
    show the effect of fee on pnl.
    plot pnl graph for the fixed pair.
    then show the result on many pairs.
    """
    ############################################################
    #                         load data                        #
    ############################################################

    if network == "MAINNET":
        quote_tokens = ["USDC", "USDT", "DAI", "WBTC"]
    else:
        quote_tokens = ["USDC", "USDCe", "USDT", "DAI", "WBTC"]

    fees = [5, 30, 100]

    v3_swaps_list = []
    v3_arbs_list = []
    for quote_token in quote_tokens:
        for fee in fees:
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
            v3_swaps_list.append(v3_swaps)
            v3_arbs_list.append(v3_arbs)

    ############################################################
    #           plot the cumulative fee - lvr graph            #
    ############################################################

    # Create the plot
    plt.figure(figsize=(10, 6))

    # plot the lines
    for i, fee in enumerate(fees):
        plt.plot(
            quote_tokens,
            [
                (
                    (
                        v3_swaps_list[j * len(fees) + i]["FEE"]
                        - v3_swaps_list[j * len(fees) + i]["LVR"]
                    )
                    / v3_swaps_list[j * len(fees) + i]["poolValue"]
                ).sum()
                * 100
                for j, quote_token in enumerate(quote_tokens)
            ],
            label=f"{fee}bps",
        )

    # Label the axes
    plt.ylabel("per unit pool value (%)")
    plt.legend()
    plt.xticks(rotation=45)
    plt.title(
        f"{network} V3: cumulative (FEE - LVR) per unit pool value for various fees"
    )

    # Save & Show the plot
    plt.savefig(f"results/v3_fee_and_pnl_{network}_WETH_all.png", dpi=300)


def v3_pnl_and_vol(
    network,
    use_instant_volatility,
    interval,
    window,
):
    """
    show the effect of high volatility on pnl.
    plot pnl graph for the fixed pair.
    then show the result on many pairs.
    """
    ############################################################
    #                         load data                        #
    ############################################################

    if network == "MAINNET":
        quote_tokens = ["USDC", "USDT", "DAI", "WBTC"]
    else:
        quote_tokens = ["USDC", "USDCe", "USDT", "DAI", "WBTC"]

    fees = [5, 30, 100]

    v3_swaps_list = []
    v3_arbs_list = []
    for quote_token in quote_tokens:
        for fee in fees:
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
            v3_swaps_list.append(v3_swaps)
            v3_arbs_list.append(v3_arbs)

    ############################################################
    #           plot the cumulative fee - lvr graph            #
    ############################################################

    v3_swaps_lowVol = v3_swaps_list[0][
        v3_swaps_list[0]["volSquared"] < v3_swaps_list[0]["volSquared"].quantile(0.25)
    ]
    v3_swaps_highVol = v3_swaps_list[0][
        v3_swaps_list[0]["volSquared"] >= v3_swaps_list[0]["volSquared"].quantile(0.75)
    ]

    # Create the plot
    plt.figure(figsize=(10, 6))

    # plot the lines
    plt.plot(
        v3_swaps_list[0]["timestamp"],
        (
            (v3_swaps_list[0]["FEE"] - v3_swaps_list[0]["LVR"])
            / v3_swaps_list[0]["poolValue"]
        ).cumsum()
        * 100,
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
    plt.ylabel("per unit pool value (%)")
    plt.legend()
    plt.title("all vs. low vol vs. high vol")

    # Save & Show the plot
    plt.savefig(
        f"results/vol_and_pnl_{network}_v3_WETH_{quote_token}_{fee}.png",
        dpi=300,
    )

    ############################################################
    #      all vs. low vol vs. high vol across many pools      #
    ############################################################
    i = 0
    xticks = []
    v3_swaps_lowVols = []
    v3_swaps_highVols = []
    for quote_token in quote_tokens:
        for fee in fees:
            v3_swaps_lowVols.append(
                v3_swaps_list[i][
                    v3_swaps_list[i]["volSquared"]
                    < v3_swaps_list[i]["volSquared"].quantile(0.25)
                ]
            )
            v3_swaps_highVols.append(
                v3_swaps_list[i][
                    v3_swaps_list[i]["volSquared"]
                    >= v3_swaps_list[i]["volSquared"].quantile(0.75)
                ]
            )
            xticks.append(f"{quote_token} {fee}bps")
            i += 1

    # Create the plot
    plt.figure(figsize=(i, 6))

    # plt.scatter(
    #     xticks, [
    #         ((df["FEE"] - df["LVR"]) / df["poolValue"]).sum() * 100 for df in v3_swaps_list
    #     ], label="all"
    # )
    plt.scatter(
        xticks,
        [
            ((df["FEE"] - df["LVR"]) / df["poolValue"]).sum() * 100
            for df in v3_swaps_highVols
        ],
        label="high vol",
    )
    plt.scatter(
        xticks,
        [
            ((df["FEE"] - df["LVR"]) / df["poolValue"]).sum() * 100
            for df in v3_swaps_lowVols
        ],
        label="low vol",
    )

    # Adding titles, labels, and legend
    plt.title("low vol vs. high vol")
    plt.ylabel("per unit pool value (%)")
    plt.legend()
    plt.xticks(rotation=45)

    # Displaying the plot
    plt.savefig(
        f"results/vol_and_pnl_{network}_v3_WETH_all.png",
    )


if __name__ == "__main__":
    # mainnet
    # v2_and_v3_pnl(
    #     "MAINNET",
    #     "UNI_V2",
    #     "WETH",
    #     "USDC",
    #     30,
    #     False,
    #     1800,
    #     24,
    # )
    # print("v2 and v3 comparison is done for mainnet")
    # v3_fee_and_pnl(
    #     "MAINNET",
    #     False,
    #     1800,
    #     24,
    # )
    # print("v3 comparison across fees is done for mainnet")
    # v3_pnl_and_vol(
    #     "MAINNET",
    #     False,
    #     1800,
    #     24,
    # )
    # print("v3 comparison across volatility is done for mainnet")

    # arbitrum
    v2_and_v3_pnl(
        "ARBITRUM",
        "SUSHI",
        "WETH",
        "USDC",
        30,
        False,
        1800,
        24,
    )
    print("v2 and v3 comparison is done for arbitrum")
    v3_fee_and_pnl(
        "ARBITRUM",
        False,
        1800,
        24,
    )
    print("v3 comparison across fees is done for arbitrum")
    v3_pnl_and_vol(
        "ARBITRUM",
        False,
        1800,
        24,
    )
    print("v3 comparison across volatility is done for arbitrum")

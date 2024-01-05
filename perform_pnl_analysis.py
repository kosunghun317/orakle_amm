"""
pools with too small TVL are excluded.
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import pearsonr
import statsmodels.api as sm

'''
def perform_profit_analysis(swaps):
    """
    fee - lvr

    mu, sigma, median, correlation coefficient
    (fee - lvr), ordersize correlation

    save the stats and figures.
    """
    pass


def perform_error_analysis():
    """
    error := real - theory.

    mu, sigma, median, correlation coefficient

    topic: LVR, P_trade (~= ARB / LVR)

    save the stats and figures.
    """
    quantile_ranges = np.arange(0, 1.1, 0.1)
    quantile_labels = [
        "10%",
        "20%",
        "30%",
        "40%",
        "50%",
        "60%",
        "70%",
        "80%",
        "90%",
        "100%",
    ]

    arbitrages["poolValueQuantile"] = pd.qcut(
        arbitrages["meanPoolValue"], quantile_ranges, labels=quantile_labels
    )
    arbitrages["gasQuantile"] = pd.qcut(
        arbitrages["meanBaseFeePerGas"], quantile_ranges, labels=quantile_labels
    )

    # fix pool value quantile

    # fix gas quantile


if __name__ == "__main__":
    # read csv files
    v3_swaps = []

    for quote_token in ['DAI', 'USDT', 'USDC']:
        for fee in [5, 30, 100]:
            v3_swaps.append(
                pd.read_csv(
                    f"results/swaps/MAINNET_UNI_V3_WETH_{quote_token}_{fee}bps.csv"
                )
            )
    ###################################
    v2_swaps = []

    for quote_token in ['DAI', 'USDT', 'USDC']:
        for dex in ['UNI_V2', 'SUSHI']:
            v2_swaps.append(
                pd.read_csv(
                    f"results/swaps/MAINNET_{dex}_WETH_{quote_token}_30bps.csv"
                )
            )
    ###################################
'''
sns.set_style("dark")

############################################################
#                    Mainnet V3 entire                     #
############################################################

# Create the plot
plt.figure(figsize=(10, 6))

# plot the lines
for quote_token in ["DAI", "USDT", "USDC"]:
    for fee in [30, 100]:
        df = pd.read_csv(
            f"results/swaps/MAINNET_UNI_V3_WETH_{quote_token}_{fee}bps.csv"
        )
        plt.plot(
            df["timestamp"],
            ((df["FEE"] - df["LVR"]) / df["poolValue"]).cumsum(),
            label=f"WETH-{quote_token} {fee}bps",
        )

# Label the axes
plt.xlabel("timestamp")
plt.ylabel("per unit pool value")
plt.legend()
plt.title("Mainnet V3: (FEE - LVR) per unit pool value from entire orderflow")

# Save & Show the plot
plt.savefig("results/MAINNET_UNI_V3_ENTIRE_pnl.png", dpi=300)
plt.show()

############################################################
#                Mainnet V3 arbitrage only                 #
############################################################

# Create the plot
plt.figure(figsize=(10, 6))

# plot the lines
for quote_token in ["DAI", "USDT", "USDC"]:
    for fee in [30, 100]:
        df = pd.read_csv(
            f"results/arbitrages/MAINNET_UNI_V3_WETH_{quote_token}_{fee}bps.csv"
        )
        plt.plot(
            df["timestamp"],
            df["realizedARBperPoolValue"].cumsum(),
            label=f"WETH-{quote_token} {fee}bps w/o gas fee",
        )


# Label the axes
plt.xlabel("timestamp")
plt.ylabel("per unit pool value")
plt.legend()
plt.title("Mainnet V3: cumulative arbitrageurs' profit per unit pool value")

# Save & Show the plot
plt.savefig("results/MAINNET_UNI_V3_Arb_pnl.png", dpi=300)
plt.show()

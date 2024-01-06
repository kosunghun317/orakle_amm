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
#                       Mainnet V3                         #
############################################################

# Create the plot
plt.figure(figsize=(10, 6))

# plot the lines
for fee in [5, 30, 100]:
    for quote_token in ["DAI", "USDT", "USDC"]:
        df = pd.read_csv(
            f"results/arbitrages/MAINNET_UNI_V3_WETH_{quote_token}_{fee}bps.csv"
        )
        plt.plot(
            df["timestamp"],
            df["expectedLVRperPoolValue"].cumsum(),
            label=f"WETH-{quote_token} {fee}bps expected LVR",
        )

# Label the axes
plt.xlabel("timestamp")
plt.ylabel("per unit pool value")
plt.legend()
plt.title("Mainnet V3: theory vs. reality")

# Save & Show the plot
plt.savefig("results/MAINNET_UNI_V3_LVR_expected.png", dpi=300)
plt.show()

# Create the plot
plt.figure(figsize=(10, 6))

# plot the lines
for fee in [5, 30, 100]:
    for quote_token in ["DAI", "USDT", "USDC"]:
        df = pd.read_csv(
            f"results/arbitrages/MAINNET_UNI_V3_WETH_{quote_token}_{fee}bps.csv"
        )
        plt.plot(
            df["timestamp"],
            df["expectedLVRperPoolValue"].cumsum(),
            label=f"WETH-{quote_token} {fee}bps expected LVR",
            color='grey'
        )

# plot the lines
for fee in [5, 30, 100]:
    for quote_token in ["DAI", "USDT", "USDC"]:
        df = pd.read_csv(
            f"results/arbitrages/MAINNET_UNI_V3_WETH_{quote_token}_{fee}bps.csv"
        )
        plt.plot(
            df["timestamp"],
            df["realizedLVRperPoolValue"].cumsum(),
            label=f"WETH-{quote_token} {fee}bps realized LVR",
            color='blue'
        )

# Label the axes
plt.xlabel("timestamp")
plt.ylabel("per unit pool value")
plt.legend()
plt.title("Mainnet V3: theory vs. reality")

# Save & Show the plot
plt.savefig("results/MAINNET_UNI_V3_LVR_both.png", dpi=300)
plt.show()

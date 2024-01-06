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
for fee in [5, 30, 100]:
    for quote_token in ["DAI", "USDT", "USDC"]:
        df = pd.read_csv(
            f"results/swaps/MAINNET_UNI_V3_WETH_{quote_token}_{fee}bps.csv"
        )
        percentile_99th = abs(df["FEE"] - df["LVR"]).quantile(0.99)
        df_filtered = df[abs(df["FEE"] - df["LVR"]) < percentile_99th]
        plt.plot(
            df_filtered["timestamp"],
            (
                (df_filtered["FEE"] - df_filtered["LVR"]) / df_filtered["poolValue"]
            ).cumsum(),
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
for fee in [5, 30, 100]:
    for quote_token in ["DAI", "USDT", "USDC"]:
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

############################################################
#                    Mainnet V2 entire                     #
############################################################

# Create the plot
plt.figure(figsize=(10, 6))

# plot the lines
for quote_token in ["DAI", "USDT", "USDC"]:
    for dex in ["UNI_V2", "SUSHI"]:
        df = pd.read_csv(f"results/swaps/MAINNET_{dex}_WETH_{quote_token}_30bps.csv")
        percentile_99th = abs(df["FEE"] - df["LVR"]).quantile(0.99)
        df_filtered = df[abs(df["FEE"] - df["LVR"]) < percentile_99th]
        plt.plot(
            df_filtered["timestamp"],
            (
                (df_filtered["FEE"] - df_filtered["LVR"]) / df_filtered["poolValue"]
            ).cumsum(),
            label=f"{dex} WETH-{quote_token} 30bps",
        )

# Label the axes
plt.xlabel("timestamp")
plt.ylabel("per unit pool value")
plt.legend()
plt.title("Mainnet V2: (FEE - LVR) per unit pool value from entire orderflow")

# Save & Show the plot
plt.savefig("results/MAINNET_UNI_V2_ENTIRE_pnl.png", dpi=300)
plt.show()

############################################################
#                Mainnet V2 arbitrage only                 #
############################################################

# Create the plot
plt.figure(figsize=(10, 6))

# plot the lines
for quote_token in ["DAI", "USDT", "USDC"]:
    for dex in ["UNI_V2", "SUSHI"]:
        df = pd.read_csv(
            f"results/arbitrages/MAINNET_{dex}_WETH_{quote_token}_30bps.csv"
        )
        plt.plot(
            df["timestamp"],
            df["realizedARBperPoolValue"].cumsum(),
            label=f"{dex} WETH-{quote_token} 30bps w/o gas fee",
        )


# Label the axes
plt.xlabel("timestamp")
plt.ylabel("per unit pool value")
plt.legend()
plt.title("Mainnet V2: cumulative arbitrageurs' profit per unit pool value")

# Save & Show the plot
plt.savefig("results/MAINNET_UNI_V2_Arb_pnl.png", dpi=300)
plt.show()

############################################################
#                   ARBITRUM V3 entire                     #
############################################################

# Create the plot
plt.figure(figsize=(10, 6))

# plot the lines
for fee in [5, 30, 100]:
    for quote_token in ["DAI", "USDT", "USDC", "USDCe"]:
        df = pd.read_csv(
            f"results/swaps/ARBITRUM_UNI_V3_WETH_{quote_token}_{fee}bps.csv"
        )
        percentile_99th = abs(df["FEE"] - df["LVR"]).quantile(0.99)
        df_filtered = df[abs(df["FEE"] - df["LVR"]) < percentile_99th]
        plt.plot(
            df_filtered["timestamp"],
            (
                (df_filtered["FEE"] - df_filtered["LVR"]) / df_filtered["poolValue"]
            ).cumsum(),
            label=f"WETH-{quote_token} {fee}bps",
        )

# Label the axes
plt.xlabel("timestamp")
plt.ylabel("per unit pool value")
plt.legend()
plt.title("ARBITRUM V3: (FEE - LVR) per unit pool value from entire orderflow")

# Save & Show the plot
plt.savefig("results/ARBITRUM_UNI_V3_ENTIRE_pnl.png", dpi=300)
plt.show()

############################################################
#               ARBITRUM V3 arbitrage only                 #
############################################################

# Create the plot
plt.figure(figsize=(10, 6))

# plot the lines
for fee in [5, 30, 100]:
    for quote_token in ["DAI", "USDT", "USDC", "USDCe"]:
        df = pd.read_csv(
            f"results/arbitrages/ARBITRUM_UNI_V3_WETH_{quote_token}_{fee}bps.csv"
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
plt.title("ARBITRUM V3: cumulative arbitrageurs' profit per unit pool value")

# Save & Show the plot
plt.savefig("results/ARBITRUM_UNI_V3_Arb_pnl.png", dpi=300)
plt.show()

############################################################
#                   ARBITRUM V2 entire                     #
############################################################

# Create the plot
plt.figure(figsize=(10, 6))

# plot the lines
for dex, quote_token in [
    ("SUSHI", "USDC"),
    ("SUSHI", "USDT"),
    ("SUSHI", "DAI"),
    ("SUSHI", "USDCe"),
    ("CAMELOT", "USDCe"),
]:
    df = pd.read_csv(f"results/swaps/ARBITRUM_{dex}_WETH_{quote_token}_30bps.csv")
    percentile_99th = abs(df["FEE"] - df["LVR"]).quantile(0.99)
    df_filtered = df[abs(df["FEE"] - df["LVR"]) < percentile_99th]
    plt.plot(
        df_filtered["timestamp"],
        ((df_filtered["FEE"] - df_filtered["LVR"]) / df_filtered["poolValue"]).cumsum(),
        label=f"{dex} WETH-{quote_token} 30bps",
    )

# Label the axes
plt.xlabel("timestamp")
plt.ylabel("per unit pool value")
plt.legend()
plt.title("ARBITRUM V2: (FEE - LVR) per unit pool value from entire orderflow")

# Save & Show the plot
plt.savefig("results/ARBITRUM_UNI_V2_ENTIRE_pnl.png", dpi=300)
plt.show()

############################################################
#               ARBITRUM V2 arbitrage only                 #
############################################################

# Create the plot
plt.figure(figsize=(10, 6))

# plot the lines
for dex, quote_token in [
    ("SUSHI", "USDC"),
    ("SUSHI", "USDT"),
    ("SUSHI", "DAI"),
    ("SUSHI", "USDCe"),
    ("CAMELOT", "USDCe"),
]:
    df = pd.read_csv(f"results/arbitrages/ARBITRUM_{dex}_WETH_{quote_token}_30bps.csv")
    plt.plot(
        df["timestamp"],
        df["realizedARBperPoolValue"].cumsum(),
        label=f"{dex} WETH-{quote_token} 30bps w/o gas fee",
    )


# Label the axes
plt.xlabel("timestamp")
plt.ylabel("per unit pool value")
plt.legend()
plt.title("ARBITRUM V2: cumulative arbitrageurs' profit per unit pool value")

# Save & Show the plot
plt.savefig("results/ARBITRUM_UNI_V2_Arb_pnl.png", dpi=300)
plt.show()

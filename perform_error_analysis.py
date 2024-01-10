"""
pools with too small TVL are excluded.
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import pearsonr
import statsmodels.api as sm

sns.set_style("dark")

############################################################
#                       Mainnet V3                         #
############################################################

# Create the plot
plt.figure(figsize=(10, 6))

# plot the lines
for dex, quote_token in [
    ("CAMELOT", "USDCe"),
    ("SUSHI", "USDC"),
    ("SUSHI", "USDT"),
    ("SUSHI", "DAI"),
    ("SUSHI", "USDCe"),
]:
    df = pd.read_csv(f"results/arbitrages/ARBITRUM_{dex}_WETH_{quote_token}_30bps.csv")
    plt.plot(
        df["timestamp"],
        df["expectedLVRperPoolValue"].cumsum(),
        label=f"{dex} WETH-{quote_token} 30bps expected LVR",
    )

# plot the lines
for dex, quote_token in [
    ("CAMELOT", "USDCe"),
    ("SUSHI", "USDC"),
    ("SUSHI", "USDT"),
    ("SUSHI", "DAI"),
    ("SUSHI", "USDCe"),
]:
    df = pd.read_csv(f"results/arbitrages/ARBITRUM_{dex}_WETH_{quote_token}_30bps.csv")
    plt.plot(df["timestamp"], df["realizedLVRperPoolValue"].cumsum(), alpha=0)

# Label the axes
plt.xlabel("timestamp")
plt.ylabel("per unit pool value")
plt.legend()
plt.title("ARBITRUM V2: theory vs. reality")

# Save & Show the plot
plt.savefig("results/ARBITRUM_UNI_V2_LVR_expected.png", dpi=300)
plt.show()

# Create the plot
plt.figure(figsize=(10, 6))

# plot the lines
for dex, quote_token in [
    ("CAMELOT", "USDCe"),
    ("SUSHI", "USDC"),
    ("SUSHI", "USDT"),
    ("SUSHI", "DAI"),
    ("SUSHI", "USDCe"),
]:
    df = pd.read_csv(f"results/arbitrages/ARBITRUM_{dex}_WETH_{quote_token}_30bps.csv")
    plt.plot(
        df["timestamp"],
        df["expectedLVRperPoolValue"].cumsum(),
        label=f"{dex} WETH-{quote_token} 30bps expected LVR",
        alpha=0.2,
    )

# plot the lines
for dex, quote_token in [
    ("CAMELOT", "USDCe"),
    ("SUSHI", "USDC"),
    ("SUSHI", "USDT"),
    ("SUSHI", "DAI"),
    ("SUSHI", "USDCe"),
]:
    df = pd.read_csv(f"results/arbitrages/ARBITRUM_{dex}_WETH_{quote_token}_30bps.csv")
    plt.plot(
        df["timestamp"],
        df["realizedLVRperPoolValue"].cumsum(),
        label=f"{dex} WETH-{quote_token} 30bps realized LVR",
    )

# Label the axes
plt.xlabel("timestamp")
plt.ylabel("per unit pool value")
plt.legend()
plt.title("ARBITRUM V2: theory vs. reality")

# Save & Show the plot
plt.savefig("results/ARBITRUM_UNI_V2_LVR_both.png", dpi=300)
plt.show()

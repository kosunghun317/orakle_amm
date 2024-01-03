"""
pools with too small TVL are excluded.
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import pearsonr
import statsmodels.api as sm


def perform_profit_analysis(swaps):
    """
    fee - lvr

    mu, sigma, median, correlation coefficient
    (fee - lvr), ordersize correlation

    save the stats and figures.
    """
    pass


def perform_error_analysis(arbitrages):
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

    # generate clusters

    # perform analysis

    pass

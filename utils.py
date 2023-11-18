
import numpy as np
import pandas as pd
from math import sqrt


def get_liquidity(x, y):
    """Liquidity is defined as the positive sqrt(k), where: x * y = k"""
    return sqrt(x * y)


def get_price_path(sigma_per_day, blocks_per_day, M, num_days, initial_price=1000):
    """# Use geometrical Brownian motion to simulate price evolution."""
    np.random.seed(123)
    mu = 0.0  # assume delta neutral behavior
    T = num_days
    n = T * blocks_per_day  # calc each time step
    dt = T / n

    # simulation using numpy arrays
    St = np.exp(
        (mu - sigma_per_day**2 / 2) * dt
        + sigma_per_day * np.random.normal(0, np.sqrt(dt), size=(M, n - 1))
    )
    # include array of 1's
    # St = np.vstack([np.ones(M), St]) # when M != 1
    St = np.insert(St, 0, 1)

    # multiply through by S0 and return the cumulative product of elements along a given simulation path (axis=0).
    St = initial_price * St.cumprod(axis=0)

    return St


def create_sample_df(sigma_per_day, blocks_per_day, M, num_days, initial_price=1000):
    """currently not in use due to data type"""
    # generate price path
    all_prices = get_price_path(
        sigma_per_day, blocks_per_day, M, num_days, initial_price
    )

    # Generate time axis
    num_block = np.arange(all_prices.shape[0]) * 1

    df = pd.DataFrame(
        {
            "num_block": num_block,  # num_block을 1차원 배열로 변환
            "all_price": all_prices,  # all_prices를 1차원 배열로 변환
        }
    )

    # initialize other cols
    # T(blocktime), Pool Price, Pool Value, x reserve , y reserve, collected fee, cum lvr, arg_gain
    for col in ["all_lvr", "all_cum_lvr", "all_fees", "all_cum_fees", "all_arb_gain"]:
        df[col] = None

    return df


def update_array(data, row):
    return np.vstack((data, row))

def get_block_from_timestamp(w3, target_timestamp):
    """
    Find the earliest block after the given timestamp using binary search.
    Then return the block number and timestamp.
    """
    latest_block = w3.eth.get_block("latest")

    # binary search
    low = 0
    high = latest_block.number
    while low <= high:
        mid = (low + high) // 2
        mid_timestamp = w3.eth.get_block(mid).timestamp

        if mid_timestamp < target_timestamp:
            low = mid + 1
        elif mid_timestamp > target_timestamp:
            high = mid - 1
        else:
            return mid, mid_timestamp  # Exact match
    return (
        low,
        w3.eth.get_block(low).timestamp,
    )  # low_timestamp > target_timestamp > high_timestamp

import os
from dotenv import load_dotenv
from datetime import datetime, timezone
import web3
from decimal import Decimal
import time
import pandas as pd
from utils import *

load_dotenv()


def query_v3_events(
    start_timestamp, end_timestamp, network, dex, base_token, quote_token, fee_rate
):
    # settings
    w3 = web3.Web3(web3.Web3.HTTPProvider(os.getenv(f"{network}_ALCHEMY_URL")))
    from_block, from_timestamp = get_block_from_timestamp(w3, start_timestamp)
    to_block, to_timestamp = get_block_from_timestamp(w3, end_timestamp)
    print(f"Block range: {from_block}:{to_block}")

    # factory
    dex_factory = w3.eth.contract(
        address=os.getenv(f"{network}_{dex}_FACTORY_ADDRESS"),
        abi=os.getenv("UNI_V3_FACTORY_ABI"),
    )

    # pool
    pool_address = dex_factory.functions.getPool(
        os.getenv(f"{network}_{base_token}"),
        os.getenv(f"{network}_{quote_token}"),
        fee_rate,
    ).call()
    pool = w3.eth.contract(address=pool_address, abi=os.getenv("UNI_V3_POOL_ABI"))

    # query the events
    print(f"Querying the events on address {pool.address} ..")
    chunk_size = 1800
    swaps = []
    for block_number in range(from_block, to_block, chunk_size):
        chunk_end = min(block_number + chunk_size, to_block) - 1
        time.sleep(0.1)

        swap_logs = pool.events.Swap().get_logs(
            fromBlock=block_number, toBlock=chunk_end
        )
        swaps.extend(
            [
                {
                    "blockNumber": swap_log.blockNumber,
                    "logIndex": swap_log.logIndex,
                    "amount0": Decimal(swap_log.args.amount0),
                    "amount1": Decimal(swap_log.args.amount1),
                    "sqrtPriceX96": Decimal(swap_log.args.sqrtPriceX96),
                    "liquidity": Decimal(swap_log.args.liquidity),
                }
                for swap_log in swap_logs
            ]
        )

    # create DF from list of dictionary
    print("Constructing DataFrame..")
    df = pd.DataFrame(swaps)

    # sort by blockNumber, then logIndex
    df.sort_values(by=["blockNumber", "logIndex"], inplace=True)

    # replace column names
    is_base_token_token0 = os.getenv(f"{network}_{base_token}") < os.getenv(
        f"{network}_{quote_token}"
    )
    if is_base_token_token0:
        df.rename(
            columns={
                "amount0": "baseAmount",
                "amount1": "quoteAmount",
                "sqrtPriceX96": "price",
            },
            inplace=True,
        )
    else:
        df.rename(
            columns={
                "amount1": "baseAmount",
                "amount0": "quoteAmount",
                "sqrtPriceX96": "price",
            },
            inplace=True,
        )

    # rescale the numbers
    base_decimals = (
        w3.eth.contract(
            address=os.getenv(f"{network}_{base_token}"), abi=os.getenv("ERC20_ABI")
        )
        .functions.decimals()
        .call()
    )
    quote_decimals = (
        w3.eth.contract(
            address=os.getenv(f"{network}_{quote_token}"), abi=os.getenv("ERC20_ABI")
        )
        .functions.decimals()
        .call()
    )

    df["baseAmount"] /= Decimal(10**base_decimals)
    df["quoteAmount"] /= Decimal(10**quote_decimals)
    df["liquidity"] /= Decimal(10 ** int((base_decimals + quote_decimals) / 2))

    if is_base_token_token0:
        df["price"] = (
            (df["price"] / Decimal(2**96)) ** 2
            * Decimal(10**base_decimals)
            / Decimal(10**quote_decimals)
        )
    else:
        df["price"] = (
            (Decimal(2**96) / df["price"]) ** 2
            * Decimal(10**base_decimals)
            / Decimal(10**quote_decimals)
        )

    # save into csv file
    print("Saving into csv file..")
    df.to_csv(
        f"data/onchain_events/{network}_{dex}_{base_token}_{quote_token}_{int(fee_rate/100)}bps_events.csv",
        index=False,
    )


if __name__ == "__main__":
    start_timestamp = int(datetime(2023, 10, 1, tzinfo=timezone.utc).timestamp())
    end_timestamp = int(datetime(2023, 12, 1, tzinfo=timezone.utc).timestamp())
    network = "MAINNET"
    dex = "UNI_V3"
    base_token = "WETH"
    quote_token = "USDC"
    fee_rate = 10000
    print("Start!")
    start_time = time.perf_counter()
    query_v3_events(
        start_timestamp, end_timestamp, network, dex, base_token, quote_token, fee_rate
    )
    print(f"End! It took {int(time.perf_counter() - start_time)} seconds.")

import os
from dotenv import load_dotenv
from datetime import datetime, timezone
import web3
from decimal import Decimal
import time
import pandas as pd
from utils import *

load_dotenv()


def query_v2_events(
    start_timestamp, end_timestamp, network, dex, base_token, quote_token
):
    # settings
    w3 = web3.Web3(web3.Web3.HTTPProvider(os.getenv(f"{network}_ALCHEMY_URL")))
    from_block = get_block_from_timestamp(w3, start_timestamp)[0]
    to_block = get_block_from_timestamp(w3, end_timestamp)[0]
    print(f"Block range: {from_block}:{to_block}")

    # factory
    dex_factory = w3.eth.contract(
        address=os.getenv(f"{network}_{dex}_FACTORY_ADDRESS"),
        abi=os.getenv("UNI_V2_FACTORY_ABI"),
    )

    # pair
    pair_address = dex_factory.functions.getPair(
        os.getenv(f"{network}_{base_token}"), os.getenv(f"{network}_{quote_token}")
    ).call()
    pair = w3.eth.contract(address=pair_address, abi=os.getenv("UNI_V2_PAIR_ABI"))

    # query the events
    print(f"Querying the events on address {pair.address} ..")
    chunk_size = 1800
    swaps = []
    mints_and_burns = []
    syncs = []
    for block_number in range(from_block, to_block, chunk_size):
        time.sleep(0.1)
        chunk_end = min(block_number + chunk_size, to_block) - 1

        swap_logs = pair.events.Swap().get_logs(
            fromBlock=block_number, toBlock=chunk_end
        )
        swaps.extend(
            [
                {
                    "blockNumber": swap_log.blockNumber,
                    "logIndex": swap_log.logIndex,
                    "amount0In": Decimal(swap_log.args.amount0In),
                    "amount1In": Decimal(swap_log.args.amount1In),
                    "amount0Out": Decimal(swap_log.args.amount0Out),
                    "amount1Out": Decimal(swap_log.args.amount1Out),
                }
                for swap_log in swap_logs
            ]
        )

        mint_logs = pair.events.Transfer().get_logs(
            fromBlock=block_number,
            toBlock=chunk_end,
            argument_filters={"from": "0x" + "0" * 40},
        )
        mints_and_burns.extend(
            [
                {
                    "blockNumber": mint_log.blockNumber,
                    "logIndex": mint_log.logIndex,
                    "amount": Decimal(mint_log.args.value),
                }
                for mint_log in mint_logs
            ]
        )

        burn_logs = pair.events.Transfer().get_logs(
            fromBlock=block_number,
            toBlock=chunk_end,
            argument_filters={"to": "0x" + "0" * 40},
        )
        mints_and_burns.extend(
            [
                {
                    "blockNumber": burn_log.blockNumber,
                    "logIndex": burn_log.logIndex,
                    "amount": Decimal(-burn_log.args.value),
                }
                for burn_log in burn_logs
            ]
        )

        sync_logs = pair.events.Sync().get_logs(
            fromBlock=block_number,
            toBlock=chunk_end,
        )
        syncs.extend(
            [
                {
                    "blockNumber": sync_log.blockNumber,
                    "logIndex": sync_log.logIndex,
                    "reserve0": Decimal(sync_log.args.reserve0),
                    "reserve1": Decimal(sync_log.args.reserve1),
                }
                for sync_log in sync_logs
            ]
        )

    mints_and_burns = [
        {
            "blockNumber": from_block,
            "logIndex": 0,
            "amount": Decimal(
                pair.functions.totalSupply().call(block_identifier=from_block - 1)
            ),
        }
    ] + mints_and_burns
    syncs = [
        {
            "blockNumber": from_block,
            "logIndex": 0,
            "reserve0": Decimal(
                pair.functions.getReserves().call(block_identifier=from_block - 1)[0]
            ),
            "reserve1": Decimal(
                pair.functions.getReserves().call(block_identifier=from_block - 1)[1]
            ),
        }
    ] + syncs

    # create DFs from list of dictionary
    print("Constructing DataFrame..")
    df_swaps = pd.DataFrame(swaps)
    df_mints_and_burns = pd.DataFrame(mints_and_burns)
    df_syncs = pd.DataFrame(syncs)

    # join them
    df = pd.merge(
        df_swaps, df_mints_and_burns, on=["blockNumber", "logIndex"], how="outer"
    )
    df = pd.merge(df, df_syncs, on=["blockNumber", "logIndex"], how="outer")

    # sort by blockNumber, then logIndex
    df.sort_values(by=["blockNumber", "logIndex"], inplace=True)

    # replace column names
    print("Replacing the column names..")
    is_base_token_token0 = os.getenv(f"{network}_{base_token}") < os.getenv(
        f"{network}_{quote_token}"
    )
    if is_base_token_token0:
        df.rename(
            columns={
                "amount0In": "baseIn",
                "amount0Out": "baseOut",
                "amount1In": "quoteIn",
                "amount1Out": "quoteOut",
                "amount": "totalSupply",
                "reserve0": "baseReserve",
                "reserve1": "quoteReserve",
            },
            inplace=True,
        )
    else:
        df.rename(
            columns={
                "amount1In": "baseIn",
                "amount1Out": "baseOut",
                "amount0In": "quoteIn",
                "amount0Out": "quoteOut",
                "amount": "totalSupply",
                "reserve1": "baseReserve",
                "reserve0": "quoteReserve",
            },
            inplace=True,
        )

    # forward fill the reserves
    for column_name in ["quoteReserve", "baseReserve"]:
        df[column_name].replace(Decimal(0), pd.NA, inplace=True)
        df[column_name].ffill(inplace=True)
    df.fillna(Decimal(0), inplace=True)

    # rescale the numbers
    print("Rescaling the numbers..")
    df["totalSupply"] = df["totalSupply"].cumsum()
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

    df["baseIn"] /= Decimal(10**base_decimals)
    df["baseOut"] /= Decimal(10**base_decimals)
    df["baseReserve"] /= Decimal(10**base_decimals)
    df["quoteOut"] /= Decimal(10**quote_decimals)
    df["quoteIn"] /= Decimal(10**quote_decimals)
    df["quoteReserve"] /= Decimal(10**quote_decimals)
    df["totalSupply"] /= Decimal(10 ** int((base_decimals + quote_decimals) / 2))

    # filter the rows
    print("Filtering the rows..")
    filtered_df = df[
        (df["quoteIn"] != Decimal(0))
        | (df["baseIn"] != Decimal(0))
        | (df["quoteOut"] != Decimal(0))
        | (df["baseOut"] != Decimal(0))
    ]
    final_df = pd.concat(
        [df.iloc[:1], filtered_df], ignore_index=True
    ).drop_duplicates()

    # save into csv file
    print("Saving the DataFrame into csv file..")
    final_df.to_csv(
        f"data/onchain_events/{network}_{dex}_{base_token}_{quote_token}_events.csv",
        index=False,
    )


if __name__ == "__main__":
    start_timestamp = int(datetime(2023, 10, 1, tzinfo=timezone.utc).timestamp())
    end_timestamp = int(datetime(2023, 12, 1, tzinfo=timezone.utc).timestamp())

    for network, dex, base_token, quote_token in [
        # mainnet pairs
        ("MAINNET", "SUSHI", "WETH", "USDC"),
        ("MAINNET", "SUSHI", "WETH", "USDT"),
        ("MAINNET", "SUSHI", "WETH", "DAI"),
        ("MAINNET", "SUSHI", "WETH", "WBTC"),
        ("MAINNET", "UNI_V2", "WETH", "USDC"),
        ("MAINNET", "UNI_V2", "WETH", "USDT"),
        ("MAINNET", "UNI_V2", "WETH", "DAI"),
        ("MAINNET", "UNI_V2", "WETH", "WBTC"),
        # arbitrum pairs
        ("ARBITRUM", "CAMELOT", "WETH", "USDCe"),
        ("ARBITRUM", "CAMELOT", "WETH", "WBTC"),
        ("ARBITRUM", "SUSHI", "WETH", "USDC"),
        ("ARBITRUM", "SUSHI", "WETH", "USDT"),
        ("ARBITRUM", "SUSHI", "WETH", "DAI"),
        ("ARBITRUM", "SUSHI", "WETH", "USDCe"),
        ("ARBITRUM", "SUSHI", "WETH", "WBTC"),
    ]:
        print("Start!")
        start_time = time.perf_counter()
        query_v2_events(
            start_timestamp, end_timestamp, network, dex, base_token, quote_token
        )
        print(f"End! It took {int(time.perf_counter() - start_time)} seconds.")
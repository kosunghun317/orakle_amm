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
    from_block, from_timestamp = get_block_from_timestamp(w3, start_timestamp)
    to_block, to_timestamp = get_block_from_timestamp(w3, end_timestamp)
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
            "logIndex": 1,
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
    # sync is only need for getting AMM spot price. We remain only the latest reserves within each block.
    df_syncs = pd.DataFrame(syncs)
    idx = df_syncs.groupby("blockNumber")["logIndex"].idxmax()
    df_syncs = df_syncs.loc[idx]

    # join them
    df = pd.merge(
        df_swaps, df_mints_and_burns, on=["blockNumber", "logIndex"], how="outer"
    )
    df = pd.merge(df, df_syncs, on=["blockNumber", "logIndex"], how="outer")

    # sort by blockNumber, then logIndex
    df.sort_values(by=["blockNumber", "logIndex"], inplace=True)

    # fill the zeros
    df.fillna(Decimal(0), inplace=True)

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

    # trim the rows
    print("Trimming the rows..")
    df.reset_index(inplace=True, drop=True)

    for i in range(1, len(df)):
        if df.at[i, "quoteReserve"] == Decimal(0):
            df.at[i, "quoteReserve"] = df.at[i - 1, "quoteReserve"]
        if df.at[i, "baseReserve"] == Decimal(0):
            df.at[i, "baseReserve"] = df.at[i - 1, "baseReserve"]
        if df.at[i, "totalSupply"] == Decimal(0):
            df.at[i, "totalSupply"] = df.at[i - 1, "totalSupply"]

    df = df[
        (df["quoteIn"] != Decimal(0))
        | (df["baseIn"] != Decimal(0))
        | (df["quoteOut"] != Decimal(0))
        | (df["baseOut"] != Decimal(0))
    ]
    df.reset_index(inplace=True, drop=True)

    # save into csv file
    print("Saving into csv file..")
    df.to_csv(
        f"data/onchain_events/{network}_{dex}_{base_token}_{quote_token}_events.csv",
        index=False,
    )


if __name__ == "__main__":
    start_timestamp = int(datetime(2023, 10, 1, tzinfo=timezone.utc).timestamp())
    end_timestamp = int(datetime(2023, 12, 1, tzinfo=timezone.utc).timestamp())
    network = "ARBITRUM"
    dex = "SUSHI"
    base_token = "WETH"
    quote_token = "USDCe"
    print("Start!")
    start_time = time.perf_counter()
    query_v2_events(
        start_timestamp, end_timestamp, network, dex, base_token, quote_token
    )
    print(f"End! It took {int(time.perf_counter() - start_time)} seconds.")

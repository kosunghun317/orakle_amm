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


def token_to_ticker(token):
    if token.endswith("ETH"):
        return "ETH"
    elif token.endswith("BTC"):
        return "BTC"
    else:
        return "USD"

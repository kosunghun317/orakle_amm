# pragma version ^0.3.0

factory: public(address)
base_token: public(address)
quote_token: public(address)

last_block_number: uint256

# TODO: pack the reserves and gammas into two slots (for gas saving)
reference_base_reserve: uint112
reference_quote_reserve: uint112

main_base_reserve: uint112
main_quote_reserve: uint112

buy_gamma: uint32
sell_gamma: uint32

REFRENCE_GAMMA: constant(uint32) = 9999  # 0.01%
MAIN_GAMMA: constant(uint32) = 9990  # 0.1%


@external
def initialize(_base_token: address, _quote_token: address):
    assert self.factory != empty(address)

    self.factory = msg.sender
    self.base_token = _base_token
    self.quote_token = _quote_token


@external
@view
def get_reserves_and_gamma():
    pass


@internal
def _update_gamma():
    pass


@internal
def _allocate_volume():
    pass


@external
@nonreentrant("lock")
def swap():
    pass


@external
@nonreentrant("lock")
def mint():
    pass


@external
@nonreentrant("lock")
def burn():
    pass

import v2_data_processor
import v3_data_processor
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
import web3
from decimal import Decimal
import time
import pandas as pd
import polars as pl
import numpy as np
from utils import *
import matplotlib.pyplot as plt

load_dotenv()


def compare_volatility():
    """
    show that volatility is consistent across the various combinations of interval and window.
    """
    pass


def compare_lvr_theory_real():
    """
    show the expected and realized LVR.
    plot pnl graph for the fixed pair. (cumulative)
    then show the result on many pairs. (boxplot)
    """
    pass


def compare_arb_theory_real():
    """
    show the expected and realized arbitrage pnl.
    plot pnl graph for the fixed pair. (cumulative)
    then show the result on many pairs. (boxplot)
    """
    pass

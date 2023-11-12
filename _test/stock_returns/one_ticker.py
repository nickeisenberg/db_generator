"""
There was an issue when initializing the stock market database if only one
ticker was entered. The script was to investigate the best way to handle the
issue
"""

import yfinance as yf
import datetime as dt
import pandas as pd
import time
import numpy as np

start = dt.datetime.now().replace(
    hour=4-3, minute=0, second=0, microsecond=0
) - dt.timedelta(days=5)
end = dt.datetime.now().replace(
    hour=20-3, minute=0, second=0, microsecond=0
)
time_step = '1m'

tickers = ['GME', 'SPY', 'QQQ', 'AMZN']
df0 = yf.download(
    tickers=tickers,
    start=start,
    end=end,
    interval=time_step,
    prepost=True,
)

tickers = ['GME']
now = time.time()
df1 = yf.download(
    tickers=tickers,
    start=start,
    end=end,
    interval=time_step,
    prepost=True,
)

new_col = pd.MultiIndex.from_product([df1.columns.values, ['GME']])
df1 = df1.set_axis(new_col, axis=1)
df1


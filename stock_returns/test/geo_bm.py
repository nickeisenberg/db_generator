import numpy as np
from scipy.signal import medfilt
from stock_returns.create import Create
import sqlalchemy as db
from sqlalchemy.orm import declarative_base as Base
import pandas as pd
import matplotlib.pyplot as plt

engine = db.create_engine(
    "mysql+pymysql://root:@127.0.0.1:3306/stock_return?unix_socket=/tmp/mysql.sock"
)

query = "select * from ohlcv where ticker = 'AMZN' "
query += "and datetime < '2023-08-20'"
df = pd.read_sql(query, engine)

def geo_b_motion(T, delta, S0, mu, sigma):
    dB = np.sqrt(delta) * np.random.normal(0, 1, T.shape[0] - 1)
    B = np.cumsum(dB)
    B = np.hstack((0, B))
    return S0 * np.exp((mu - sigma ** 2 / 2) * T + sigma * B)

# log returns
df.head()

open = df['open'].interpolate().values
open = medfilt(open, 3)
time = np.arange(open.size)

mu = np.diff(np.log(open)).mean()
std = np.diff(np.log(open)).std()
sims = np.array([geo_b_motion(time, 1, open[0], mu, std) for _ in range(100)])

inds = np.mean((sims - open) ** 2, axis=1).argsort()

plt.plot(time, sims[inds[0]])
plt.plot(time, open)
plt.show()

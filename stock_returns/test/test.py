import numpy as np
from scipy.signal import medfilt
from stock_returns.create import Create
import sqlalchemy as db
from sqlalchemy.orm import declarative_base as Base
import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt

engine = db.create_engine(
    "mysql+pymysql://root:@127.0.0.1:3306/stock_return?unix_socket=/tmp/mysql.sock"
)

base = Base()
database = Create(engine=engine, base=base)

database.initialize(tickers=['AMZN', 'VOO'])


#--------------------------------------------------
query = "select * from ohlcv where ticker = 'AMZN' "
query += "and datetime < '2023-08-20'"
df = pd.read_sql(query, engine)

# test the trigger
query = "select * from transaction_history"
df = pd.read_sql(query, engine)
data = [
    [1, dt.datetime(2023, 1, 1, 6, 0, 0), 'spy', 1, 1, 5, 10],
    [2, dt.datetime(2023, 1, 1, 6, 0, 0), 'spy', 1, 1, 5, 20],
    [3, dt.datetime(2023, 1, 1, 7, 0, 0), 'spy', 1, -1, 5, 15],
    [4, dt.datetime(2023, 1, 1, 8, 0, 0), 'spy', 1, 1, 5, 10],
    [5, dt.datetime(2023, 1, 1, 8, 0, 0), 'spy', -1, -1, 5, 20],
    [6, dt.datetime(2023, 1, 1, 8, 0, 0), 'spy', -1, -1, 5, 10],
    [7, dt.datetime(2023, 1, 1, 8, 0, 0), 'spy', -1, 1, 5, 5]
]
pd.concat(
    (df, pd.DataFrame(data, columns=df.columns))
).to_sql('transaction_history', engine, if_exists='append', index=False)

# see if the tirgger worked
query = "select * from transaction_history"
df = pd.read_sql(query, engine)

query = "select * from portfolio"
df = pd.read_sql(query, engine)

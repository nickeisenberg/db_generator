import datetime as dt
import numpy as np
from stock_returns.create import Create
import sqlalchemy as db
from sqlalchemy.orm import declarative_base as Base
import pandas as pd

engine = db.create_engine(
    "mysql+pymysql://root:@127.0.0.1:3306/stock_return?unix_socket=/tmp/mysql.sock"
)

base = Base()
database = Create(engine=engine, base=base)

database.initialize()

#--------------------------------------------------

query = "show columns from transaction_history"
df = pd.read_sql(query, engine)
print(df)

query = "show tables;"
df = pd.read_sql(query, engine)
print(df)

query = "select * from transaction_history"
df = pd.read_sql(query, engine)
print(df)

query = "select * from portfolio"
df = pd.read_sql(query, engine)
print(df)

# find longest chain of nan's
query = "select open from ohlcv where ticker = 'AMZN'"
df = pd.read_sql(query, engine)
inds = np.isnan(df.values)
df.iloc[inds] = np.repeat(999999, len(df.iloc[np.isnan(df.values)]))
nan_in_a_row = df.values.reshape(-1)
nan_in_a_row = np.where(
    np.diff(np.hstack(([False], nan_in_a_row==999999, [False])))
)[0].reshape((-1, 2))
print(nan_in_a_row[np.argmax(np.diff(nan_in_a_row, axis=1))])

query = f"select datetime from ohlcv"
dates = pd.read_sql(
    query, engine
)['datetime'].values.astype(str)

d = dates[0]
l = d[:10]
r = d[11: -10]
d = l + ' ' + r

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

query = "select * from transaction_history "
query += "where user_id = 1 and ticker = 'AMZN' and position_type = -1"
df = pd.read_sql(query, engine)
print(df)

query = f"select datetime from ohlcv"
dates = pd.read_sql(
    query, engine
)['datetime'].values.astype(str)

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

query = "select * from ohlcv where ticker = 'SPY' "
query += "and datetime < '2023-08-02'"
df = pd.read_sql(query, engine)

df.head()

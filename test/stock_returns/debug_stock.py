import numpy as np
import datetime as dt
import pandas as pd
import stock_returns.create as src
import stock_returns.utils as utils
from sqlalchemy_utils import drop_database, database_exists, create_database
from sqlalchemy.orm import declarative_base, sessionmaker
import sqlalchemy as db


# set up the engine
engine = db.create_engine(
    "mysql+pymysql://root:@127.0.0.1:3306/stock_return?unix_socket=/tmp/mysql.sock"
)

# create the database
if database_exists(engine.url):
    drop_database(engine.url) 
if not database_exists(engine.url):
    create_database(engine.url) 

# get the base
base = declarative_base()

# make the tables
OHLCV = src.OHLCV(base)
TransactionHistory = src.TransactionHistory(base)
Portfolio = src.Portfolio(base)

base.metadata.create_all(bind=engine)

# set up the trigger
trigger_path='./stock_returns/sql/trans_to_port_trig.sql'
with engine.connect() as conn:
    conn.execute(
        db.text(
            utils.convert_sql_to_string(trigger_path)
        )
    )
    conn.commit()


# start the session
session = sessionmaker(bind=engine)()

# add rows for user 1
for i in range(1, 3):
    entry = TransactionHistory(
        1, dt.datetime(2000, 1, i), 'abc', 1, 1, 10, 10
    )
    session.add(entry)
    entry = TransactionHistory(
        2, dt.datetime(2000, 1, i), 'abc', 1, 1, 10, 10
    )
    session.add(entry)
session.commit()

# add rows for user 1
for i in range(1, 3):
    entry = TransactionHistory(
        1, dt.datetime(2000, 1, i + 2), 'abc', 1, -1, 2, 10
    )
    session.add(entry)
session.commit()

debug = utils.Debug(engine)

debug.debug

debug.trans_df

query = "select ticker from transaction_history "
query += f"where user_id = 1"
np.unique(pd.read_sql(query, engine).values)


query = f"ticker == 'abc' "
query += f"and user_id == 1"
query += "and position_type == 1"
debug.port_df.query(query)['position'].values[0]

_trans_df = self.trans_df.query(query)

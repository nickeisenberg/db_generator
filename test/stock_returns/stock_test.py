import platform
import datetime as dt
import numpy as np
from stock_returns.create import Create, TransactionHistory
import sqlalchemy as db
from sqlalchemy.orm import declarative_base as Base
from sqlalchemy.orm import sessionmaker
import pandas as pd
from stock_returns.utils import Debug, longest_chain_of_nans

p='boomhower'

if platform.system() == 'Linux':
    engine = db.create_engine(
        f"mysql+pymysql://root:{p}@127.0.0.1:3306/stock_return"
    )
else:    
    engine = db.create_engine(
        f"mysql+pymysql://root:{p}@127.0.0.1:3306/stock_return?unix_socket=/tmp/mysql.sock"
    )

base = Base()
database = Create(engine=engine, base=base)

database.initialize(tickers = ['SPY'], no_investors=1, make_nans=0)

debug = Debug(engine)
debug.debug


#--------------------------------------------------
# check if the trigger works after the fact

query = "select * from transaction_history where position_type = 1"
trans_df0 = pd.read_sql(query, engine)
query = "select * from portfolio where position_type = 1"
port_df0 = pd.read_sql(query, engine)

# create a new session and add a transaction to see of the portfolio updates
engine = db.create_engine(
    "mysql+pymysql://root:@127.0.0.1:3306/stock_return?unix_socket=/tmp/mysql.sock"
)
session = sessionmaker(engine)()

base = Base()
transaction = TransactionHistory(base)

entry = transaction(1, dt.datetime(2000, 1, 1), 'SPY', 1, 1, 99, 99)

session.add(entry)
session.commit()

query = "select * from transaction_history where position_type = 1"
trans_df = pd.read_sql(query, engine)
query = "select * from portfolio where position_type = 1"
port_df = pd.read_sql(query, engine)

trans_df0
trans_df

port_df0
port_df

#--------------------------------------------------

# see if the position_type check works
query = "show columns from transaction_history"
trans_df = pd.read_sql(query, engine)

insert_bad_trans = pd.DataFrame(
    data=[[1, 100, dt.datetime(2000, 1, 1), 'ABC', 2, 1, 100, 10]],
    columns = trans_df['Field']
)
insert_bad_trans.to_sql('transaction_history', engine, if_exists='append', index=False)

#--------------------------------------------------

# find longest chain of nan's

longest_chain_of_nans(engine, 'SPY')


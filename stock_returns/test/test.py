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

database.initialize()

#--------------------------------------------------

query = "select * from transaction_history"
df = pd.read_sql(query, engine)

df

query = "select * from portfolio"
df = pd.read_sql(query, engine)

df

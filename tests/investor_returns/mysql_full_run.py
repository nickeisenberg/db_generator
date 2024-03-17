import platform
import sqlalchemy as db
import os
import pandas as pd

from dbgen.investor_returns import create
from tests.investor_returns.utils import Debug


p = os.environ['MYSQL_ROOT']
if platform.system() == 'Linux':
    engine = db.create_engine(
        f"mysql+pymysql://root:{p}@127.0.0.1:3306/stock_return"
    )
else:    
    engine = db.create_engine(
        f"mysql+pymysql://root:{p}@127.0.0.1:3306/stock_return?unix_socket=/tmp/mysql.sock"
    )

database = create(engine=engine)

debug = Debug(engine)
debug.debug

query = 'select * from transaction_history'
df = pd.read_sql(query, con=engine)
print(df.head())

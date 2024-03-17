import platform
import sqlalchemy as db
import os

from dbgen.stock_returns import create
from tests.stock_returns.utils import Debug


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

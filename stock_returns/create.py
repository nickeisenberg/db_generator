import sqlalchemy as db
from sqlalchemy.orm import declarative_base as Base
from sqlalchemy_utils import create_database, database_exists, drop_database
import faker
import numpy as np
from stock_returns.utils import get_prices

_fk = faker.Faker()

_base = Base()

def OHLC(base):

    class _OHLC(base):
        # table name for User model
        __tablename__ = "ohlc"
    
        # user columns
        datetime = db.Column(
            db.String(20), primary_key=True, autoincrement=False
        )
        ticker = db.Column(
            db.String(5), primary_key=True, autoincrement=False
        )
        open = db.Column(db.Float())
        high = db.Column(db.Float())
        low = db.Column(db.Float())
        close = db.Column(db.Float())
        volume = db.Column(db.Float())
        unix_timestamp = db.Column(db.Integer())
     
        def __init__(
            self,
            ticker,
            open,
            high,
            low,
            close,
            volume,
            unix_timestamp,
            datetime
        ):
            self.ticker = ticker
            self.open = open 
            self.high = high
            self.low = low
            self.close = close
            self.volume = volume
            self.unix_timestamp = unix_timestamp
            self.datetime = datetime 

    return _OHLC


class Create:

    def __init__(
        self,
        engine,
        base=_base,
        OHLC=OHLC,
    ):
        self.engine = engine
        self.base = base
        self.OHLC = OHLC(base)
        self._initialized = False


    def initialize(
            self,
        tickers,
        ohlc,
        with_entries=True,
        drop_db_if_exists=True,
        faker_seed=0,
        numpy_seed=0
    ):

        np.random.seed(numpy_seed) 
        faker.Faker.seed(faker_seed)


        if self._initialized:
          raise Exception("Database already initialized.")
        
        if drop_db_if_exists:
            if database_exists(self.engine.url):
                drop_database(self.engine.url) 

        if not database_exists(self.engine.url):
            create_database(self.engine.url) 

        self.base.metadata.create_all(bind=self.engine)

        self._initialized = True
        
        if not with_entries:
            return None

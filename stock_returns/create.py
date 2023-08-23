import sqlalchemy as db
from sqlalchemy.orm import declarative_base as Base
from sqlalchemy_utils import create_database, database_exists, drop_database
import numpy as np
import datetime as dt
import yfinance as yf

_base = Base()

def OHLCV(base):

    class _OHLCV(base):
        # table name for User model
        __tablename__ = "ohlcv"
    
        # user columns
        datetime = db.Column(
            db.String(25), primary_key=True, autoincrement=False
        )
        ticker = db.Column(
            db.String(5), primary_key=True, autoincrement=False
        )
        open = db.Column(db.Float())
        high = db.Column(db.Float())
        low = db.Column(db.Float())
        close = db.Column(db.Float())
        volume = db.Column(db.Float())
        timestamp = db.Column(db.Integer())
     
        def __init__(
            self,
            datetime,
            ticker,
            open,
            high,
            low,
            close,
            volume,
            timestamp,
        ):
            self.ticker = ticker
            self.open = open 
            self.high = high
            self.low = low
            self.close = close
            self.volume = volume
            self.timestamp = timestamp
            self.datetime = datetime 

    return _OHLCV


class Create:

    def __init__(
        self,
        engine,
        base=_base,
        OHLCV=OHLCV
    ):
        self.engine = engine
        self.base = base
        self.OHLCV = OHLCV(base)
        self._initialized = False

    def initialize(
        self,
        tickers=None,
        start=None,
        end=None,
        time_step='1m',
        with_entries=True,
        drop_db_if_exists=True,
    ):

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

        if tickers is None:
                tickers = ['SPY', 'QQQ', 'VTI']

        if end is None:
            end = dt.datetime.now().replace(
                hour=20-3, minute=0, second=0, microsecond=0
            )

        if start is None:
            start = dt.datetime.now().replace(
                hour=4-3, minute=0, second=0, microsecond=0
            ) - dt.timedelta(days=29)
        
        # contining the initialization with entries
        elapsed_time = (end - start).total_seconds()
        batch_time = 60 * 60 * 24 * 7
        
        batch_no = 0
        while batch_no * batch_time < elapsed_time:
            batch_no += 1
            print(
                f'batch {batch_no} / {elapsed_time // batch_time + 1}'
            )

            df = yf.download(
                tickers=tickers,
                start=start + dt.timedelta(seconds = batch_time * (batch_no - 1)),
                end=min(
                    start + dt.timedelta(seconds = batch_time * batch_no), 
                    end
                ),
                interval=time_step,
                prepost=True
            )


            df.index = df.index.to_series().apply(
                lambda x: str(x)[: -6]
            ).reset_index(drop=True)

            df.columns.names = ['ohlcv', 'ticker']

            for ticker in tickers:

                query = f"ticker == '{ticker}' "
                query += "and ohlcv in ['Open', 'High', 'Low', 'Close', 'Volume']"
                sub_df = df.T.query(
                    query
                ).T.reset_index()
                
                sub_df['timestamp'] = [
                    dt.datetime.strptime(
                        npdt, '%Y-%m-%d %H:%M:%S'
                    ).timestamp() for npdt in sub_df['Datetime'].values
                ]

                sub_df.insert(1, 'ticker', np.repeat(ticker, len(sub_df)))

                sub_df.columns = [
                    x.lower() 
                    for x in sub_df.columns.get_level_values('ohlcv').values
                ]
                cols = [
                    'datetime', 'ticker', 'open',
                    'high', 'low', 'close', 'volume', 'timestamp'
                ]
                # sub_df = sub_df[cols]
                sub_df[cols].to_sql(
                    'ohlcv', self.engine, if_exists='append', index=False)



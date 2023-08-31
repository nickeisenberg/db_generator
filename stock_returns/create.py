import sqlalchemy as db
from sqlalchemy import event
from sqlalchemy.orm import declarative_base as Base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists, drop_database
import numpy as np
import datetime as dt
import yfinance as yf
from stock_returns.trigger import convert_sql_trigger_to_string


_base = Base()


def OHLCV(base):

    class _OHLCV(base):
        # table name for User model
        __tablename__ = "ohlcv"
    
        # user columns
        datetime = db.Column(
            db.DateTime(), primary_key=True, autoincrement=False
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
            self.datetime = datetime 
            self.ticker = ticker
            self.open = open 
            self.high = high
            self.low = low
            self.close = close
            self.volume = volume
            self.timestamp = timestamp

    return _OHLCV

def TransactionHistory(base):

    class _TransactionHistory(base):
        # table name for User model
        __tablename__ = "transaction_history"
    
        # user columns
        id = db.Column(
            db.Integer(), primary_key=True, autoincrement=True
        )
        datetime = db.Column(
            db.DateTime()
        )
        ticker = db.Column(
            db.String(6),
        )
        position_type = db.Column(
            db.Integer()
        )
        action = db.Column(
            db.Integer()
        )
        no_shares = db.Column(db.Float())
        at_price = db.Column(db.Float())
     
        def __init__(
            self,
            datetime,
            ticker,
            position_type,
            action,
            no_shares,
            at_price
        ):
            self.datetime = datetime 
            self.ticker = ticker 
            self.position_type = position_type 
            self.action = action
            self.no_shares = no_shares
            self.at_price = at_price

    return _TransactionHistory 


def Portfolio(base):

    class _Portfolio(base):
        # table name for User model
        __tablename__ = "portfolio"
    
        # user columns
        ticker = db.Column(
            db.String(6), primary_key=True, autoincrement=False
        )
        position_type = db.Column(
            db.Integer, primary_key=True, autoincrement=False
        )
        position = db.Column(
            db.Integer()
        )
        last_price = db.Column(db.Float())
        cost_basis = db.Column(db.Float())
        current_value = db.Column(db.Float())
        realized_profit = db.Column(db.Float())
        gain = db.Column(db.Float())
     
        def __init__(
            self,
            ticker,
            position_type,
            position,
            last_price,
            cost_basis,
            current_value,
            realized_profit,
            gain
        ):
            self.ticker = ticker
            self.position_type = position_type 
            self.position = position 
            self.last_price = last_price 
            self.cost_basis = cost_basis
            self.current_value = current_value 
            self.realized_profit = realized_profit 
            self.gain = gain

    return _Portfolio 


class Create:

    def __init__(
        self,
        engine,
        base=_base,
        OHLCV=OHLCV,
        TransactionHistory=TransactionHistory,
        Portfolio=Portfolio
    ):
        self.engine = engine
        self.base = base
        self.OHLCV = OHLCV(base)
        self.TransactionHistory = TransactionHistory(base)
        self.Portfolio = Portfolio(base)
        self._initialized = False

    def initialize(
        self,
        with_entries=True,
        with_trigger=False,
        tickers = ['SPY', 'QQQ', 'VTI'],
        start = dt.datetime.now().replace(
            hour=4-3, minute=0, second=0, microsecond=0
        ) - dt.timedelta(days=29),
        end = dt.datetime.now().replace(
            hour=20-3, minute=0, second=0, microsecond=0
        ),
        time_step='1m',
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
        
        if with_trigger:
            with self.engine.connect() as conn:
                conn.execute(
                    db.text(
                        convert_sql_trigger_to_string(
                            './stock_returns/trigger.sql'
                        )
                    )
                )
                conn.commit()

        self._initialized = True
        
        if not with_entries:
            return None
        
        # batch the time for yfinance stock scraping
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

            # remove the GMT time part that yfinaces gives
            df.index = df.index.to_series().apply(
                lambda x: str(x)[: -6]
            ).reset_index(drop=True)
            
            # rename the multicolumn
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
                
                # add a columns with just the ticker repeated
                sub_df.columns = [
                    x.lower() 
                    for x in sub_df.columns.get_level_values('ohlcv').values
                ]
                cols = [
                    'datetime', 'ticker', 'open',
                    'high', 'low', 'close', 'volume', 'timestamp'
                ]
                
                # push to the sql server
                sub_df[cols].to_sql(
                    'ohlcv', self.engine, if_exists='append', index=False)




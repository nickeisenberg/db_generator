import sqlalchemy as db
from sqlalchemy import event
from sqlalchemy.orm import declarative_base as Base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists, drop_database
import numpy as np
import datetime as dt
import yfinance as yf
import pandas as pd
from stock_returns.utils import convert_sql_trigger_to_string


_base = Base()


def OHLCV(base):
    """
    This function takes a SQLAlchemy declarative_base and returns a SQLAlchemy 
    table/mapper.
    
    Parameters:
    base (sqlalchemy.orm.declarative_base)
    --------------------------------------------------

    Returns:
    _OHLCV (SQLAlchemy table/mapper class). 
    --------------------------------------------------

    _OHLCVwill maps rows to a sql table names "ohlcv" under a 
    sqlalchemy.orm.sessionmaker setting. 
    The _OHLCV.__init__() takes the following parameters.

    Parameters:
    --------------------------------------------------
    datetime (datetime.datetime)
    ticker (str) : stock ticker
    open (float) : open price of the period
    high (float) : high price of the period
    low (float) : low price of the period
    close (float : close price of the period)
    volume (float) : volume price of the period
    timestamp (float) : unix timestamp

    Example Usage:
    --------------------------------------------------
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.orm import declarative_base as Base
    
    base = Base()
    ohlcv = OHLCV(base)

    engine = create_engine(...)

    if not database_exists(engine.url):
        create_database(engine.url) 
    base.metadata.create_all(bind=engine)

    session = sessionmaker(bind=engine)()

    entry = ohlcv(datetime, ticker, open, high, low, close, volumne, timestamp)

    session.add(entry)
    session.commit()
    session.close()
    """

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
    """
    This function takes a SQLAlchemy declarative_base and returns a SQLAlchemy 
    table/mapper.
    
    Parameters:
    base (sqlalchemy.orm.declarative_base)
    --------------------------------------------------

    Returns:
    _Transaction_History (SQLAlchemy table/mapper class). 
    --------------------------------------------------

    _Transaction_History will map rows to a sql table named 
    "transaction_history" under a sqlalchemy.orm.sessionmaker setting. 
    The _Transaction_History.__init__() takes the following parameters.

    Parameters:
    --------------------------------------------------
    datetime (datetme.datetime): datetime of the transaction
    ticker (str): stock ticker
    position_type (float): -1 or 1 indicating a short or long position_type
                           respectively
    action (float): -1 or 1 indicating a purchase or a sell
    no_shares (float): number of shares in the transaction
    at_price (float): price per share of the transaction

    Example Usage:
    --------------------------------------------------
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.orm import declarative_base as Base
    
    base = Base()
    transaction_history = Transaction_History(base)

    engine = create_engine(...)

    if not database_exists(engine.url):
        create_database(engine.url) 
    base.metadata.create_all(bind=engine)

    session = sessionmaker(bind=engine)()

    entry = transaction_history(
        datetime, ticker, position_type, action, no_shares, at_price
    )

    session.add(entry)
    session.commit()
    session.close()
    """

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
    """
    Warning:
    I suggest to not use this function directly. I believe it would be better
    to pair Transaction_History with a trigger that is connected to the
    "portfolio" table. This function should usually be used to just create
    the "portfolio" tables.

    This function takes a SQLAlchemy declarative_base and returns a SQLAlchemy 
    table/mapper.
    
    Parameters:
    base (sqlalchemy.orm.declarative_base)
    --------------------------------------------------

    Returns:
    _Portfolio (SQLAlchemy table/mapper class). 
    --------------------------------------------------

    _Portfolio will map rows to a sql table named 
    "portfolio" under a sqlalchemy.orm.sessionmaker setting. 
    The _Portfolio.__init__() takes the following parameters.

    Parameters:
    --------------------------------------------------
    datetime (datetme.datetime): datetime of the transaction
    ticker (str): stock ticker
    position_type (float): -1 or 1 indicating a short or long position_type
                           respectively
    action (float): -1 or 1 indicating a purchase or a sell
    no_shares (float): number of shares in the transaction
    at_price (float): price per share of the transaction

    Example Usage:
    --------------------------------------------------
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.orm import declarative_base as Base
    
    base = Base()
    transaction_history = Transaction_History(base)

    engine = create_engine(...)

    if not database_exists(engine.url):
        create_database(engine.url) 
    base.metadata.create_all(bind=engine)

    session = sessionmaker(bind=engine)()

    entry = transaction_history(
        datetime, ticker, position_type, action, no_shares, at_price
    )

    session.add(entry)
    session.commit()
    session.close()
    """

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
        total_invested = db.Column(db.Float())
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
            total_invested,
            current_value,
            realized_profit,
            gain
        ):
            self.ticker = ticker
            self.position_type = position_type 
            self.position = position 
            self.last_price = last_price 
            self.cost_basis = cost_basis
            self.total_invested = total_invested 
            self.current_value = current_value 
            self.realized_profit = realized_profit 
            self.gain = gain

    return _Portfolio 


class Create:
    """
    Warning:
    All of the features of this class are only tested with MySQL syntax. The
    trigger used in the classes initialize method will need to be editted to
    satisfy the syntax of other SQL syntaxes such as sqlite, postgreSQL, etc.
    The classes initialize method should work on databases other than MySQL
    if the user sets with_trigger=False or writes his or her own trigger. If 
    a with_trigger=False then the "portfolio" table created by initialize will 
    not be populated.


    The Create class will be used to create the database, create the tables
    and initiallize the tables with some data.

    parameters:
    engine (SQLAlchemy engine): A sqlalchemy engine 
    base (sqlalchemy.orm.declarative_base): A sqlalchemy declarative_base 
    OHLCV (OHLCV(base)):
    Transaction_History (Transaction_History(base)):
    Portfolio (Portfolio(base)):
    """
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
        tickers = ['SPY', 'NVDA', 'AMZN'],
        start = dt.datetime.now().replace(
            hour=4-3, minute=0, second=0, microsecond=0
        ) - dt.timedelta(days=29),
        end = dt.datetime.now().replace(
            hour=20-3, minute=0, second=0, microsecond=0
        ),
        time_step='1m',
        with_trigger=True,
        trigger_path='./stock_returns/trigger.sql',
        with_investments=True,
        drop_db_if_exists=True,
    ):
        """
        This function will initialize the database, create the tables and then
        populate the tables with data.

        parameters:
        --------------------------------------------------
        with_entries (boolean): autopopulate the ohlcv table with the tickers
            listed in the ticker list. scraps data using
            yfinance.
        tickers (list): list of tickers
        start (datetime): start time for the stock prices
        end (datetime): end time for the stock prices
        time_step (str): time step for the stock data. yfinance has limitations
            on this.
        with_trigger (boolean): If true, then the trigger will be set to auto
            update the portfolio with the transaction_history 
        trigger_path (path to custom trigger): defaults to a mysql trigger.
            needs to be updated if using a different sql server.
        with_investments (boolean): Will generate investments and auto update
            the porfolio.
        drop_db_if_exists (boolean): Will drop the database and recreate it
            if already exists.

        returns:
        """

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
                        convert_sql_trigger_to_string(trigger_path)
                    )
                )
                conn.commit()

        self._initialized = True
        
        if not with_entries:
            return None
        
        # batch the time for yfinance stock scraping
        elapsed_time = (end - start).total_seconds()
        batch_time = 60 * 60 * 24 * 5
        
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

        if with_investments:

            query = f"select datetime from ohlcv"
            dates = pd.read_sql(
                query, self.engine
            )['datetime'].values.astype(str)

            long_invs = {
                "SPY": [
                    (dates[int(dates.size * 0)], 1, 20), 
                    (dates[int(dates.size * .2)], 1, 10), 
                    (dates[int(dates.size * .25)], -1, 15), 
                    (dates[int(dates.size * .7)], -1, 3), 
                    (dates[int(dates.size * .8)], 1, 10)
                ],
                "AMZN": [
                    (dates[int(dates.size * 0)], 1, 32), 
                    (dates[int(dates.size * .5)], 1, 100), 
                    (dates[int(dates.size * .9)], -1, 100), 
                ],
                "NVDA": [
                    (dates[int(dates.size * .3)], 1, 10), 
                    (dates[int(dates.size * .8)], 1, 50), 
                    (dates[int(dates.size * .9)], -1, 40), 
                ],
            }

            short_invs = {
                "AMZN": [
                    (dates[int(dates.size * .21)], -1, 20), 
                    (dates[int(dates.size * .4)], 1, 10), 
                    (dates[int(dates.size * .8)], -1, 100) 
                ],
                "NVDA": [
                    (dates[int(dates.size * .9)], -1, 10), 
                    (dates[int(dates.size * .98)], 1, 4) 
                ],
            }
            
            session  = sessionmaker(bind=self.engine)()

            for ticker in long_invs.keys():
                for trans in long_invs[ticker]:
                    datetime, action, no_shares = trans
                    l = datetime[:10]
                    r = datetime[11: -10]
                    query = f"select open from ohlcv "
                    query += f"where datetime = '{l + ' ' + r}' "
                    query += f"and ticker = '{ticker}'"
                    open = pd.read_sql(query, self.engine)['open'].values[0]

                    transaction = self.TransactionHistory(
                        datetime, ticker, 1, action, no_shares, open
                    )
                    session.add(transaction)

            for ticker in short_invs.keys():
                for trans in short_invs[ticker]:
                    datetime, action, no_shares = trans 
                    l = datetime[:10]
                    r = datetime[11: -10]
                    query = f"select open from ohlcv "
                    query += f"where datetime = '{l + ' ' + r}' "
                    query += f"and ticker = '{ticker}'"
                    open = pd.read_sql(query, self.engine)['open'].values[0]

                    transaction = self.TransactionHistory(
                        datetime, ticker, -1, action, no_shares, open
                    )
                    session.add(transaction)

            session.commit()
            session.close()

        return None





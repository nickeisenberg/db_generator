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
    table/mapper. The mapper will take the open, low, high, close and volume
    of a stock during a specified time period as well as the datetime 
    and timestamp from the beginning of the time period and the timestep from
    the open of a period to the close of a period and map it to a row in a 
    database titled "ohlcv". The mapper contains of an underlying sqlalchemy
    metadata object that can be combined with an engine to create the table
    if it does not exist. See example usage below.
    
    Parameters
    --------------------------------------------------
    base : sqlalchemy.orm.declarative_base
        A declarative_base that will be inheirited by the underlying class
       

    Returns
    --------------------------------------------------
    _OHLCV(base) : SQLAlchemy table/mapper class 
        _OHLCV maps rows to a sql table names "ohlcv" under a 
        sqlalchemy.orm.sessionmaker setting. 

    Example Usage
    --------------------------------------------------
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.orm import declarative_base as Base
    
    base = Base()
    _OHLCV = OHLCV(base)

    engine = create_engine(...)

    if not database_exists(engine.url):
        create_database(engine.url) 
    base.metadata.create_all(bind=engine)

    session = sessionmaker(bind=engine)()

    entry = _OHLCV(
        datetime, ticker, open, high, low, close, volumne, timestamp
    )

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
            """
            Parameters
            --------------------------------------------------
            datetime : datetime.datetime
                The datetime from the start of the period for the stock price
            ticker : str
                stock ticker
            open : float 
                open price of the period
            high : float
                high price of the period
            low : float 
                low price of the period
            close : float 
                close price of the period
            volume : float
                volume price of the period
            timestamp : float 
                unix timestamp of the datetime

            Returns
            --------------------------------------------------
            A mapable object that can be sent to a sql table with the use of
            sqlalchemy.creat_engine and sqlalchemy.orm.sessionmaker. See 
            example usage in help(OHLCV)
            """
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
    table/mapper. The mapper will take information of a stock transaction and 
    return a mappable object that can then be sent sql table titled
    "transaction_history". The mapper contains of an underlying sqlalchemy
    metadata object that can be combined with an engine to create the table
    if it does not exist. See example usage below.
    
    Parameters
    --------------------------------------------------
    base : sqlalchemy.orm.declarative_base
        A declarative_base that will be inheirited by the underlying class
       

    Returns
    --------------------------------------------------
    _Transaction_History(base) : SQLAlchemy table/mapper class 
        _Transaction_History maps rows to a sql table named 
        "transaction_history" under a sqlalchemy.orm.sessionmaker setting. 

    Example Usage
    --------------------------------------------------
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.orm import declarative_base as Base
    
    base = Base()
    _Transaction_History = Transaction_History(base)

    engine = create_engine(...)

    if not database_exists(engine.url):
        create_database(engine.url) 
    base.metadata.create_all(bind=engine)

    session = sessionmaker(bind=engine)()

    entry = _Transaction_History(
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
            """
            Parameters
            --------------------------------------------------
            datetime : datetime.datetime
                The datetime of the transaction.
            ticker : str
                stock ticker
            position_type : float 
                Indication of whether the transaction is a short or long. 1.0
                refers to a long position and -1.0 refers to a short position.
            action : float
                Indication of whether the transaction refers to a sell or a 
                purchase. 1.0 indicates a purchase and -1.0 indicates a sell.
            no_shares : float 
                Amount of shares either bought or sold in the transaction.
            at_price : float 
                The price per share of the transaction.

            Returns
            --------------------------------------------------
            A mapable object that can be sent to a sql table with the use of
            sqlalchemy.creat_engine and sqlalchemy.orm.sessionmaker. See
            example usage in help(Transaction_History)
            """
            self.datetime = datetime 
            self.ticker = ticker 
            self.position_type = position_type 
            self.action = action
            self.no_shares = no_shares
            self.at_price = at_price

    return _TransactionHistory 


def Portfolio(base):
    """
    Suggestion
    --------------------------------------------------
    This function should predominatly used to only create the portfolio table
    and it should be used in tandom with Transaction_History with the use of
    a SQL trigger. A Trigger should be applied to take the
    transaction_history rows and calculate the performace of the portfolio 
    ranther than using this function and its underlying _Portfolio(base)
    class directly to update the portfolio table.

    This function takes a SQLAlchemy declarative_base and returns a SQLAlchemy 
    table/mapper.
    The mapper will map information about a investorys portfolio to a table
    named "portfolio".
    The mapper contains of an underlying sqlalchemy
    metadata object that can be combined with an engine to create the table
    if it does not exist. See example usage below.
    
    Parameters
    --------------------------------------------------
    base : sqlalchemy.orm.declarative_base
        A declarative_base that will be inheirited by the underlying class
       

    Returns
    --------------------------------------------------
    _Porfolio(base) : SQLAlchemy table/mapper class 
        _Portfolio maps rows to a sql table named "portfolio" under a 
        sqlalchemy.orm.sessionmaker setting. 

    Example Usage
    --------------------------------------------------
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.orm import declarative_base as Base
    
    base = Base()
    _Transaction_History = Transaction_History(base)

    engine = create_engine(...)

    if not database_exists(engine.url):
        create_database(engine.url) 
    base.metadata.create_all(bind=engine)

    session = sessionmaker(bind=engine)()

    entry = _Transaction_History(
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
    Warning
    --------------------------------------------------
    The default settings of the initialize method of this class requires a 
    MySQL server. The default trigger uses MySQL syntax and will not work with
    other SQL servers. However the user may use his or her own trigger if they
    prefer to use another SQL server such as Postgre, sqlite etc.

    This class will be used to create the database, tables and populate the
    tables with real stock data that will be scrapped from yahoo using the 
    yfinance library. The class will also create some fake transactions for an 
    investory and store these transactions in a table named
    "transaction_history". The performance of these transactions will 
    automatically be calculated apon each insertioin of a new transaction by
    the use of a trigger. The performance of the portfolio will be tracked in 
    the "portfolio" table.

    Parameters
    --------------------------------------------------
    engine : sqlalchemy engine
        The engine connecting sqlalchemy to the database.
    base : sqlalchemy.orm.declarative_base, default _base = declarative_base()
        A default is set to a declarative_base().
    OHLCV : default OHLCV(base)
    TransactionHistory : default TransactionHistory(base)
    Portfolio : default Portfolio(base)

    Methods 
    --------------------------------------------------
    initialize
        Initializes the database and data and populates with stock data 
        scraped from yfinance as well as some fake transaction data.

    Example Usage
    --------------------------------------------------
    import sqlalchemy as db
    from sqlalchemy.orm import declarative_base as Base
    import pandas as pd

    dialect="mysql",
    driver="pymysql",
    username="root",
    password="password",
    host="127.0.0.1",
    port="3306",
    db="stock_returns",
    unix_socket="/tmp/mysql.sock"
    
    engine_text = f"{dialect}+{driver}"
    engine_text += f"://{username}:{password}"
    engine_text += f"@{host}:{port}/{db}?unix_socket={unix_socket}"
    
    engine = db.create_engine(
        engine_text
    )
    
    base = Base()
    database = Create(engine=engine, base=base)
    
    database.initialize()

    # query from the database into a pandas dataframe

    query = "select * from transaction_history"
    trans_hist = pd.read_sql(query, engine)
    
    query = "select * from portfolio"
    portfolio = pd.read_sql(query, engine)
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

        Parameters
        --------------------------------------------------
        with_entries : boolean default True
            Autopopulate the ohlcv table with the tickers listed in the 
            ticker list. Scraps data using yfinance.
        tickers : list, Default ['SPY', 'AMZN', 'NVDA']
            list of tickers
        start : datetime, Default dt.datetime.now().replace(
                    hour=4-3, minute=0, second=0, microsecond=0
                ) - dt.timedelta(days=29),
            Start time for the stock prices
        end : datetime, Default dt.datetime.now().replace(
                    hour=4-3, minute=0, second=0, microsecond=0
                ),
            End time for the stock prices. This also assumes the user is in a 
            PST timezone.
        time_step : str, Default '1m'
            Time step for the stock data. yfinance has limitations on this.
        with_trigger : boolean, Default True
            If true, then the trigger will be set to auto update the 
            portfolio with the transaction_history 
        trigger_path : str Default './stock_returns/trigger.sql'
            Defaults to a mysql trigger and needs to be updated if using a 
            different sql server.
        with_investments : boolean, Default True
            Will generate investments and auto update the porfolio.
        drop_db_if_exists : boolean, Default True
            Will drop the database and recreate it if already exists.

        returns:
            The function will create a database with name specified in the 
            engine which is inputed by the user. It will populate the database
            with three tabled named "ohlcv", "transaction_history", and 
            "portfolio"."transaction_history" will be linked to "portfolio" 
            through a trigger and all transactions will update the portfolio
            automatically.
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



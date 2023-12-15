from importlib import resources
from types import NoneType
import sqlalchemy as db
from sqlalchemy import event
from sqlalchemy.orm import declarative_base as Base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists, drop_database
import numpy as np
import datetime as dt
import yfinance as yf
import pandas as pd
from .utils import convert_sql_to_string, transaction_chain


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
        user_id = db.Column(
            db.Integer(), primary_key=True, autoincrement=False
        )
        trans_id = db.Column(
            db.Integer(), primary_key=True, autoincrement=True
        )
        datetime = db.Column(
            db.DateTime()
        )
        ticker = db.Column(
            db.String(6),
        )
        position_type = db.Column(
            db.Integer(), 
            db.CheckConstraint("position_type = 1 or position_type = -1")
        )
        action = db.Column(
            db.Integer()
        )
        no_shares = db.Column(db.Float())
        at_price = db.Column(db.Float())
     
        def __init__(
            self,
            user_id,
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
            user_id : int
                ID of the investor

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
            self.user_id = user_id 
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
    _Portfolio = Portfolio(base)

    engine = create_engine(...)

    if not database_exists(engine.url):
        create_database(engine.url) 
    base.metadata.create_all(bind=engine)

    session = sessionmaker(bind=engine)()

    entry = Portfolio(
        ticker, position_type, position, last_price, cost_basis, 
        total_invested, current_value, realized_profit, gain 
    )

    session.add(entry)
    session.commit()
    session.close()
    """
    class _Portfolio(base):
        # table name for User model
        __tablename__ = "portfolio"
    
        # user columns
        user_id = db.Column(
            db.Integer, primary_key=True, autoincrement=False
        )
        ticker = db.Column(
            db.String(6), primary_key=True, autoincrement=False
        )
        position_type = db.Column(
            db.Integer(), 
            db.CheckConstraint("position_type = 1 or position_type = -1"),
            primary_key=True,
            autoincrement=False
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
            user_id,
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
            """
            These values will be auto-calculated by 
            './stock_returns/sql/trans_to_port_trig.sql'.

            Parameters
            --------------------------------------------------
            user_id : int 
                ID of the investor.

            ticker : str
                The stock's ticker.

            position_type : float
                Indication whether the position is a short or a long.
                1.0 indicated a long and -1.0 indicates a short.

            position : float
                The amount of shares either long or short.

            last_price : float
                The last known price of the stock of the position.

            cost_basis : float
                *   For a long postition, this is the average price per share 
                    of the position and this is only affected by new purchases.
                    For a purchase of size no_shares at a price of at_price, 
                    the cost basis is calculated to be 
                        cost_basis = (
                            ((position - no_shares) * cost_basis) 
                            + no_shares * at_price
                        ) / position,
                    where position = position + no_shares is the size of the 
                    position after the purchase of no_shares new shares.
                *   For a short postition, this represents the average price 
                    per share that the investory needs to pay back and this 
                    is only affected by borrowing more of the stock.
                    For a short of size no_shares at a price of at_price, 
                    the cost basis is calculated to be 
                        cost_basis = (
                            (-1.0 * (position + no_shares) * cost_basis) 
                            + (no_shares * at_price)
                        ) / position * -1.0,
                    where position = position - no_shares is the size of the 
                    position after the short of no_share additional shares.

            total_invested : float 
                *   For a long position, this is the total amount of money that
                    has been invested into the stock.
                *   For a short position, this is the total amount of money 
                    that has gone into repaying the total amount borrowed.

            current_value : float 
                last_price * position. 
                *   For a long position...
                *   For a short position...

            realized_profit : float 
                *   For long position, this is the total amount of money that 
                    has been accrued through selling stock. 
                    This number being positive does not mean that the 
                    invester is up on his investment, it just means that 
                    they have sold some of their stock.
                *   For a short position, this is...

            gain : float 
                This is the percent gain that the investor has on thier
                investment.
                *   For a long position...
                *   For a short position...

            Returns
            --------------------------------------------------
            A mapable object that can be sent to a sql table with the use of
            sqlalchemy.creat_engine and sqlalchemy.orm.sessionmaker. See
            example usage in help(Portfolio).
            """
            self.user_id = user_id 
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
        with_entries: bool = True,
        no_investors: int = 5,
        tickers: list[str] = ['SPY', 'NVDA', 'AMZN'],
        start: dt.datetime = dt.datetime.now().replace(
            hour=4-3, minute=0, second=0, microsecond=0
        ) - dt.timedelta(days=29),
        end: dt.datetime = dt.datetime.now().replace(
            hour=20-3, minute=0, second=0, microsecond=0
        ),
        time_step: str = '1m',
        with_trigger: bool = True,
        trigger_path: str | NoneType = None,
        with_investments: bool = True,
        make_nans: int = 20,
        max_nans_in_a_row: int = 5,
        drop_db_if_exists: bool = True,
    ):
        """
        This function will initialize the database, create the tables and then
        populate the tables with data.

        Parameters
        --------------------------------------------------
        with_entries : boolean default True
            Autopopulate the ohlcv table with the tickers listed in the 
            ticker list. Scraps data using yfinance.

        no_investors : int, Default 5
            The number of investors for which transaction and portfolio data 
            is generated for.

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

        make_nans : int, Default 20
            This will randomly select make_nans many dates for the open, high,
            low, close and volume columns to set to None.

        max_nans_in_a_row : int, Default 5
            This will randomly select a number 1 to max_nans_in_a_row for each 
            date from the randomly selected date from make_nans and then set 
            that many timestamps in a row to None.

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
            if trigger_path is None:
                with resources.open_text(
                    'dbgen.stock_returns._sql', 'trigger.sql'
                ) as file:
                    sql_content = file.read()
        
                with self.engine.connect() as conn:
                    conn.execute(db.text(sql_content))
            else:
                with self.engine.connect() as conn:
                    conn.execute(
                        db.text(
                            convert_sql_to_string(trigger_path)
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

            if len(tickers) == 1:
                col = pd.MultiIndex.from_product([df.columns.values, tickers])
                df = df.set_axis(col, axis=1)

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
                
                for col in ['Open', 'High', 'Low', 'Close', 'Volume']:
                    sub_df[col] = sub_df[col].astype(float).interpolate()
                
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

            session  = sessionmaker(bind=self.engine)()
            
            dates_used = np.array([])
            for user_id in range(1, no_investors + 1):
                
                # num_longs = np.random.choice(np.arange(5))
                num_longs = 3
                long_invs = {
                    t: transaction_chain(1.0, num_longs, dates) for t in tickers
                }


                for ticker in long_invs.keys():
                    for trans in long_invs[ticker]:
                        datetime, action, no_shares = trans
                        dates_used = np.append(dates_used, datetime)
                        l = datetime[:10]
                        r = datetime[11: -10]
                        query = f"select open from ohlcv "
                        query += f"where datetime = '{l + ' ' + r}' "
                        query += f"and ticker = '{ticker}'"
                        at_price = pd.read_sql(
                            query, self.engine
                        )['open'].values[0]

                        transaction = self.TransactionHistory(
                            user_id,
                            datetime, 
                            ticker, 
                            1,
                            action, 
                            no_shares,
                            at_price
                        )
                        session.add(transaction)


            for user_id in range(1, no_investors + 1):

                # num_shorts = np.random.choice(np.arange(5))
                num_shorts = 2
                short_invs = {
                    t: transaction_chain(-1.0, num_shorts, dates) for t in tickers
                }

                for ticker in short_invs.keys():
                    for trans in short_invs[ticker]:
                        datetime, action, no_shares = trans 
                        dates_used = np.append(dates_used, datetime)
                        l = datetime[:10]
                        r = datetime[11: -10]
                        query = f"select open from ohlcv "
                        query += f"where datetime = '{l + ' ' + r}' "
                        query += f"and ticker = '{ticker}'"
                        open = pd.read_sql(
                            query, self.engine
                        )['open'].values[0]

                        transaction = self.TransactionHistory(
                            user_id, 
                            datetime, 
                            ticker, 
                            -1, 
                            action, 
                            no_shares, 
                            open
                        )
                        session.add(transaction)

            if make_nans > 0:
                dates_not_used = np.setdiff1d(dates, dates_used)
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    rm_dates = np.random.choice(dates_not_used, make_nans)
                    for date in rm_dates:
                        l = date[:10]
                        r = date[11: -10]
                        date = l + ' ' + r
                        in_a_row = np.random.randint(1, max_nans_in_a_row)
                        for i in range(in_a_row):
                            d = dt.datetime.strptime(
                                date, '%Y-%m-%d %H:%M:%S'
                            ) + dt.timedelta(seconds=60 * i)
                            d = dt.datetime.strftime(d, '%Y-%m-%d %H:%M:%S')
                            with self.engine.connect() as conn:
                                query = f"update ohlcv "
                                query += f"set {col} = NULL "
                                query += f"where datetime = '{d}'"
                                conn.execute(
                                    db.text(query)
                                )
                                conn.commit()

            session.commit()
            session.close()

        return None




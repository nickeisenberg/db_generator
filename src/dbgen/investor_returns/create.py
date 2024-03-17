import datetime as dt
from ._tables import InvestorReutrns

def create(engine,
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
           trigger_path: str | None = None,
           make_nans: int = 20,
           max_nans_in_a_row: int = 5,
           drop_db_if_exists: bool = True):

    investor_returns = InvestorReutrns(engine)
    return investor_returns.initialize(no_investors=no_investors,
                                       tickers=tickers,
                                       start=start,
                                       end=end,
                                       time_step=time_step,
                                       with_trigger=with_trigger,
                                       trigger_path=trigger_path,
                                       make_nans=make_nans,
                                       max_nans_in_a_row=max_nans_in_a_row,
                                       drop_db_if_exists=drop_db_if_exists)


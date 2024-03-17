import numpy as np
import pandas as pd

class Debug:
    """
    A simple debugger that checks if the portfolio matches up with the 
    transaction_history after the fact. This debugger will check the 
    transaction history and see the portfolio has the correct position size.

    Example Usage
    -------------

    import sqlalchemy as db 

    engine = db.create_engine(...)

    debug = Debug(engine)
    
    # Display the discrepencies in the long position and and short positions.
    debug.debug
    """

    def __init__(self, engine):
        self.engine = engine
        self.trans_df = pd.read_sql(
            "select * from transaction_history", 
            engine
        )
        self.port_df = pd.read_sql(
            "select * from portfolio", 
            engine
        )
        self.users = np.unique(self.trans_df['user_id'])

    def _debug(self, p_type=1, verbose=False, from_debug=False):
        bad_count = 0
        id = {1: 'long', -1: 'short'}
        for user in self.users:
            query = "select ticker from portfolio "
            query += f"where user_id = {user} and position_type = {p_type}"
            tickers = np.unique(pd.read_sql(query, self.engine).values)
            if verbose:
                print(f"Starting the {id[p_type]} debug for user {user}")
                print(f"----------------------------------")
            for ticker in tickers:
                if verbose:
                    print(f"Debug for {ticker}")
                query = f"ticker == '{ticker}' "
                query += f"and user_id == {user}"
                query += f"and position_type == {p_type}"

                _trans_df = self.trans_df.query(query)
                tpos = (_trans_df['action'] * _trans_df['no_shares']).sum()

                ppos = self.port_df.query(query)['position'].values[0]
                
                if tpos - ppos != 0:
                    bad_count += 1

        if from_debug:
            return bad_count

        if bad_count == 0:
            print('all good')
        else:
            print('error')

    @property
    def debug(self):
        long = self._debug(p_type=1, from_debug=True)
        short = self._debug(p_type=-1, from_debug=True)
        print(f"long errors {long} short error {short}")

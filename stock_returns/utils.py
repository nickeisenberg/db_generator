import numpy as np
import pandas as pd

def convert_sql_to_string(filepath):
    """
    This function will take a .sql file and output all if its contents into a 
    single string. The intention of this function to allow one to exectute 
    some sql code within a sqlalchemy setting. 
    For the code to be executed properly, comments should not be used 
    inside the .sql file. See example usage below.

    Parameters
    --------------------------------------------------
    filepath : str
        The path to the sql file.

    Returns
    --------------------------------------------------
    string

    Example Usage 
    --------------------------------------------------
    Suppose we have two tables, table_1 and table_2.
    Table_1 and Table_2 have the forms
    
         | id | t1_col1 | t1_col2    | id | t2_col1 | t2_col2
         |----| --------|--------    |----| --------|--------
         | 1  |   ...   |  ...       | 1  |   ...   |  ...   
         | 2  |   ...   |  ...       | 2  |   ...   |  ...   
    
    The following are the contents of the file named 'sql_code.sql'.

    ```sql
    create trigger
        example_trigger
    after insert on
        table_1
    for each row
    begin
    INSERT INTO
        table_2 (
            t2_col1,
            t2_col2
        )
    values 
        (
            new.t1_col1,
            new.t1_col2
        ) 
    ON DUPLICATE KEY UPDATE
        t2_col1 = t2_col1 + new.t1_col1,
        t2_col2 = t2_col2 + new.t1_col2
    end;
    ```

    We can now execute sql_code.sql within a python script as follows:

    ```python
    import sqlalchemy as db

    engine = db.create_engine("...")

    path_to_sql_file = "<some_directory>/sql_code.sql"

    with engine.connect() as conn:
        conn.execute(
            db.text(
                convert_sql_to_string(path_to_sql_file)
            )
        )
        conn.commit()
    ```
    """
    lines = []
    with open(filepath, 'r') as f:
        for line in f.readlines():
            lines.append(line.strip())
    sql_code = ""
    for line in lines:
        sql_code += line
        sql_code += " "
    return sql_code


def transaction_chain(
    trans_type, 
    no_investments,
    dates
    ):
    """
    This function will produce a chain of transactions for either a long or 
    short position. For a long position, there can never be more shares sold 
    than bought and for a short position, there will never be more shares 
    returned than borrowed.

    Parameters
    --------------------------------------------------
    trans_type: float
        Indicates whether the transaction is for a short or long position.
        1.0 indicates a long and -1.0 indicates a short.

    Returns
    --------------------------------------------------
    trans_history: list
        Returns a list of tuples of the form (d, a, s) where d is the date 
        of the transaction, a is the action and is 1.0 for a buy or -1.0 for a 
        sell and s is the size of the transaction.
        
    """
    
    trans_dates = np.sort(
        np.random.choice(dates, no_investments, replace=False)
    )

    first_trans_size = np.random.randint(20, 500)
    
    # initialize the first transaction
    actions = [trans_type]
    trans_sizes = [first_trans_size]
    
    # position size must be positive for long and negative for short.
    # 0 means the position is closed
    trans_type_total = first_trans_size
    kill_total = 0

    for i in range(no_investments - 1):

        action = np.random.choice([1.0, -1.0])
        actions.append(action)

        trans_size = np.random.randint(1, first_trans_size)

        if action == trans_type:
            trans_type_total += trans_size
        else:
            kill_total += trans_size

        if kill_total > trans_type_total:
            trans_size = trans_type_total - (kill_total - trans_size)
            trans_sizes.append(trans_size)
            break

        trans_sizes.append(trans_size)

        if trans_type_total == kill_total:
            break
    
    trans_dates = trans_dates[: len(actions)]

    trans_history = [
        (d, a, s) for d, a, s in zip(trans_dates, actions, trans_sizes)
    ]

    return trans_history


class Debug:
    """
    A simple debugger that checks if the portfolio matches up with the 
    transaction_history after the fact.
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

    def debug_long(self, verbose=False, from_debug=False):
        bad_count = 0
        for user in self.users:
            query = "select ticker from portfolio "
            query += f"where user_id = {user} and position_type = 1"
            tickers = np.unique(pd.read_sql(query, self.engine).values)
            if verbose:
                print(f"Starting the long debug for user {user}")
                print(f"----------------------------------")
            for ticker in tickers:
                if verbose:
                    print(f"Debug for {ticker}")
                query = f"ticker == '{ticker}' "
                query += f"and user_id == {user}"
                query += "and position_type == 1"

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

    def debug_short(self, verbose=False, from_debug=False):
        bad_count = 0
        for user in self.users:
            query = "select ticker from portfolio "
            query += f"where user_id = {user} and position_type = -1"
            tickers = np.unique(pd.read_sql(query, self.engine).values)
            if verbose:
                print(f"Starting the short debug for user {user}")
                print(f"----------------------------------")
            for ticker in tickers:
                if verbose:
                    print(f"Debug for {ticker}")
                query = f"ticker == '{ticker}' "
                query += f"and user_id == {user}"
                query += "and position_type == -1"

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
        long = self.debug_long(from_debug=True)
        short = self.debug_short(from_debug=True)
        print(f"long errors {long} short error {short}")


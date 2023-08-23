import yfinance as yf
import datetime as dt

def get_prices(tickers, start, end):
    
    elapsed_time = (end - start).total_seconds()
    batch_time = 60 * 60 * 24 * 1

    dfs = [] 
    
    batch_no = 1
    while batch_no * batch_time < elapsed_time:

        df = yf.download(
            tickers=tickers,
            start=start + dt.timedelta(seconds = batch_time * (batch_no - 1)),
            end=min(
                start + dt.timedelta(seconds = batch_time * batch_no), 
                end
            ),
            interval='1m',
            prepost=True
        )

        df.index = df.index.to_series().apply(
            lambda x: str(x)[: -6]
        ).reset_index(drop=True)

        dfs.append(df)

        batch_no += 1
    
    return dfs

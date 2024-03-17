## Tables
The following tables will be generated after following the instructions below.


### ohlcv 
`show columns from ohlcv;` 

| Field     | Type       | Null | Key | Default | Extra |
|-----------|------------|------|-----|---------|-------|
| datetime  | datetime   | NO   | PRI | NULL    |       |
| ticker    | varchar(5) | NO   | PRI | NULL    |       |
| open      | float      | YES  |     | NULL    |       |
| high      | float      | YES  |     | NULL    |       |
| low       | float      | YES  |     | NULL    |       |
| close     | float      | YES  |     | NULL    |       |
| volume    | float      | YES  |     | NULL    |       |
| timestamp | int        | YES  |     | NULL    |       |


### transaction_history 
`show columns from transaction_history;` 

| Field         | Type       | Null | Key | Default | Extra          |
|---------------|------------|------|-----|---------|----------------|
| id            | int        | NO   | PRI | NULL    | auto_increment |
| datetime      | datetime   | YES  |     | NULL    |                |
| ticker        | varchar(6) | YES  |     | NULL    |                |
| position_type | int        | YES  |     | NULL    |                |
| action        | int        | YES  |     | NULL    |                |
| no_shares     | float      | YES  |     | NULL    |                |
| at_price      | float      | YES  |     | NULL    |                |


### portfolio 
`show columns from portfolio;` 

| Field           | Type       | Null | Key | Default | Extra |
|-----------------|------------|------|-----|---------|-------|
| ticker          | varchar(6) | NO   | PRI | NULL    |       |
| position_type   | int        | NO   | PRI | NULL    |       |
| position        | int        | YES  |     | NULL    |       |
| last_price      | float      | YES  |     | NULL    |       |
| cost_basis      | float      | YES  |     | NULL    |       |
| total_invested  | float      | YES  |     | NULL    |       |
| current_value   | float      | YES  |     | NULL    |       |
| realized_profit | float      | YES  |     | NULL    |       |
| gain            | float      | YES  |     | NULL    |       |


# How to use
* Make some instructions...


```python
import sqlalchemy as db
import pandas as pd
from dbgen.investor_returns import create

database_url = "dialect+driver://username:password@host:port/database"

engine = db.create_engine(
    database_url
)

create(engine)

# query from the database into a pandas dataframe
query = "select * from transaction_history"
trans_hist = pd.read_sql(query, engine)

query = "select * from portfolio"
portfolio = pd.read_sql(query, engine)
```

# A list of questions
1. ...

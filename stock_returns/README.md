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
from utils import engine_generator
from stock_returns.create import Create
from sqlalchemy.orm import declarative_base as Base
import pandas as pd

# Create the mysql engine
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
```

# A list of questions
1. ...

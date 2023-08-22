import sqlalchemy as db
from utils import engine_generator
from parents_and_children.create import Create
from sqlalchemy.orm import declarative_base as Base
import pandas as pd
import numpy as np

# mysql
engine = engine_generator(
    dialect='mysql', 
    driver='pymysql', 
    username='root', 
    host='127.0.0.1',
    port='3306',
    db='practice',
    unix_socket='/tmp/mysql.sock'
)

df = pd.read_sql('select child_id, parent1_id, parent2_id from children;', con=engine)

df.head()

x = df['parent1_id'].value_counts().sort_index()
y = df['parent2_id'].value_counts().sort_index()

pd.concat((x, y), axis=1).replace({np.nan: 0}).sum(axis=1).sort_index()

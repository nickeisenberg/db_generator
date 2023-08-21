import sqlalchemy as db
from utils import engine_generator
from parents_and_children.create import Create
from sqlalchemy.orm import declarative_base as Base

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

base = Base()
database = Create(engine=engine, base=base)
database.initialize(
        no_jobs=5,  # there are 15 possible jobs
        include_unemployed=True,  # this will include unemployment
        with_entries=True,  # populate the database with <no_parents> entries
        no_parents=500,  # number of parents
        no_children=600,  # number of children randomly assigned to parents
        drop_db_if_exists=True,  # will drop and recreate the database if exists
    )

import pandas as pd

df = pd.read_sql('select * from children;', con=engine)
df.shape

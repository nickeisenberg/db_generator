import sqlalchemy as db
from parents_and_children.create import Create
from sqlalchemy.orm import declarative_base as Base
import pandas as pd
from utils import engine_generator

dbname = 'parents_and_children'
engine = db.create_engine(
    f"mysql+pymysql://root:@127.0.0.1:3306/{dbname}?unix_socket=/tmp/mysql.sock"
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
        faker_seed=0,
        numpy_seed=0
    )

#--------------------------------------------------
# run some queries

query = 'select * from mailing;'
df = pd.read_sql(query, con=engine)
print(df)


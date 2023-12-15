import platform
import sqlalchemy as db
from sqlalchemy.orm import declarative_base as Base
import pandas as pd
from dbgen.parents_and_children import Create

# sqlite
path = "<path_to_where_you_want_you_sqlite.db_file>"
engine = db.create_engine(f'sqlite:///{path}')

database = Create(engine=engine)
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

query = 'select * from children where same_residence = 1'
df = pd.read_sql(query, con=engine)
print(df.head())

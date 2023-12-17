"""
sql alchemy recognizes \n an a new line and wont mess with the sql code.
Here is a little test. See ./test.sql for the code that will be exectuted with
sql alchemy.
"""


import sqlalchemy as db
from sqlalchemy.orm import declarative_base as Base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists, drop_database
import platform
import os


#--------------------------------------------------
# test table
#--------------------------------------------------
base = Base()

class TestTable(base):
    __tablename__ = "testtable"

    # user columns
    id = db.Column(
        db.Integer(), primary_key=True, autoincrement=True
    )
    name = db.Column(db.String(50))

    def __init__(self, name):
        self.name = name


#--------------------------------------------------
# init the engine
#--------------------------------------------------
p = os.environ['MYSQL_ROOT']
dbname = "testDB"
if platform.system() == 'Linux':
    engine = db.create_engine(
        f"mysql+pymysql://root:{p}@127.0.0.1:3306/{dbname}"
    )
else:    
    engine = db.create_engine(
        f"mysql+pymysql://root:{p}@127.0.0.1:3306/{dbname}?unix_socket=/tmp/mysql.sock"
    )


#--------------------------------------------------
# create the database
#--------------------------------------------------
create_database(engine.url)

#--------------------------------------------------
# create the tables
#--------------------------------------------------
base.metadata.create_all(bind=engine)

#--------------------------------------------------
# start a session
#--------------------------------------------------
session  = sessionmaker(bind=engine)()

for name in ["nick", "matt"]:
    entry = TestTable(name)
    session.add(entry)
session.commit()

sql_path = "/home/nicholas/GitRepos/DatabaseGenerator/tests/utils/sql_to_string/test.sql"

with open(sql_path, "r") as f:
    lines = f.read()

results = session.execute(db.text(lines))

for r in results:
    print(r)

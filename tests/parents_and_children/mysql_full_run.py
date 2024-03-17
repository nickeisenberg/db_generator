import platform
import sqlalchemy as db
import pandas as pd
import os
from dbgen.parents_and_children import create

p = os.environ['MYSQL_ROOT']
dbname = 'parents_and_children'
if platform.system() == 'Linux':
    engine = db.create_engine(
        f"mysql+pymysql://root:{p}@127.0.0.1:3306/{dbname}"
    )
else:    
    engine = db.create_engine(
        f"mysql+pymysql://root:{p}@127.0.0.1:3306/{dbname}?unix_socket=/tmp/mysql.sock"
    )

create(engine)

query = 'select * from children where same_residence = 1'
df = pd.read_sql(query, con=engine)
print(df.head())

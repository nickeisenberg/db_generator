import sqlalchemy as db
from utils import engine_generator
from parents_and_children.create import Create
from sqlalchemy.orm import declarative_base as Base
import pandas as pd
import numpy as np

engine = engine_generator(
    dialect='mysql', 
    driver='pymysql', 
    username='root', 
    host='127.0.0.1',
    port='3306',
    db='practice',
    unix_socket='/tmp/mysql.sock'
)

df = pd.read_sql('select * from employment;', con=engine)

avg_df = pd.pivot_table(df, values='salary', columns='job', aggfunc=np.mean).T.reset_index()

new = df.merge(avg_df, left_on='job', right_on='job', how='left')[['job', 'salary_x', 'salary_y']]
new['diff'] = new['salary_x'] - new['salary_y']
new = new.loc[new['diff'] < 0]

xx = df.groupby(by='job').count()['start_date']
yy = new.groupby(by='job').count()['diff']

yy / xx

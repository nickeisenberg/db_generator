## Tables
The following tables will be generated after following the instructions below.

### mailing
`show columns from mailing;` 

| Field      | Type         | Null | Key | Default | Extra          |
|------------|--------------|------|-----|---------|----------------|
| parent_id  | int          | NO   | PRI | NULL    | auto_increment |
| first_name | varchar(50)  | YES  |     | NULL    |                |
| last_name  | varchar(50)  | YES  |     | NULL    |                |
| address    | varchar(128) | YES  |     | NULL    |                |
| city       | varchar(128) | YES  |     | NULL    |                |
| state      | varchar(128) | YES  |     | NULL    |                |
| zip        | int          | YES  |     | NULL    |                |

### employment
`show columns from employment;` 

| Field      | Type        | Null | Key | Default | Extra          |
|------------|-------------|------|-----|---------|----------------|
| parent_id  | int         | NO   | PRI | NULL    | auto_increment |
| job        | varchar(50) | YES  |     | NULL    |                |
| salery     | int         | YES  |     | NULL    |                |
| start_date | varchar(10) | YES  |     | NULL    |                |

### finances
`show columns from finances;` 

| Field     | Type        | Null | Key | Default | Extra          |
|-----------|-------------|------|-----|---------|----------------|
| parent_id | int         | NO   | PRI | NULL    | auto_increment |
| bank_act  | varchar(20) | YES  |     | NULL    |                |
| savings   | int         | YES  |     | NULL    |                |

### children 
`show columns from children;` 

| Field          | Type        | Null | Key | Default | Extra          |
|----------------|-------------|------|-----|---------|----------------|
| child_id       | int         | NO   | PRI | NULL    | auto_increment |
| parent1_id     | int         | YES  |     | NULL    |                |
| parent2_id     | int         | YES  |     | NULL    |                |
| first_name     | varchar(50) | YES  |     | NULL    |                |
| last_name      | varchar(50) | YES  |     | NULL    |                |
| same_residence | tinyint(1)  | YES  |     | NULL    |                |
| is_student     | tinyint(1)  | YES  |     | NULL    |                |
| is_employed    | tinyint(1)  | YES  |     | NULL    |                |


## How to use
* First clone or fork the repo onto your machine.
* To create the database, you first need to create an engine.
See [this link](https://docs.sqlalchemy.org/en/20/core/engines.html) for
information on engines. There is also a function in the `utils.py` folder that
will create the engine for you. This function should work with postgresql, but
I have only tested it for mysql. If you want to use sqlite as you database,
the engine simply becomes `sqlalchemy.create_engine('sqlite:///<path_to_db>')`.
* After creating the engine, open a python script and run the following 
(the following is for mysql):

```python
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

# sqlite
path = "<path_to_where_you_want_you_sqlite.db_file>"
engine = db.create_engine(f'sqlite:///{path}')

base = Base()
database = Create(engine=engine, base=base)
database.initialize(
        no_jobs=5,  # there are 15 possible jobs
        include_unemployed=True,  # this will include unemployment
        with_entries=True,  # populate the database with <no_parents> entries
        no_parents=5,  # number of parents
        no_children=10,  # number of children randomly assigned to parents
        drop_db_if_exists=True,  # will drop and recreate the database if exists
    )
```

* The database is now generated. As a simple test, you can run the following:
```python
import pandas as pd

pd.read_sql("""select * from employment""", engine)
```

* Now use any database IDE to run queries. There are also some practice 
questions listed below.

# A list of questions
1. Find the average salaries of each of the professions
2. Within each profession, what is the percentage of people that make less than
the average.
3. Find the average saving for the employees that have worked for longer than
5 years
4. Find the top three employees in terms of savings within the job that has
the third-highest average salary. List the names of these employees.
5. Find the average number of children that employees have within each job.
6. Suppose that all of the childen who have jobs are earning %30 of what the
average of their parents are earning. List the names of the top three children
according to their salaries.
7. On average, do the children who live in the same residence as their parents
or the children who do not live with their parents earn more money. Again,
assume that the child's salary is 30% of what their parents earn.
8. List the average savings of the parents with no child, one child and more
than one child.
9. List the names of the family who have the highest combined income, again
assuming that if the child works, then he or she earns %30 of the average of
their parent's salary.
10. How many parents have children with different partners? There may be a case
where a parent has a None type parter. If this is the case, cosider "None" as
a partner.

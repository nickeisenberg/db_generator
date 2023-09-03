import pandas as pd
import sqlalchemy as db
from sqlalchemy.orm import declarative_base as Base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists, drop_database
import datetime as dt
import faker
import numpy as np
from parents_and_children.constants import JOBS, SALARY_AVG
from copy import deepcopy


_fk = faker.Faker()

_base = Base()


def Mailing(base):
    """
    This function takes a SQLAlchemy declarative_base and returns a SQLAlchemy 
    table/mapper. The mapper will take a persons name and address and map it 
    to a row in a table titled "mailing". The mapper contains 
    an underlying sqlalchemy metadata object that can be combined with 
    an engine to create the table if it does not exist. 
    See example usage below.
    
    Parameters
    --------------------------------------------------
    base : sqlalchemy.orm.declarative_base
        A declarative_base that will be inheirited by the underlying class
       

    Returns
    --------------------------------------------------
    _Mailing(base) : SQLAlchemy table/mapper class 
        _Mailing maps rows to a sql table names "mailing" under a 
        sqlalchemy.orm.sessionmaker setting. 

    Example Usage
    --------------------------------------------------
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.orm import declarative_base as Base
    
    base = Base()
    _Mailing = Mailing(base)

    engine = create_engine(...)

    if not database_exists(engine.url):
        create_database(engine.url) 
    base.metadata.create_all(bind=engine)

    session = sessionmaker(bind=engine)()

    entry = _Mailing(
        first_name, last_name, address, city, state, zip
    )

    session.add(entry)
    session.commit()
    session.close()
    """

    class _Mailing(base):
        # table name for User model
        __tablename__ = "mailing"
    
        # user columns
        parent_id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
        first_name = db.Column(db.String(50))
        last_name = db.Column(db.String(50))
        address = db.Column(db.String(128))
        city = db.Column(db.String(128))
        state = db.Column(db.String(128))
        zip = db.Column(db.Integer())
     
        def __init__(
            self,
            first_name,
            last_name,
            address,
            city,
            state,
            zip
        ):
            """
            Parameters
            --------------------------------------------------
            first_name : str
                The first name of the person.

            last_name : str
                The last name of the person.

            address : str
                The address of the person.

            city : str
                The city the person lives in.

            state : str
                The state the person lives in.

            zip : int 
                The zipcode the person lives in.

            Returns
            --------------------------------------------------
            A mapable object that can be sent to a sql table with the use of
            sqlalchemy.creat_engine and sqlalchemy.orm.sessionmaker. See 
            example usage in help(Mailing)
            """
            self.first_name = first_name
            self.last_name = last_name
            self.address = address
            self.city = city
            self.state = state
            self.zip = zip

    return _Mailing

def Employment(base):
    """
    This function takes a SQLAlchemy declarative_base and returns a SQLAlchemy 
    table/mapper. The mapper will take a persons employment info and map it 
    to a row in a table titled "employment". The mapper contains 
    an underlying sqlalchemy metadata object that can be combined with 
    an engine to create the table if it does not exist. 
    See example usage below.
    
    Parameters
    --------------------------------------------------
    base : sqlalchemy.orm.declarative_base
        A declarative_base that will be inheirited by the underlying class
       

    Returns
    --------------------------------------------------
    _Employment(base) : SQLAlchemy table/mapper class 
        _Employment(base) maps rows to a sql table names "mailing" under a 
        sqlalchemy.orm.sessionmaker setting. 

    Example Usage
    --------------------------------------------------
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.orm import declarative_base as Base
    
    base = Base()
    _Employment = Employment(base)

    engine = create_engine(...)

    if not database_exists(engine.url):
        create_database(engine.url) 
    base.metadata.create_all(bind=engine)

    session = sessionmaker(bind=engine)()

    entry = _Employment(
        job, salary, start_date
    )

    session.add(entry)
    session.commit()
    session.close()
    """

    class _Employment(base):
        # table name for User model
        __tablename__ = "employment"
    
        # user columns
        parent_id = db.Column(
            db.Integer(), primary_key=True, autoincrement=True
        )
        job = db.Column(db.String(50))
        salary = db.Column(db.Integer())
        start_date = db.Column(db.String(10))
     
        def __init__(self, salary, job, start_date):
            """
            Parameters
            --------------------------------------------------
            salary : str

            job : str

            start_date : str

            Returns
            --------------------------------------------------
            A mapable object that can be sent to a sql table with the use of
            sqlalchemy.creat_engine and sqlalchemy.orm.sessionmaker. See 
            example usage in help(Employment)
            """
            self.salary = salary
            self.job = job
            self.start_date = start_date

    return _Employment

def Finances(base):
    """
    This function takes a SQLAlchemy declarative_base and returns a SQLAlchemy 
    table/mapper. The mapper will take a persons financial info and map it 
    to a row in a table titled "finances". The mapper contains 
    an underlying sqlalchemy metadata object that can be combined with 
    an engine to create the table if it does not exist. 
    See example usage below.
    
    Parameters
    --------------------------------------------------
    base : sqlalchemy.orm.declarative_base
        A declarative_base that will be inheirited by the underlying class
       

    Returns
    --------------------------------------------------
    _Finances(base) : SQLAlchemy table/mapper class 
        _Finances(base) maps rows to a sql table names "finances" under a 
        sqlalchemy.orm.sessionmaker setting. 

    Example Usage
    --------------------------------------------------
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.orm import declarative_base as Base
    
    base = Base()
    _Finances = Finances(base)

    engine = create_engine(...)

    if not database_exists(engine.url):
        create_database(engine.url) 
    base.metadata.create_all(bind=engine)

    session = sessionmaker(bind=engine)()

    entry = _Employment(
        bank_act,
        savings
    )

    session.add(entry)
    session.commit()
    session.close()
    """

    class _Finances(base):
        # table name for User model
        __tablename__ = "finances"
    
        # user columns
        parent_id = db.Column(
            db.Integer(), primary_key=True, autoincrement=True
        )
        bank_act = db.Column(db.String(20))
        savings = db.Column(db.Integer())
     
        def __init__(self, bank_act, savings):
            """
            Parameters
            --------------------------------------------------
            bank_act : int

            savings : int

            Returns
            --------------------------------------------------
            A mapable object that can be sent to a sql table with the use of
            sqlalchemy.creat_engine and sqlalchemy.orm.sessionmaker. See 
            example usage in help(Finances)
            """
            self.bank_act = bank_act
            self.savings = savings

    return _Finances


def Children(base):
    """
    This function takes a SQLAlchemy declarative_base and returns a SQLAlchemy 
    table/mapper. The mapper will map info about a child 
    to a row in a table titled "children". The mapper contains 
    an underlying sqlalchemy metadata object that can be combined with 
    an engine to create the table if it does not exist. 
    See example usage below.
    
    Parameters
    --------------------------------------------------
    base : sqlalchemy.orm.declarative_base
        A declarative_base that will be inheirited by the underlying class
       

    Returns
    --------------------------------------------------
    _Children(base) : SQLAlchemy table/mapper class 
        _Children(base) maps rows to a sql table names "children" under a 
        sqlalchemy.orm.sessionmaker setting. 

    Example Usage
    --------------------------------------------------
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.orm import declarative_base as Base
    
    base = Base()
    _Children = Children(base)

    engine = create_engine(...)

    if not database_exists(engine.url):
        create_database(engine.url) 
    base.metadata.create_all(bind=engine)

    session = sessionmaker(bind=engine)()

    entry = _Children(
        parent1_id, 
        parent2_id, 
        first_name, 
        last_name, 
        same_residence, 
        is_student, 
        is_employed
    )

    session.add(entry)
    session.commit()
    session.close()
    """

    class _Children(base):
    
        __tablename__ = "children"

        child_id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
        parent1_id = db.Column(db.Integer())
        parent2_id = db.Column(db.Integer(), nullable=True)
        first_name = db.Column(db.String(50))
        last_name = db.Column(db.String(50))
        same_residence = db.Column(db.Boolean())
        is_student = db.Column(db.Boolean())
        is_employed = db.Column(db.Boolean())
    
        def __init__(
            self,
            parent1_id,
            parent2_id,
            first_name,
            last_name,
            same_residence,
            is_student,
            is_employed,
        ):
            """
            Parameters
            --------------------------------------------------
            parent1_id : int
                The key of the first parent. Assumes that a table is already
                created that has the name of this parent in it.

            parent2_id : int
                The key of the second parent. Assumes that a table is already
                created that has the name of this parent in it.

            first_name : str
                The first name of the child.

            last_name : str
                The last name of the child.

            same_residence : boolean
                A boolean indicating if the child lives at home with his or her
                parents.

            is_student : boolean
                A boolean indicating if the child is a student.

            is_employed : boolean
                A boolean indicating if the child is employed.

            Returns
            --------------------------------------------------
            A mapable object that can be sent to a sql table with the use of
            sqlalchemy.creat_engine and sqlalchemy.orm.sessionmaker. See 
            example usage in help(Finances)
            """
            self.parent1_id = parent1_id 
            self.parent2_id = parent2_id 
            self.first_name = first_name
            self.last_name = last_name
            self.same_residence = same_residence
            self.is_student = is_student
            self.is_employed = is_employed

    return _Children


class SalSavStartGen:
    """
    This class attempts to produce a relatively believable salary, savings and 
    startdate for a person based on the average salary of their job.

    The startdate is randomly selected inbetween Jan 1, 2000 and Aug 15, 2023.
    
    The work duration is calculated as the amount of time one has spent 
    at the job between the startdate and Aug 15, 2023.
    
    If the avg_sal > 0 then the generated salary is based off of the sum 
    of two componets 
        1) max(normal(avg_sal, avg_sal / 4), avg_sal / 10)
        2) max(np.random.gamma(.1, avg_sal) - np.random.gamma(.1, avg_sal), 0)
    (1) is basically the average base salary and is bounded below by a tenth
    of the average salary. (2) basically is random noise that is prodominately
    zero, however can occasionally become very large, which then introduces an 
    outlier salary for those who happend to be at the very top of their
    field.

    If the avg_sal > 0 then the savings is based off of the 
    work duration and salary and is generated to be 
        np.random.normal(
            salary * work_duration / 4,
            salary / 8,
        )
    If this value is negative then that is taken to mean that the person is in 
    debt.
    If the avg_sal = 0, ie the person is unemployed, then this persons savings
    is simulated to be
        np.random.gamma(.5, 50) - np.random.gamma(.1, 50)
    There is no particular reason that I chose this for the savings of a 
    person with no salary other than the fact that I thought the histogram 
    after simulated 1000 samples looked nice.


    Parameter
    --------------------------------------------------
    avg_sal : int
        The average salary for the position

    Attributes
    --------------------------------------------------
    salary : float
        The generated salary given the average salary 
    startdate : datetime
        The generated startdate randomly selected between Jan 1, 2000 and 
        Aug 15, 2023.
    savings : float
        The generated savings given the average salary 

    View the Distributions
    --------------------------------------------------
    import matplotlib.pyplot as plt
    import numpy as np
    from faker import Faker
    import datetime as dt
    from copy import deepcopy
    
    _fk = Faker()

    # Non-zero salary
    avg_sal=100
    sav = np.array([])
    sal = np.array([])
    for i in range(10000):
        salsav = SalSavStartGen(avg_sal)
        sav = np.append(sav, salsav.savings)
        sal = np.append(sal, salsav.salary)
    
    sal.mean()
    sal.max()
    sav.mean()
    sav.max()
    
    fig, ax = plt.subplots(1, 2)
    ax[0].hist(sav, bins=100)
    ax[1].hist(sal, bins=100)
    plt.show()
    
    # Zero salary
    
    avg_sal=0
    sav = np.array([])
    for i in range(10000):
        salsav = SalSavStartGen(avg_sal)
        sav = np.append(sav, salsav.savings)
    
    sav.mean()
    sav.max()
    
    plt.hist(sav, bins=100)
    plt.show()
    """

    def __init__(self, avg):
        self.salary = self._salary_generator(avg)
        self.startdate, self.work_duration = self._startdate_generator()
        self.savings = self._savings_generator()

    @staticmethod
    def _salary_generator(avg_sal):
        if avg_sal == 0:
            return 0
        else:
            sal_noise_r = np.random.gamma(.1, avg_sal)
            sal_noise_r -= np.random.gamma(.1, avg_sal)
            sal_noise_l = np.abs(np.random.normal(avg_sal, avg_sal / 4))
            if sal_noise_r > 0:
                return sal_noise_r + sal_noise_l
            else:
                return max(sal_noise_l, avg_sal / 10)
    
     
    def _savings_generator(self):
        if self.salary == 0:
            return np.random.gamma(.5, 50) - np.random.gamma(.1, 50)
        else:
            saving = np.random.normal(
                self.salary * self.work_duration / 4,
                self.salary / 8,
            )
            return saving
    
    @staticmethod
    def _startdate_generator(
        start=dt.datetime(2000, 1, 1),
        end=dt.datetime(2023, 8, 15),
    ):
        start_date = deepcopy(dt.datetime.strftime(
            _fk.date_between(start, end), '%Y-%m-%d'
        ))

        work_duration = end - dt.datetime.strptime(start_date, '%Y-%m-%d')
        work_duration = work_duration.days / 365

        return start_date, work_duration


class Create:

    def __init__(
        self,
        engine,
        base=_base,
        Mailing=Mailing,
        Employment=Employment,
        Finances=Finances,
        Children=Children
    ):
        self.engine = engine
        self.base = base
        self.Mailing = Mailing(base)
        self.Employment = Employment(base)
        self.Finances = Finances(base) 
        self.Children = Children(base)
        self._initialized = False

    """
    This class will be used to create the database, tables and populate the
    tables with real stock data that will be scrapped from yahoo using the 
    yfinance library. The class will also create some fake transactions for an 
    investory and store these transactions in a table named
    "transaction_history". The performance of these transactions will 
    automatically be calculated apon each insertioin of a new transaction by
    the use of a trigger. The performance of the portfolio will be tracked in 
    the "portfolio" table.

    Parameters
    --------------------------------------------------
    engine : sqlalchemy engine
        The engine connecting sqlalchemy to the database.
        
    base : sqlalchemy.orm.declarative_base, default _base = declarative_base()
        A default is set to a declarative_base().

    Mailing : default Mailing(base)

    Employment : default Employment(base)

    Finances : default Finances(base)

    Children : default Children(base)

    Methods
    --------------------------------------------------
    initialize
        Initializes the database and tables as well as populates the tables
        with fake data.

    Example Usage
    --------------------------------------------------
    import sqlalchemy as db
    from sqlalchemy.orm import declarative_base as Base
    import pandas as pd

    dialect="mysql",
    driver="pymysql",
    username="root",
    password="password",
    host="127.0.0.1",
    port="3306",
    db="parents_and_children",
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

    query = "select * from children where same_residence = True"
    trans_hist = pd.read_sql(query, engine)
    
    """


    def initialize(
            self,
        no_jobs=len(JOBS),
        include_unemployed=True,
        with_entries=True,
        drop_db_if_exists=True,
        no_parents=500,
        no_children=600,
        faker_seed=0,
        numpy_seed=0
    ):
        """
        This function will initialize the database, create the tables and then
        populate the tables with data.

        Parameters
        --------------------------------------------------
        no_jobs : int 1 - len(JOBS), Default len(JOBS)
            The number of jobs to select for simulating the fake data.

        include_unemployed : boolean, Default True
            Inclued "unemployed" as a job type.

        with_entries : boolean, Default True
            If  true, then initialize will generate fake data and populate
            all of the tables.

        drop_db_if_exists : boolean, Default True
            If  true, then initialize will drop the database if it exists and 
            then recreate it.

        no_parents : int, Default 500
            The number of generated parents.

        no_children : int, Default 600
            The number of generated children. The children may have not have
            a parent in the "mailing" table.

        faker_seed : int, Default 0
            The faker.Faker seed to allow for reproducability

        numpy_seed : int, Default 0
            The numpy.random seed to allow for reproducability

        returns:
            The function will create a database with name specified in the 
            engine which is inputed by the user. It will populate the database
            with four tables named "mailing", "children", "finances" and 
            "employment". The "mailing" table is essentially the list of all 
            possible parents to the children in the "children" table. The 
            "employment" table has the parents job with salary and the
            "finances" table has the parents savings amount. The salary,
            savings and start date for the job is generated using 
            the SalSavStartGen class, which generates believable salaries 
            given the average salary of the job that the person has.
        """

        np.random.seed(numpy_seed) 
        faker.Faker.seed(faker_seed)

        jobs = JOBS[: no_jobs]
        salary_avg = {j: SALARY_AVG[j] for j in jobs}

        if include_unemployed:
            jobs.append('unemployed')
            salary_avg['unemployed'] = 0

        if self._initialized:
          raise Exception("Database already initialized.")
        
        if drop_db_if_exists:
            if database_exists(self.engine.url):
                drop_database(self.engine.url) 

        if not database_exists(self.engine.url):
            create_database(self.engine.url) 

        self.base.metadata.create_all(bind=self.engine)

        self._initialized = True
        
        if not with_entries:
            return None

        session  = sessionmaker(bind=self.engine)()
        for i in range(no_parents):
            mailing = self.Mailing(
                _fk.first_name(),
                _fk.last_name(),
                _fk.street_address(),
                _fk.city(),
                _fk.state(),
                _fk.zipcode(),
            )
            session.add(mailing)

            job = np.random.choice(jobs)
            sal_sav = SalSavStartGen(salary_avg[job])
            employment = self.Employment(
                sal_sav.salary,
                job,
                sal_sav.startdate,
            )
            session.add(employment)

            finances = self.Finances(
                _fk.bban(),
                sal_sav.savings,
            )
            session.add(finances)
        
        count = 0
        p = np.exp(-np.arange(6) / 1.3)
        p /= p.sum()
        pairs = []
        while count < no_children:

            parent_ids = np.hstack((None, np.arange(1, no_parents)))
            
            parents = np.random.choice(parent_ids, replace=False, size=2)
            
            try:
                p1 = int(parents[0])
            except:
                p1 = None
            try:
                p2 = int(parents[1])
            except:
                p2 = None

            if p1 is None:
                p1, p2 = p2, p1

            if (p1, p2) in pairs:
                continue

            if (p2, p1) in pairs:
                continue

            pairs.append((p1, p2))

            amt = np.random.choice(np.arange(6), p=p)
            amt = amt if count + amt <= no_children else no_children - count
            count += amt
            for i in range(amt):
                child = self.Children(
                    parent1_id=p1,
                    parent2_id=p2,
                    first_name=_fk.first_name(),
                    last_name=_fk.last_name(),
                    same_residence=[False, True][np.random.binomial(1, .8)],
                    is_student=[False, True][np.random.binomial(1, .8)],
                    is_employed=[False, True][np.random.binomial(1, .6)]
                )
                session.add(child)

        session.commit()

        return None

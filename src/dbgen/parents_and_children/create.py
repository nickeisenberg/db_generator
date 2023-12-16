from sqlalchemy.orm import declarative_base as Base
from sqlalchemy.orm import sessionmaker
from sqlalchemy_utils import create_database, database_exists, drop_database
import faker
import numpy as np
from ._constants import JOBS, SALARY_AVG
from ._utils import SalSavStartGen
from ._tables import Mailing, Finances, Employment, Children


_fk = faker.Faker()

_base = Base()


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

        jobs = JOBS[: min(no_jobs, len(JOBS))]
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
        for _ in range(no_parents):
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
            sal_sav = SalSavStartGen(salary_avg[job] ,_fk)
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

            parent_ids = np.hstack((np.array([None]), np.arange(1, no_parents)))
            
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
            for _ in range(amt):
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

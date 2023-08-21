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
            self.first_name = first_name
            self.last_name = last_name
            self.address = address
            self.city = city
            self.state = state
            self.zip = zip

    return _Mailing

def Employment(base):

    class _Employment(base):
        # table name for User model
        __tablename__ = "employment"
    
        # user columns
        parent_id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
        job = db.Column(db.String(50))
        salary = db.Column(db.Integer())
        start_date = db.Column(db.String(10))
        # work_duration= db.Column(db.Float(4))
     
        def __init__(self, salary, job, start_date):  #, work_duration):
            self.salary = salary
            self.job = job
            self.start_date = start_date
            # self.work_duration = work_duration

    return _Employment

def Finances(base):

    class _Finances(base):
        # table name for User model
        __tablename__ = "finances"
    
        # user columns
        parent_id = db.Column(db.Integer(), primary_key=True, autoincrement=True)
        bank_act = db.Column(db.String(20))
        savings = db.Column(db.Integer())
     
        def __init__(self, bank_act, savings):
            self.bank_act = bank_act
            self.savings = savings

    return _Finances


def Children(base):

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
            self.parent1_id = parent1_id 
            self.parent2_id = parent2_id 
            self.first_name = first_name
            self.last_name = last_name
            self.same_residence = same_residence
            self.is_student = is_student
            self.is_employed = is_employed

    return _Children


class SalSavStartGen:

    def __init__(self, avg):
        self.salary = self.salary_generator(avg)
        self.startdate, self.work_duration = self.startdate_generator()
        self.savings = self.savings_generator()
    
    @staticmethod
    def salary_generator(avg_sal):
        if avg_sal == 0:
            return 0
        else:
            sal_noise = max(
                np.random.gamma(.1, avg_sal) - np.random.gamma(.1, avg_sal),
                -1. * avg_sal / 2
            )
            return avg_sal + sal_noise
    
     
    def savings_generator(self):
        if self.salary == 0:
            return np.random.gamma(.5, 50) - np.random.gamma(.1, 50)
        else:
            saving = np.random.normal(
                self.salary * self.work_duration / 4,
                self.salary / 8,
            )
            return saving
    
    @staticmethod
    def startdate_generator(
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


    def initialize(
            self,
        no_jobs=len(JOBS),
        include_unemployed=True,
        with_entries=True,
        drop_db_if_exists=True,
        no_parents=5,
        no_children=9
    ):

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
                # salary_generator(SALARY_AVG[job]),
                job,
                sal_sav.startdate,
                # startdate_generator(),
                # sal_sav.work_duration
            )
            session.add(employment)

            finances = self.Finances(
                _fk.bban(),
                sal_sav.savings,
                # savings_generator(SALARY_AVG[job]),
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

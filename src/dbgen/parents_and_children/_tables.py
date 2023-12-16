import sqlalchemy as db
from sqlalchemy.orm.decl_api import DeclarativeMeta


def Mailing(base) -> DeclarativeMeta:
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


def Employment(base) -> DeclarativeMeta:
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


def Finances(base) -> DeclarativeMeta:
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


def Children(base) -> DeclarativeMeta:
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

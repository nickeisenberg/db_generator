from ._tables import ParentsAndChildren

def create(engine,
           no_jobs=15,
           no_parents=500,
           no_children=600,
           faker_seed=0,
           numpy_seed=0,
           drop_db_if_exists=True):
    if not no_jobs in range(16):
        raise Exception("no_jobs must be an integer in the range of 0, ..., 15.")
    parents_and_children = ParentsAndChildren(engine)
    return parents_and_children.initialize(no_jobs=no_jobs,
                                           no_parents=no_parents,
                                           no_children=no_children,
                                           faker_seed=faker_seed,
                                           numpy_seed=numpy_seed,
                                           drop_db_if_exists=drop_db_if_exists)

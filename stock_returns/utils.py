def convert_sql_to_string(filepath):
    """
    This function will take a .sql file and output all if its contents into a 
    single string. The intention of this function to allow one to exectute 
    some sql code within a sqlalchemy setting. 
    For the code to be executed properly, comments should not be used 
    inside the .sql file. See example usage below.

    Parameters
    --------------------------------------------------
    filepath : str
        The path to the sql file.

    Returns
    --------------------------------------------------
    string

    Example Usage 
    --------------------------------------------------
    Suppose we have two tables, table_1 and table_2.
    Table_1 and Table_2 have the forms
    
         | id | t1_col1 | t1_col2    | id | t2_col1 | t2_col2
         |----| --------|--------    |----| --------|--------
         | 1  |   ...   |  ...       | 1  |   ...   |  ...   
         | 2  |   ...   |  ...       | 2  |   ...   |  ...   
    
    The following are the contents of the file named 'sql_code.sql'.

    ```sql
    create trigger
        example_trigger
    after insert on
        table_1
    for each row
    begin
    INSERT INTO
        table_2 (
            t2_col1,
            t2_col2
        )
    values 
        (
            new.t1_col1,
            new.t1_col2
        ) 
    ON DUPLICATE KEY UPDATE
        t2_col1 = t2_col1 + new.t1_col1,
        t2_col2 = t2_col2 + new.t1_col2
    end;
    ```

    We can now execute sql_code.sql within a python script as follows:

    ```python
    import sqlalchemy as db

    engine = db.create_engine("...")

    path_to_sql_file = "<some_directory>/sql_code.sql"

    with engine.connect() as conn:
        conn.execute(
            db.text(
                convert_sql_to_string(path_to_sql_file)
            )
        )
        conn.commit()
    ```
    """
    lines = []
    with open(filepath, 'r') as f:
        for line in f.readlines():
            lines.append(line.strip())
    sql_code = ""
    for line in lines:
        sql_code += line
        sql_code += " "
    return sql_code

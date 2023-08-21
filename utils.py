from sqlalchemy import create_engine

def engine_generator(
    dialect="",
    driver="",
    username="",
    password="",
    host="",
    port="",
    db="",
    unix_socket=""
):
    engine = f"{dialect}+{driver}://{username}:{password}@{host}:{port}/{db}"
    if unix_socket == "":
        return create_engine(engine)
    else:
        return create_engine(engine + f"?unix_socket={unix_socket}")
    

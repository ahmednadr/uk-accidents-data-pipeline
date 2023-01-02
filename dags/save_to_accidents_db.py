def save(path,
    username = "nader" ,
    password = "nader" ,
    ipaddress = "accidentsDB" ,
    port = 5432 ,
    dbname = "accidents"):

    # data_final_form.to_sql
    from sqlalchemy import create_engine
    import pandas as pd

    # Defining our connection variables
     
    # A long string that contains the necessary Postgres login information
    postgres_str = f'postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'
            
    # Create the connection
    cnx = create_engine(postgres_str)

    data_final_form = pd.read_parquet(path)

    data_final_form.to_sql("accidents_ready",con=cnx)
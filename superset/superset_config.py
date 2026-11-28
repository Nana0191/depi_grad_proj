import os
#we use this library to read the env variables
#and here we are making our connection string that will connect to the sql server
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://"
    f"{os.environ['SUPERSET_DB_USER']}:"
    f"{os.environ['SUPERSET_DB_PASSWORD']}@"
    f"superset-db:5432/"
    f"{os.environ['SUPERSET_DB_NAME']}"
)

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
#to be secure 
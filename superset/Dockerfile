FROM apache/superset:4.1.1

USER root
#the user as a root so we can install the packages 

RUN pip install --no-cache-dir pymssql psycopg2-binary 
#this is sqlalchemy library that connects the sql server with the superset interface 
#and the other library to connect the postgres database with the superset

USER superset
#return from the root

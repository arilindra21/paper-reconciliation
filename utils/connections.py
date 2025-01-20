import os
from askquinta import About_ArangoDB, About_BQ, About_MySQL

def call_arangodb():
    return About_ArangoDB(
        arango_url=os.getenv('ARANGO_URL'),
        username=os.getenv('ARANGO_USERNAME'),
        password=os.getenv('ARANGO_PASSWORD'),
    )

def call_bq():
    return About_BQ(
        project_id='paper-prod',
        credentials_loc='credential_bq.json',
        location='asia-southeast1'
    )

def call_mysql():
    return About_MySQL(
        host=os.getenv('MYSQL_HOST'),
        port=int(os.getenv('MYSQL_PORT')),
        username=os.getenv('MYSQL_USERNAME'),
        password=os.getenv('MYSQL_PASSWORD'),
        database_name=os.getenv('MYSQL_DATABASE')
    )

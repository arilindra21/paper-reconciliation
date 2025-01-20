from askquinta import About_ArangoDB, About_BQ, About_Gsheet, About_MySQL

def call_arangodb():
    return About_ArangoDB(
            arango_url='https://arangodb-replication.paper.id/',
            username='metabase',
            password='PaperMetabase#2021',
        )
def call_bq():
    #If environment variables are not set, you can set connection details manually
    return About_BQ(project_id = 'paper-prod',
                    credentials_loc = '/home/yogi/dags/credentials/credential_bq.json',
                    location = 'asia-southeast1')

def call_mysql():
    # Set up the About_MySQL object with environment variables if available
    return About_MySQL(
                        host = "34.101.80.240",
                        port = 14045,
                        username = "data_team_new",
                        password = "XmWcStOXkJQV9pGL2RhRx0VmW",
                        database_name='paper_invoicer')

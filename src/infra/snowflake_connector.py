from snowflake.snowpark import Session
from src.infra.aws_connector import get_secret_value
def get_snowpark_session() -> Session:
    snowflake_secrets = get_secret_value('SnowflakeSecrets')
    username = snowflake_secrets['username']
    password = snowflake_secrets['password']
    account = snowflake_secrets['account']
    role = snowflake_secrets['role']
    database = snowflake_secrets['database']
    schema = snowflake_secrets['schema']

    connection_parameters = {
        "account": account,
        "user": username,
        "password": password,
        "role": role,  # optional
        # "warehouse": "<your snowflake warehouse>",  # optional
        "database": database,  # optional
        "schema": schema,  # optional
    }

    session = Session.builder.configs(connection_parameters).create()

    return session
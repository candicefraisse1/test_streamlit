from snowflake.snowpark import Session
from src.infra.aws_connector import get_secret_value
import pandas as pd
import snowflake.snowpark as sp


class SnowflakeConnector:
    def __init__(self):
        self.session = self._get_snowpark_session()

    def _get_snowpark_session(self) -> Session:
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

    def get_df_from_sql_query(self, query: str) -> pd.DataFrame:
        df = self.session.sql(query).to_pandas()
        return df

    def execute_sql_query_in_snowflake(self, query: str):
        self.session.sql(query).collect()

    def overwrite_snowflake_table(self, df: pd.DataFrame, table_name: str):
        snowflake_dataframe = self.session.create_dataframe(df)
        snowflake_dataframe.write.mode("overwrite").save_as_table(table_name)

    def append_data_to_snowflake_table(self, df: pd.DataFrame, table_name: str):
        snowflake_dataframe = self.session.create_dataframe(df)
        snowflake_dataframe.write.mode("append").save_as_table(table_name)



import pandas as pd

from src.entity.kpi_gim_debit_pointe_dataframe import KpiGimDebitPointeDataframe
from src.infra.snowflake_connector import SnowflakeConnector


class YearlyTableCreation():

    def __init__(self, snowflake_connector: SnowflakeConnector):
        self.snowflake_connector = snowflake_connector

    def add_kpi_gim_debit_pointe_data_to_snowflake_table(self, year):
        query = f"SELECT DISTINCT JOURNEE FROM KPI_GIM_DEBIT_POINTE WHERE JOURNEE LIKE '{year}%'"
        df = self.snowflake_connector.get_df_from_sql_query(query)
        if df.empty:
            list_of_rows = KpiGimDebitPointeDataframe.LIST_COMBINAISON_SITE_COMBUSTIBLE_UNITE.value
            day_list=pd.period_range(start=f'{year}-01-01', end=f'{year}-12-31').to_list()
            for day in day_list:
                df = pd.DataFrame(
                    list_of_rows,
                    columns=[
                        KpiGimDebitPointeDataframe.SITE.value,
                        KpiGimDebitPointeDataframe.COMBUSTIBLE.value,
                        KpiGimDebitPointeDataframe.UNITE.value
                    ]
                )
                day_string = day.strftime('%Y-%m-%d')
                df.insert(0, KpiGimDebitPointeDataframe.JOURNEE.value, day_string, allow_duplicates=True)
                df[KpiGimDebitPointeDataframe.VALEUR_PREVISIONNELLE.value] = 0
                df[KpiGimDebitPointeDataframe.VALEUR_REELLE.value] = 0
                df[KpiGimDebitPointeDataframe.VALEUR_CONSOLIDEE.value] = 0

                self.snowflake_connector.append_data_to_snowflake_table(df, KpiGimDebitPointeDataframe.SNOWFLAKE_TABLE_NAME.value)

# generate data 2023. Must not be run twice.
# if __name__ == '__main__':
#     sf_connector = SnowflakeConnector()
#     ytc = YearlyTableCreation(sf_connector)
#     ytc.add_kpi_gim_debit_pointe_data_to_snowflake_table(2023)
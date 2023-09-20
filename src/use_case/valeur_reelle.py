import streamlit as st

from datetime import date, timedelta

from src.entity.kpi_gim_debit_pointe_dataframe import KpiGimDebitPointeDataframe
from src.infra.snowflake_connector import SnowflakeConnector
from src.infra.streamlit_features import display_button_to_download_displayed_dataframe_into_csv


class ValeurReelle:

    def __init__(self, snowflake_connector: SnowflakeConnector):
        self.snowflake_connector = snowflake_connector

    def show_tab(self):

        previous_date_str = (date.today() - timedelta(1)).strftime('%Y-%m-%d')
        st.title(f"Saisie des données réelles pour le {previous_date_str}")

        with st.form("valeur_reelle_form"):
            query = f"select * from KPI_GIM_DEBIT_POINTE where JOURNEE = DATE('{previous_date_str}')"
            yesterday_df = self.snowflake_connector.get_df_from_sql_query(query)
            yesterday_df_updated = st.data_editor(
                yesterday_df,
                use_container_width=True,
                disabled=[
                    KpiGimDebitPointeDataframe.JOURNEE.value, KpiGimDebitPointeDataframe.SITE.value, KpiGimDebitPointeDataframe.COMBUSTIBLE.value, KpiGimDebitPointeDataframe.UNITE.value, "VALEUR_PREVISIONNELLE"
                ],
                hide_index=True,
                column_config={
                    KpiGimDebitPointeDataframe.VALEUR_CONSOLIDEE.value:None
                }
            )
            submit_valeur_reelle_button = st.form_submit_button("Submit")

        if submit_valeur_reelle_button:
            self.snowflake_connector.overwrite_snowflake_table(yesterday_df_updated, "TEMPORARY_VALEUR_REELLE")
            query = "UPDATE KPI_GIM_DEBIT_POINTE SET KPI_GIM_DEBIT_POINTE.VALEUR_REELLE=TEMPORARY_VALEUR_REELLE.VALEUR_REELLE FROM TEMPORARY_VALEUR_REELLE WHERE KPI_GIM_DEBIT_POINTE.JOURNEE=TEMPORARY_VALEUR_REELLE.JOURNEE AND KPI_GIM_DEBIT_POINTE.SITE=TEMPORARY_VALEUR_REELLE.SITE AND KPI_GIM_DEBIT_POINTE.COMBUSTIBLE=TEMPORARY_VALEUR_REELLE.COMBUSTIBLE"
            self.snowflake_connector.execute_sql_query_in_snowflake(query)
            st.info("data updated in snowflake")

        display_button_to_download_displayed_dataframe_into_csv(yesterday_df_updated, 'donnees_reelles_dispatcher.csv')
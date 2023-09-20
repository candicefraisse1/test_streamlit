import streamlit as st

from datetime import date, timedelta

from src.entity.kpi_gim_debit_pointe_dataframe import KpiGimDebitPointeDataframe
from src.infra.snowflake_connector import SnowflakeConnector
from src.infra.streamlit_features import display_button_to_download_displayed_dataframe_into_csv, \
    get_start_end_date_from_calendar_filter


class ValeurReelle:

    def __init__(self, snowflake_connector: SnowflakeConnector):
        self.snowflake_connector = snowflake_connector

    def show_tab(self):

        yesterday_date = date.today() - timedelta(1)
        min_date = yesterday_date - timedelta(days=30)
        max_date = yesterday_date + timedelta(days=30)

        st.title(f"Saisie des données réelles")

        date_tuple = get_start_end_date_from_calendar_filter(yesterday_date, min_date, max_date)
        start_date = date_tuple[0]
        end_date = date_tuple[-1]

        with st.form("valeur_reelle_form"):

            query = f"SELECT * FROM KPI_GIM_DEBIT_POINTE WHERE JOURNEE BETWEEN DATE('{start_date}') AND DATE('{end_date}')"
            df = self.snowflake_connector.get_df_from_sql_query(query)
            df_updated = st.data_editor(
                df,
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
            self.snowflake_connector.overwrite_snowflake_table(df_updated, "TEMPORARY_VALEUR_REELLE")
            query = "UPDATE KPI_GIM_DEBIT_POINTE SET KPI_GIM_DEBIT_POINTE.VALEUR_REELLE=TEMPORARY_VALEUR_REELLE.VALEUR_REELLE FROM TEMPORARY_VALEUR_REELLE WHERE KPI_GIM_DEBIT_POINTE.JOURNEE=TEMPORARY_VALEUR_REELLE.JOURNEE AND KPI_GIM_DEBIT_POINTE.SITE=TEMPORARY_VALEUR_REELLE.SITE AND KPI_GIM_DEBIT_POINTE.COMBUSTIBLE=TEMPORARY_VALEUR_REELLE.COMBUSTIBLE"
            self.snowflake_connector.execute_sql_query_in_snowflake(query)
            st.info("data updated in snowflake")

        display_button_to_download_displayed_dataframe_into_csv(df_updated, 'donnees_reelles_dispatcher.csv')
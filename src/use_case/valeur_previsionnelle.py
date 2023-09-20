import streamlit as st

from datetime import date

from src.entity.kpi_gim_debit_pointe_dataframe import KpiGimDebitPointeDataframe
from src.infra.snowflake_connector import SnowflakeConnector
from src.infra.streamlit_features import display_button_to_download_displayed_dataframe_into_csv

class ValeurPrevisionnelle:

    def __init__(self, snowflake_connector: SnowflakeConnector):
        self.snowflake_connector = snowflake_connector

    def show_tab(self):
        today_date_str = date.today().strftime('%Y-%m-%d')

        st.title(f"Saisie Prévisionnelle pour le {today_date_str}")

        chosen_date = today_date_str
        query = f"select * from KPI_GIM_DEBIT_POINTE where JOURNEE = DATE('{chosen_date}')"
        df_on_today_date = self.snowflake_connector.get_df_from_sql_query(query)

        with st.form("valeur_previsionnelle_form"):
            df_on_today_date = st.data_editor(
                df_on_today_date,
                use_container_width=True,
                disabled=[KpiGimDebitPointeDataframe.JOURNEE.value, KpiGimDebitPointeDataframe.SITE.value, KpiGimDebitPointeDataframe.COMBUSTIBLE.value, KpiGimDebitPointeDataframe.UNITE.value],
                hide_index=True,
                column_config={
                    KpiGimDebitPointeDataframe.VALEUR_REELLE.value:None,
                    KpiGimDebitPointeDataframe.VALEUR_CONSOLIDEE.value:None
                }
            )
            submitted = st.form_submit_button("Submit")


        if submitted:
            self.snowflake_connector.overwrite_snowflake_table(df_on_today_date, "TEMPORARY_VALEUR_PREVISIONNELLE")
            st.text("data entered")
            query = "UPDATE KPI_GIM_DEBIT_POINTE SET KPI_GIM_DEBIT_POINTE.VALEUR_PREVISIONNELLE=TEMPORARY_VALEUR_PREVISIONNELLE.VALEUR_PREVISIONNELLE, KPI_GIM_DEBIT_POINTE.VALEUR_REELLE=TEMPORARY_VALEUR_PREVISIONNELLE.VALEUR_PREVISIONNELLE FROM TEMPORARY_VALEUR_PREVISIONNELLE WHERE KPI_GIM_DEBIT_POINTE.JOURNEE=TEMPORARY_VALEUR_PREVISIONNELLE.JOURNEE AND KPI_GIM_DEBIT_POINTE.SITE=TEMPORARY_VALEUR_PREVISIONNELLE.SITE AND KPI_GIM_DEBIT_POINTE.COMBUSTIBLE=TEMPORARY_VALEUR_PREVISIONNELLE.COMBUSTIBLE"
            self.snowflake_connector.execute_sql_query_in_snowflake(query)
            st.info("data updated in snowflake")

        display_button_to_download_displayed_dataframe_into_csv(df_on_today_date, "donnes_previsionnelles_du_jour_dispatcher.csv")

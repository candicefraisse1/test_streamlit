from datetime import date, timedelta

import streamlit as st

from src.entity.kpi_gim_debit_pointe_dataframe import KpiGimDebitPointeDataframe
from src.infra.snowflake_connector import SnowflakeConnector
from src.infra.streamlit_features import get_start_end_date_from_calendar_filter


class ValeurConsolidee:

    def __init__(self, snowflake_connector: SnowflakeConnector):
        self.snowflake_connector = snowflake_connector

    def show_tab(self):
        yesterday_date = date.today() - timedelta(1)
        current_year = date.today().year
        min_date = date(current_year,1,1)

        st.title(f"Contrôle de cohérence: validation des valeurs réelles")

        date_tuple = get_start_end_date_from_calendar_filter(yesterday_date, min_date, yesterday_date)
        start_date = date_tuple[0]
        end_date = date_tuple[-1]

        with st.form("valeur_consolidee_form"):
            query = f"SELECT * FROM KPI_GIM_DEBIT_POINTE WHERE JOURNEE BETWEEN DATE('{start_date}') AND DATE('{end_date}')"
            valeur_a_consolider_df = self.snowflake_connector.get_df_from_sql_query(query)
            valeur_a_consolider_df = st.data_editor(
                valeur_a_consolider_df,
                use_container_width=True,
                disabled=[KpiGimDebitPointeDataframe.JOURNEE.value, KpiGimDebitPointeDataframe.SITE.value, KpiGimDebitPointeDataframe.COMBUSTIBLE.value, KpiGimDebitPointeDataframe.UNITE.value, "VALEUR_PREVISIONNELLE", KpiGimDebitPointeDataframe.VALEUR_REELLE.value],
                hide_index=True
            )
            submit_valeur_consolidee_button = st.form_submit_button("Submit")

        if submit_valeur_consolidee_button:
            self.snowflake_connector.overwrite_snowflake_table(valeur_a_consolider_df, "TEMPORARY_VALEUR_CONSOLIDEE")
            query = "UPDATE KPI_GIM_DEBIT_POINTE SET KPI_GIM_DEBIT_POINTE.VALEUR_CONSOLIDEE=TEMPORARY_VALEUR_CONSOLIDEE.VALEUR_CONSOLIDEE FROM TEMPORARY_VALEUR_CONSOLIDEE WHERE KPI_GIM_DEBIT_POINTE.JOURNEE=TEMPORARY_VALEUR_CONSOLIDEE.JOURNEE AND KPI_GIM_DEBIT_POINTE.SITE=TEMPORARY_VALEUR_CONSOLIDEE.SITE AND KPI_GIM_DEBIT_POINTE.COMBUSTIBLE=TEMPORARY_VALEUR_CONSOLIDEE.COMBUSTIBLE"
            self.snowflake_connector.execute_sql_query_in_snowflake(query)
            st.info("data updated in snowflake, click refresh to refresh the page")

            formbtn = st.button("Refresh")

            if "formbtn_state" not in st.session_state:
                st.session_state.formbtn_state = False

            if formbtn or st.session_state.formbtn_state:
                st.session_state.formbtn_state = True
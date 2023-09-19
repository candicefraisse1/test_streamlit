import streamlit as st
import pandas as pd
import numpy as np
import json
import snowflake.connector
import datetime
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from src.infra.aws_connector import get_secret_value
import plotly.express as px
import plotly.figure_factory as ff
from datetime import date, timedelta

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
    "role": role, # optional
    # "warehouse": "<your snowflake warehouse>",  # optional
    "database": database,  # optional
    "schema": schema,  # optional
  }  

session = Session.builder.configs(connection_parameters).create()


st.set_page_config(page_title="Données debit pointes", layout="wide")
left, middle, right = st.columns(
    (1.5, 1, 1)
)
with middle:
    st.image('c-logo.svg')


tab1, tab2, tab3, tab4, tab5 = st.tabs(["Saisie Prévisionnelle", "Saisie des données réelles", "Contrôle de cohérence", "Insertion des données", "Exploration des données"])

with tab1:
    today_date_str = date.today().strftime('%Y-%m-%d')

    st.title(f"Saisie Prévisionnelle pour le {today_date_str}")

    chosen_date = today_date_str
    df_on_today_date = session.sql(f"select * from KPI_GIM_DEBIT_POINTE where JOURNEE = DATE('{chosen_date}')").to_pandas()

    # initialisation des données du jour si elles n'existent pas encore
    if df_on_today_date.empty:
        list_of_rows = [
            ["SAINT_OUEN_1", "GAZ", "tv"],
            ["SAINT_OUEN_2", "CHARBON_ET_BOIS", "tv"],
            ["SAINT_OUEN_3", "GAZ_AA", "tv"],
            ["SAINT_OUEN_3", "GAZ_RS","tv"],
            ["SAINT_OUEN_3", "GAZ_PC","tv"],
            ["BERCY", "GAZ", "tv"],
            ["BERCY", "BIOGAZ", "tv"],
            ["BERCY", "BIO_COMB_LIQUIDE", "tv"],
            ["GRENELLE", "GAZ", "tv"],
            ["GRENELLE", "BIOGAZ", "tv"],
            ["GRENELLE", "BIO_COMB_LIQUIDE","tv"],
            ["VAUGIGARD", "GAZ", "tv"],
            ["VAUGIGARD", "BIOGAZ","tv"],
            ["IVRY", "GAZ","tv"],
            ["IVRY", "BIOGAZ","tv"],
            ["KB", "GAZ", "tv"],
            ["VITRY", "GAZ_AA", "tv"],
            ["VITRY", "GAZ_RS", "tv"],
            ["VITRY", "GAZ_PC", "tv"],
            ["SALPETRIERE", "GAZ","tv"],
            ["SYCTOM_IP13", "OM_IP13","tv"],
            ["SYCTOM_ISSEANE", "OM_ISSEANE","tv"],
            ["SYCTOM_ST_OUEN", "OM_ST_OUEN","tv"]
        ]
        df_on_today_date = pd.DataFrame(
            list_of_rows,
            columns=["SITE", "COMBUSTIBLE", "UNITE"]
        )

        df_on_today_date.insert(0, "JOURNEE", chosen_date, allow_duplicates=True)
        df_on_today_date["VALEUR_PREVISIONNELLE"] = 0
        df_on_today_date["VALEUR_REELLE"] = 0
        df_on_today_date["VALEUR_CONSOLIDEE"] = False

        snowflake_df = session.create_dataframe(df_on_today_date)
        snowflake_df.write.mode("append").save_as_table("KPI_GIM_DEBIT_POINTE")

    with st.form("valeur_previsionnelle_form"):
        edited_valeur_previsionnelle_df = st.data_editor(
            df_on_today_date,
            use_container_width=True,
            disabled=["JOURNEE", "SITE", "COMBUSTIBLE", "UNITE"],
            hide_index=True,
            column_config={
                "VALEUR_REELLE":None,
                "VALEUR_CONSOLIDEE":None
            }
        )
        submitted = st.form_submit_button("Submit")

    if submitted:
        snowflake_df = session.create_dataframe(edited_valeur_previsionnelle_df)
        snowflake_df.write.mode("overwrite").save_as_table("TEMPORARY_VALEUR_PREVISIONNELLE")
        query = "UPDATE KPI_GIM_DEBIT_POINTE SET KPI_GIM_DEBIT_POINTE.VALEUR_PREVISIONNELLE=TEMPORARY_VALEUR_PREVISIONNELLE.VALEUR_PREVISIONNELLE, KPI_GIM_DEBIT_POINTE.VALEUR_REELLE=TEMPORARY_VALEUR_PREVISIONNELLE.VALEUR_PREVISIONNELLE FROM TEMPORARY_VALEUR_PREVISIONNELLE WHERE KPI_GIM_DEBIT_POINTE.JOURNEE=TEMPORARY_VALEUR_PREVISIONNELLE.JOURNEE AND KPI_GIM_DEBIT_POINTE.SITE=TEMPORARY_VALEUR_PREVISIONNELLE.SITE AND KPI_GIM_DEBIT_POINTE.COMBUSTIBLE=TEMPORARY_VALEUR_PREVISIONNELLE.COMBUSTIBLE"
        session.sql(query).collect()
        st.info("data updated in snowflake")

    csv_df_donnees_previsionnelles = edited_valeur_previsionnelle_df.to_csv().encode('utf-8')

    st.download_button(
        label="Download data as CSV",
        data=csv_df_donnees_previsionnelles,
        file_name='donnees_previsionnelles_du_jour_dispatcher.csv',
        mime='text/csv',
    )


with tab2:
    previous_date_str = (date.today() - timedelta(1)).strftime('%Y-%m-%d')
    st.title(f"Saisie des données réelles pour le {previous_date_str}")

    with st.form("valeur_reelle_form"):
        yesterday_df = session.sql(f"select * from KPI_GIM_DEBIT_POINTE where JOURNEE = DATE('{previous_date_str}')").to_pandas()
        yesterday_df_updated = st.data_editor(
            yesterday_df,
            use_container_width=True,
            disabled=["JOURNEE", "SITE", "COMBUSTIBLE", "UNITE", "VALEUR_PREVISIONNELLE"],
            hide_index=True,
            column_config={
                "VALEUR_CONSOLIDEE":None
            }
        )
        submit_valeur_reelle_button = st.form_submit_button("Submit")

    if submit_valeur_reelle_button:
        snowflake_df = session.create_dataframe(yesterday_df_updated)
        snowflake_df.write.mode("overwrite").save_as_table("TEMPORARY_VALEUR_REELLE")
        query = "UPDATE KPI_GIM_DEBIT_POINTE SET KPI_GIM_DEBIT_POINTE.VALEUR_REELLE=TEMPORARY_VALEUR_REELLE.VALEUR_REELLE FROM TEMPORARY_VALEUR_REELLE WHERE KPI_GIM_DEBIT_POINTE.JOURNEE=TEMPORARY_VALEUR_REELLE.JOURNEE AND KPI_GIM_DEBIT_POINTE.SITE=TEMPORARY_VALEUR_REELLE.SITE AND KPI_GIM_DEBIT_POINTE.COMBUSTIBLE=TEMPORARY_VALEUR_REELLE.COMBUSTIBLE"
        session.sql(query).collect()
        st.info("data updated in snowflake")

    csv_df_donnes_reelles = yesterday_df_updated.to_csv().encode('utf-8')

    st.download_button(
        label="Download data as CSV",
        data=csv_df_donnes_reelles,
        file_name='donnees_reelles_dispatcher.csv',
        mime='text/csv',
    )

with tab3:
    st.title(f"Contrôle de cohérence: validation des valeurs réelles")

    with st.form("valeur_CONSOLIDEE_form"):
        valeur_a_CONSOLIDEEr_df = session.sql(f"select * from KPI_GIM_DEBIT_POINTE where VALEUR_CONSOLIDEE = FALSE and JOURNEE IS DISTINCT FROM DATE('{today_date_str}')").to_pandas()
        valeur_a_CONSOLIDEEr_df = st.data_editor(
            valeur_a_CONSOLIDEEr_df,
            use_container_width=True,
            disabled=["JOURNEE", "SITE", "COMBUSTIBLE", "UNITE", "VALEUR_PREVISIONNELLE", "VALEUR_REELLE"],
            hide_index=True
        )
        submit_valeur_CONSOLIDEE_button = st.form_submit_button("Submit")

    if submit_valeur_CONSOLIDEE_button:
        snowflake_df = session.create_dataframe(valeur_a_CONSOLIDEEr_df)
        snowflake_df.write.mode("overwrite").save_as_table("TEMPORARY_VALEUR_CONSOLIDEE")
        query = "UPDATE KPI_GIM_DEBIT_POINTE SET KPI_GIM_DEBIT_POINTE.VALEUR_CONSOLIDEE=TEMPORARY_VALEUR_CONSOLIDEE.VALEUR_CONSOLIDEE FROM TEMPORARY_VALEUR_CONSOLIDEE WHERE KPI_GIM_DEBIT_POINTE.JOURNEE=TEMPORARY_VALEUR_CONSOLIDEE.JOURNEE AND KPI_GIM_DEBIT_POINTE.SITE=TEMPORARY_VALEUR_CONSOLIDEE.SITE AND KPI_GIM_DEBIT_POINTE.COMBUSTIBLE=TEMPORARY_VALEUR_CONSOLIDEE.COMBUSTIBLE"
        session.sql(query).collect()
        st.info("data updated in snowflake, click refresh to refresh the page")

        formbtn = st.button("Refresh")

        if "formbtn_state" not in st.session_state:
            st.session_state.formbtn_state = False

        if submit_valeur_CONSOLIDEE_button or st.session_state.formbtn_state:
            st.session_state.formbtn_state = True


with tab4:
    st.title("Insertion des données")
    # Get the current credentials
    with st.form("Debit pointe "):
        today = datetime.datetime.now()
        d = st.date_input("🗓️ Date", today)
        site_sb = st.selectbox(
            '🏭 Selectionnez un site',
            ('VITRY', 'SAINT OUEN', 'VAUGIRARD'))
        combustible_sb = st.selectbox(
            '⚡ Selectionnez un combustible',
            ('GAZ', 'BIO GAZ', 'BIO LIQUIDE'))
        valeur_text = st.number_input('🔢 Insérez une valeur numérique', step=3)
        ok_button = st.form_submit_button("Valider")
    df = session.sql("select * from DEBIT_POINTE")
    st.write(
        """Table mise à jour avec les dernières données 
        """
    )

    edited_df = st.data_editor(
        df,
        use_container_width=True,
        disabled=["JOURNEE", "SITE", "COMBUSTIBLE", "VALEUR"],
        hide_index=True
    )

    snowdf = session.create_dataframe(edited_df)
    valid_bt = st.button('Valider')

    if ok_button:
        query = "INSERT INTO DEBIT_POINTE (journee,site,combustible,valeur) VALUES ('{date}','{site}','{combustible}','{valeur}')".format(
            date=d, site=site_sb, combustible=combustible_sb, valeur=valeur_text)
        op = session.sql(query).collect()
        st.write(op)
        st.experimental_rerun()
    if valid_bt:
        snowdf.write.mode("overwrite").save_as_table("DEBIT_POINTE")
        st.experimental_rerun()


with tab5:
    st.title("Exploration des données des données")
    df = session.sql("select site,sum(valeur)as valeur from DEBIT_POINTE group by site")
    fig = px.bar(df, y='VALEUR', x='SITE',
                 color=['#91c8ab', '#1a985b', '#fec828'],
                 color_discrete_map="identity",
                 title='Histogramme montrant la production de combustibles par site')
    st.plotly_chart(fig)

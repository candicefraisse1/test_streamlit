import streamlit as st
import pandas as pd
import numpy as np
import json
import snowflake.connector
import datetime
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from aws_connector import get_secret_value
import plotly.express as px
import plotly.figure_factory as ff
from datetime import date


# # Get the credentials
# config_location = '.'
#
# config = json.loads(open(str(config_location+'/secrets.json')).read())
#
# username = config['secrets']['username']
# password = config['secrets']['password']
# account = config['secrets']['account']
# role = config['secrets']['role']
# database = config['secrets']['database']
# schema = config['secrets']['schema']

snowflake_secrets = get_secret_value('SnowflakeSecrets')
username = snowflake_secrets['username']
password = snowflake_secrets['password']
account = snowflake_secrets['account']
role = snowflake_secrets['role']
database = snowflake_secrets['database']
schema = snowflake_secrets['schema']

# ctx = snowflake.connector.connect( user=username, password=password, account=account)
# cs = ctx.cursor()
# try:
#     cs.execute("SELECT current_version()")
#     one_row = cs.fetchone()
#     print(one_row[0])
# finally:
#     cs.close()
# ctx.close()

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


tab1, tab2, tab3 = st.tabs(["Explo données", "Insertion données", "Validation"])

with tab1:
    df = session.sql("select site,sum(valeur)as valeur from DEBIT_POINTE group by site")
    fig = px.bar(df, y='VALEUR', x='SITE',
                 color=['#91c8ab', '#1a985b', '#fec828'],
                color_discrete_map="identity",
                 title='Histogramme montrant la production de combustibles par site')
    st.plotly_chart(fig)

    # pass

with tab2:
    st.title("Debit pointe ")
    st.write(
        """Exemple pour simuler le fichier excel sur sharepoint debit pointe 
        """
    )

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


with tab3:
    st.title("Interface de Validation")

    if ok_button:
        pass

    today_date_str = date.today().strftime('%Y-%m-%d')

    list_of_rows = [
        [today_date_str, "SAINT_OUEN_1", "GAZ", 0, 0],
        [today_date_str, "SAINT_OUEN_2", "CHARBON_ET_BOIS", 0, 0],
        [today_date_str, "SAINT_OUEN_3", "GAZ_AA", 0, 0],
        [today_date_str, "SAINT_OUEN_3", "GAZ_RS", 0, 0],
        [today_date_str, "SAINT_OUEN_3", "GAZ_PC", 0, 0],
        [today_date_str, "BERCY", "GAZ", 0, 0],
        [today_date_str, "BERCY", "BIOGAZ", 0, 0],
        [today_date_str, "BERCY", "BIO_COMB_LIQUIDE", 0, 0],
        [today_date_str, "GRENELLE", "GAZ", 0, 0],
        [today_date_str, "GRENELLE", "BIOGAZ", 0, 0],
        [today_date_str, "GRENELLE", "BIO_COMB_LIQUIDE", 0, 0],
        [today_date_str, "VAUGIGARD", "GAZ", 0, 0],
        [today_date_str, "VAUGIGARD", "BIOGAZ", 0, 0],
        [today_date_str, "IVRY", "GAZ", 0, 0],
        [today_date_str, "IVRY", "BIOGAZ", 0, 0],
        [today_date_str, "KB", "GAZ", 0, 0],
        [today_date_str, "VITRY", "GAZ_AA", 0, 0],
        [today_date_str, "VITRY", "GAZ_RS", 0, 0],
        [today_date_str, "VITRY", "GAZ_PC", 0, 0],
        [today_date_str, "SALPETRIERE", "GAZ", 0, 0],
        [today_date_str, "SYCTOM", "OM_IP13", 0, 0],
        [today_date_str, "SYCTOM", "OM_ISSEANE", 0, 0],
        [today_date_str, "SYCTOM", "OM_ST_OUEN", 0, 0],
    ]
    df = pd.DataFrame(
        list_of_rows,
        columns=["JOURNEE", "SITE", "COMBUSTIBLE", "VALEUR_PREVISIONNELLE", "VALEUR_REELLE"]
    )

    with st.form("my_form"):
        st.data_editor(
            df,
            use_container_width=True,
            disabled=["JOURNEE", "SITE", "COMBUSTIBLE", "VALEUR_PREVISIONNELLE"],
            hide_index=True
        )
        submitted = st.form_submit_button("Submit")

    if submitted:

        pass

    # if valid_bt:
    #     snowdf.write.mode("overwrite").save_as_table("DEBIT_POINTE")
    #     st.experimental_rerun()
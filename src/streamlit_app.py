import streamlit as st
import pandas as pd
import numpy as np
import json
import snowflake.connector
import datetime
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col
from src.aws_connector import get_secret_value

# Get the credentials
config_location = '.'

config = json.loads(open(str(config_location+'/secrets.json')).read())

username = config['secrets']['username']
password = config['secrets']['password']
account = config['secrets']['account']
role = config['secrets']['role']
database = config['secrets']['database']
schema = config['secrets']['schema']

# snowflake_secrets = get_secret_value('SnowflakeSecrets')
# username = snowflake_secrets['username']
# password = snowflake_secrets['password']
# account = snowflake_secrets['account']
# role = snowflake_secrets['role']
# database = snowflake_secrets['database']
# schema = snowflake_secrets['schema']

ctx = snowflake.connector.connect( user=username, password=password, account=account)
cs = ctx.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()
ctx.close()


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


tab1, tab2, tab3 = st.tabs(["Explo données", "Insertion données", "Validation"])

with tab1:
    df = session.sql("select site,combustible,sum(valeur)as valeur from DEBIT_POINTE group by site,combustible")
    st.bar_chart(df,
                 x='SITE',
                 y='VALEUR'
                 )
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
        d = st.date_input("Date", today)
        site_sb = st.selectbox(
            'Selectionnez un site ?',
            ('VITRY', 'SAINT OUEN', 'VAUGIRARD'))
        combustible_sb = st.selectbox(
            'Selectionnez un combustible ?',
            ('GAZ', 'BIO GAZ', 'BIO LIQUIDE'))
        valeur_text = st.number_input('inserez une valeur numérique')
        ok_button = st.form_submit_button("Valider")
    df = session.sql("select * from DEBIT_POINTE")
    col1, col2 = st.columns(2)
    col1.table(df.select(
        col("journee"), col("site"), col("combustible"), col("valeur")
    ))
    edited_df = col2.data_editor(df, use_container_width=True)
    # col2.table(df)
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

    with st.form("Debit "):
        today = datetime.datetime.now()
        site_sb = st.selectbox(
            'Selectionnez un site ?',
            ('VITRY', 'SAINT OUEN', 'VAUGIRARD'))
        combustible_sb = st.selectbox(
            'Selectionnez un combustible ?',
            ('GAZ', 'BIO GAZ', 'BIO LIQUIDE'))
        ok_button = st.form_submit_button("Valider")

    if ok_button:
        df = session.sql(f"select * from DEBIT_POINTE where site='{site_sb}' and combustible='{combustible_sb}'")
        st.data_editor(
            df,
            disabled=True
        )
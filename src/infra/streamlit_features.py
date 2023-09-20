import pandas as pd
import streamlit as st

def display_button_to_download_displayed_dataframe_into_csv(df: pd.DataFrame, file_name: str) -> st.download_button:
    csv_file = df.to_csv().encode('utf-8')
    st.download_button(
        label = "Download data as CSV",
        data = csv_file,
        file_name = file_name,
        mime = 'text/csv',
    )

from datetime import date

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

def get_start_end_date_from_calendar_filter(default_date: date, min_date: date, max_date: date) -> [date, date]:
    date_tuple = st.date_input(
        label="Choisissez une plage de date (la même date peut être sélectionnée deux fois)",
        value=(default_date, default_date),
        min_value=min_date,
        max_value=max_date,
        format='YYYY-MM-DD',
        disabled=False
    )
    return date_tuple
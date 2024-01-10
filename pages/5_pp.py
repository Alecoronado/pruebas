import streamlit as st
import pandas as pd
import numpy as np
import threading

# Configuración inicial
LOGGER = st.logger.get_logger(__name__)
_lock = threading.Lock()

# URLs de las hojas de Google Sheets
sheet_url_proyectos = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSHedheaRLyqnjwtsRvlBFFOnzhfarkFMoJ04chQbKZCBRZXh_2REE3cmsRC69GwsUK0PoOVv95xptX/pub?gid=2084477941&single=true&output=csv"
sheet_url_operaciones = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSHedheaRLyqnjwtsRvlBFFOnzhfarkFMoJ04chQbKZCBRZXh_2REE3cmsRC69GwsUK0PoOVv95xptX/pub?gid=1468153763&single=true&output=csv"
sheet_url_desembolsos = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSHedheaRLyqnjwtsRvlBFFOnzhfarkFMoJ04chQbKZCBRZXh_2REE3cmsRC69GwsUK0PoOVv95xptX/pub?gid=1657640798&single=true&output=csv"

st.title("Análisis de Desembolsos por Proyecto")

def load_data(url):
    with _lock:
        return pd.read_csv(url)

def convert_to_float(monto_str):
    try:
        monto_str = monto_str.replace('.', '').replace(',', '.')
        return float(monto_str)
    except ValueError:
        return np.nan

def process_data(df_proyectos, df_operaciones, df_operaciones_desembolsos, selected_countries):
    if selected_countries:
        df_operaciones = df_operaciones[df_operaciones['Pais'].isin(selected_countries)]

    df_proyectos = df_proyectos[['NoProyecto', 'IDAreaPrioritaria', 'IDAreaIntervencion']]
    df_operaciones = df_operaciones[['NoProyecto', 'NoOperacion', 'IDEtapa', 'Alias', 'Pais', 'FechaVigencia', 'Estado', 'AporteFONPLATAVigente']]
    df_operaciones_desembolsos = df_operaciones_desembolsos[['IDDesembolso', 'IDOperacion', 'Monto', 'FechaEfectiva']]

    df_operaciones_desembolsos['Monto'] = df_operaciones_desembolsos['Monto'].apply(convert_to_float)

    merged_df = pd.merge(df_operaciones_desembolsos, df_operaciones, left_on='IDOperacion', right_on='IDEtapa', how='left')
    merged_df = pd.merge(merged_df, df_proyectos, on='NoProyecto', how='left')

    merged_df['FechaEfectiva'] = pd.to_datetime(merged_df['FechaEfectiva'], dayfirst=True, errors='coerce')
    merged_df['FechaVigencia'] = pd.to_datetime(merged_df['FechaVigencia'], dayfirst=True, errors='coerce')
    merged_df['Ano'] = ((merged_df['FechaEfectiva'] - merged_df['FechaVigencia']).dt.days / 366).fillna(-1)
    merged_df['Ano'] = merged_df['Ano'].astype(int)

    merged_df['Porcentaje'] = ((merged_df['Monto'] / merged_df['AporteFONPLATAVigente']) * 100).round(2)

    merged_df['Monto'] = (merged_df['Monto'] / 1_000_000).round(3)

    return merged_df[merged_df['Ano'] >= 0]

def create_pivot_table(filtered_df, value_column):
    pivot_table = pd.pivot_table(filtered_df, values=value_column, index='IDEtapa', columns='Ano', aggfunc='sum', fill_value=0)
    pivot_table['Total'] = pivot_table.sum(axis=1)
    return pivot_table

df_proyectos = load_data(sheet_url_proyectos)
df_operaciones = load_data(sheet_url_operaciones)
df_operaciones_desembolsos = load_data(sheet_url_desembolsos)

unique_countries = df_operaciones['Pais'].unique().tolist()
selected_countries = st.multiselect('Seleccione Países', unique_countries, default=unique_countries)

processed_data = process_data(df_proyectos, df_operaciones, df_operaciones_desembolsos, selected_countries)

pivot_table_monto = create_pivot_table(processed_data, 'Monto')
st.write("Tabla Pivote de Monto de Desembolsos por Proyecto y Año")
st.dataframe(pivot_table_monto)

pivot_table_porcentaje = create_pivot_table(processed_data, 'Porcentaje')
st.write("Tabla Pivote de Porcentaje de Desembolsos por Proyecto y Año")
st.dataframe(pivot_table_porcentaje)



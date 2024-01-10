import streamlit as st
import pandas as pd
import numpy as np
import threading
import io
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Configuración inicial
LOGGER = st.logger.get_logger(__name__)
_lock = threading.Lock()

# URLs de las hojas de Google Sheets
sheet_url_proyectos = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSHedheaRLyqnjwtsRvlBFFOnzhfarkFMoJ04chQbKZCBRZXh_2REE3cmsRC69GwsUK0PoOVv95xptX/pub?gid=2084477941&single=true&output=csv"
sheet_url_operaciones = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSHedheaRLyqnjwtsRvlBFFOnzhfarkFMoJ04chQbKZCBRZXh_2REE3cmsRC69GwsUK0PoOVv95xptX/pub?gid=1468153763&single=true&output=csv"
sheet_url_desembolsos = "https://docs.google.com/spreadsheets/d/e/2PACX-1vSHedheaRLyqnjwtsRvlBFFOnzhfarkFMoJ04chQbKZCBRZXh_2REE3cmsRC69GwsUK0PoOVv95xptX/pub?gid=1657640798&single=true&output=csv"

# Inicializar la aplicación de Streamlit
st.title("Análisis de Desembolsos por Proyecto")

# Función para cargar los datos desde las hojas de Google Sheets
def load_data(url):
    with _lock:
        return pd.read_csv(url)

# Función para convertir el monto a un número flotante
def convert_to_float(monto_str):
    try:
        monto_str = monto_str.replace('.', '').replace(',', '.')
        return float(monto_str)
    except ValueError:
        return np.nan

# Función para procesar los datos
def process_data(df_proyectos, df_operaciones, df_operaciones_desembolsos):
    # Preparar los DataFrames seleccionando las columnas requeridas
    df_proyectos = df_proyectos[['NoProyecto', 'IDAreaPrioritaria', 'IDAreaIntervencion']]
    df_operaciones = df_operaciones[['NoProyecto', 'NoOperacion', 'IDEtapa', 'Alias', 'Pais', 'FechaVigencia', 'Estado', 'AporteFONPLATAVigente']]
    df_operaciones_desembolsos = df_operaciones_desembolsos[['IDDesembolso', 'IDOperacion', 'Monto', 'FechaEfectiva']]

    # Convertir 'Monto' a numérico
    df_operaciones_desembolsos['Monto'] = df_operaciones_desembolsos['Monto'].apply(convert_to_float)

    # Fusionar DataFrames con el método de fusión 'left' para 'OperacionesDesembolsos' y 'Operaciones'
    merged_df = pd.merge(df_operaciones_desembolsos, df_operaciones, left_on='IDOperacion', right_on='IDEtapa', how='left')

    # Fusionar el DataFrame resultante con 'Proyectos'
    merged_df = pd.merge(merged_df, df_proyectos, on='NoProyecto', how='left')

    # Convertir fechas y calcular años
    merged_df['FechaEfectiva'] = pd.to_datetime(merged_df['FechaEfectiva'], dayfirst=True, errors='coerce')
    merged_df['FechaVigencia'] = pd.to_datetime(merged_df['FechaVigencia'], dayfirst=True, errors='coerce')
    merged_df['Ano'] = ((merged_df['FechaEfectiva'] - merged_df['FechaVigencia']).dt.days / 366).fillna(-1)
    merged_df['Ano_FechaEfectiva'] = pd.to_datetime(merged_df['FechaEfectiva']).dt.year

    # Filtrar para mantener solo las filas con 'Ano' >= 0 y convertir 'Ano' a entero
    filtered_df = merged_df[merged_df['Ano'] >= 0]
    filtered_df['Ano'] = filtered_df['Ano'].astype(int)
    st.write(filtered_df)

    # Calcular Monto y Monto Acumulado para cada año
    df_monto_anual = filtered_df.groupby('Ano')["Monto"].sum().reset_index()
    df_monto_acumulado_anual = df_monto_anual['Monto'].cumsum()

    # Calcular Porcentaje del Monto de forma acumulativa
    aporte_total = filtered_df['AporteFONPLATAVigente'].iloc[0]  # Asume que AporteFonplata es constante
    df_porcentaje_monto_anual = (df_monto_anual['Monto'] / aporte_total * 100).round(2)
    df_porcentaje_monto_acumulado_anual = (df_monto_acumulado_anual / aporte_total * 100).round(2)

    # Crear DataFrame combinado para el cuadro de resumen
    combined_df = pd.DataFrame({
        'Ano': df_monto_anual['Ano'],
        'Monto': df_monto_anual['Monto'],
        'Monto Acumulado': df_monto_acumulado_anual,
        'Porcentaje del Monto': df_porcentaje_monto_anual,
        'Porcentaje del Monto Acumulado': df_porcentaje_monto_acumulado_anual
    })
    st.write(combined_df)

    # Función para crear la tabla pivote
def create_pivot_table(filtered_df):
    pivot_table = pd.pivot_table(filtered_df, values='Monto', index='IDEtapa', columns='Ano', aggfunc='sum', fill_value=0)
    pivot_table['Total'] = pivot_table.sum(axis=1)
    return pivot_table

# Llamada a las funciones de carga de datos
df_proyectos = load_data(sheet_url_proyectos)
df_operaciones = load_data(sheet_url_operaciones)
df_operaciones_desembolsos = load_data(sheet_url_desembolsos)

# Procesamiento de los datos
processed_data = process_data(df_proyectos, df_operaciones, df_operaciones_desembolsos)

# Creación de la tabla pivote
pivot_table = create_pivot_table(processed_data)

# Mostrar la tabla pivote en Streamlit
st.write("Tabla Pivote de Desembolsos por Proyecto y Año")
st.dataframe(pivot_table)


    


    

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
    df_operaciones_desembolsos = df_operaciones_desembolsos[['IDOperacion','IDDesembolso', 'Monto', 'FechaEfectiva']]
    
    # Convertir 'Monto' a numérico
    df_operaciones_desembolsos['Monto'] = df_operaciones_desembolsos['Monto'].apply(convert_to_float)

    # Fusionar DataFrames
    merged_df = pd.merge(df_operaciones_desembolsos, df_operaciones, left_on='IDOperacion', right_on='IDEtapa', how='inner')
    merged_df = pd.merge(merged_df, df_proyectos, on='NoProyecto', how='left')
    
    # Convertir fechas y calcular años
    merged_df['FechaEfectiva'] = pd.to_datetime(merged_df['FechaEfectiva'], dayfirst=True, errors='coerce')
    merged_df['FechaVigencia'] = pd.to_datetime(merged_df['FechaVigencia'], dayfirst=True, errors='coerce')
    merged_df['Ano'] = ((merged_df['FechaEfectiva'] - merged_df['FechaVigencia']).dt.days / 365).fillna(-1)
    merged_df['Ano_FechaEfectiva'] = pd.to_datetime(merged_df['FechaEfectiva']).dt.year
    filtered_df = merged_df[merged_df['Ano'] >= 0]
    filtered_df['Ano'] = filtered_df['Ano'].astype(int)
    filtered_df = pd.merge(filtered_df, df_operaciones[['IDEtapa']], on='IDEtapa', how='left')
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

    # Filtrar por países múltiples
    countries = filtered_df['Pais'].unique()
    selected_countries = st.multiselect('Selecciona Países:', countries, default=countries)
    filtered_df = combined_df[filtered_df['Pais'].isin(selected_countries)]

        # Configuración del formato de visualización de los DataFrame
    pd.options.display.float_format = '{:,.2f}'.format

    # Crear la tabla de Montos con años como columnas y IDEtapa como filas
    montos_pivot = filtered_df.pivot_table(
            index='IDEtapa', 
            columns='Ano', 
            values='Monto', 
            aggfunc='sum'
        ).fillna(0)

        # Convertir los montos a millones
    montos_pivot = (montos_pivot / 1_000_000).round(3)

        # Agregar la columna de totales al final de la tabla de Montos
    montos_pivot['Total'] = montos_pivot.sum(axis=1)

        # Crear la tabla de Porcentajes con años como columnas y IDEtapa como filas
    porcentaje_pivot = filtered_df.pivot_table(
            index='IDEtapa', 
            columns='Ano', 
            values='Porcentaje del Monto', 
            aggfunc='sum'
        ).fillna(0)

        # Redondear a dos decimales en el DataFrame de porcentajes
    porcentaje_pivot = porcentaje_pivot.round(2)

        # Agregar la columna de totales al final de la tabla de Porcentajes
    porcentaje_pivot['Total'] = porcentaje_pivot.sum(axis=1).round(0)






    # Crear diccionario para mapear IDEtapa a Alias
    etapa_to_alias = df_operaciones.set_index('IDEtapa')['Alias'].to_dict()
    filtered_df['IDEtapa'] = filtered_df['IDEtapa'].astype(str)
    filtered_df['IDEtapa_Alias'] = filtered_df['IDEtapa'].map(lambda x: f"{x} ({etapa_to_alias.get(x, '')})")

    # Selectbox para filtrar por IDEtapa
    unique_etapas_alias = filtered_df['IDEtapa_Alias'].unique()
    selected_etapa_alias = st.selectbox('Select IDEtapa to filter', unique_etapas_alias)
    selected_etapa = selected_etapa_alias.split(' ')[0]
    filtered_result_df = filtered_df[filtered_df['IDEtapa'] == selected_etapa]

    # Realizar cálculos
    result_df = filtered_result_df.groupby(['IDEtapa', 'Ano'])['Monto'].sum().reset_index()
    result_df['Monto Acumulado'] = result_df.groupby(['IDEtapa'])['Monto'].cumsum().reset_index(drop=True)
    result_df['Porcentaje del Monto'] = result_df.groupby(['IDEtapa'])['Monto'].apply(lambda x: x / x.sum() * 100).reset_index(drop=True)
    result_df['Porcentaje Acumulado'] = result_df.groupby(['IDEtapa'])['Monto Acumulado'].apply(lambda x: x / x.max() * 100).reset_index(drop=True)

    # Convertir 'Monto' y 'Monto Acumulado' a millones y redondear a 2 decimales
    result_df['Monto'] = (result_df['Monto'] / 1000000).round(2)
    result_df['Monto Acumulado'] = (result_df['Monto Acumulado'] / 1000000).round(2)

    return result_df

# Llamar a la función de procesamiento de datos
df_proyectos = load_data(sheet_url_proyectos)
df_operaciones = load_data(sheet_url_operaciones)
df_operaciones_desembolsos = load_data(sheet_url_desembolsos)

result_df= process_data(df_proyectos, df_operaciones, df_operaciones_desembolsos)

def calculate_matrices(dataframe):
    # Calcular Monto y Monto Acumulado para cada año
    df_monto_anual = dataframe.groupby('Ano')["Monto"].sum().reset_index()
    df_monto_acumulado_anual = df_monto_anual['Monto'].cumsum()

    # Calcular Porcentaje del Monto de forma acumulativa
    aporte_total = df_operaciones['AporteFONPLATAVigente'].iloc[0]  # Asume que AporteFONPLATAVigente es constante
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

    return combined_df



    

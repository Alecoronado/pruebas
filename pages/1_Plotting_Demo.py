import streamlit as st
import pandas as pd
import altair as alt
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
st.title("Aplicación de Preprocesamiento de Datos")

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
    df_operaciones_desembolsos = df_operaciones_desembolsos[['IDDesembolso', 'NoOperacion', 'Monto', 'FechaEfectiva']]
    
    # Convertir 'Monto' a numérico
    df_operaciones_desembolsos['Monto'] = df_operaciones_desembolsos['Monto'].apply(convert_to_float)

    # Fusionar DataFrames
    merged_df = pd.merge(df_operaciones_desembolsos, df_operaciones, on='NoOperacion', how='left')
    merged_df = pd.merge(merged_df, df_proyectos, on='NoProyecto', how='left')
    
    # Convertir fechas y calcular años
    merged_df['FechaEfectiva'] = pd.to_datetime(merged_df['FechaEfectiva'], dayfirst=True, errors='coerce')
    merged_df['FechaVigencia'] = pd.to_datetime(merged_df['FechaVigencia'], dayfirst=True, errors='coerce')
    merged_df['Ano'] = ((merged_df['FechaEfectiva'] - merged_df['FechaVigencia']).dt.days / 365).fillna(-1)
    merged_df['Ano_FechaEfectiva'] = pd.to_datetime(merged_df['FechaEfectiva']).dt.year
    filtered_df = merged_df[merged_df['Ano'] >= 0]
    filtered_df['Ano'] = filtered_df['Ano'].astype(int)
    st.write(filtered_df)

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
    
    # Realizar cálculos para result_df_ano_efectiva
    result_df_ano_efectiva = filtered_result_df.groupby(['IDEtapa', 'Ano_FechaEfectiva'])['Monto'].sum().reset_index()
    result_df_ano_efectiva['Monto Acumulado'] = result_df_ano_efectiva.groupby(['IDEtapa'])['Monto'].cumsum().reset_index(drop=True)
    result_df_ano_efectiva['Porcentaje del Monto'] = result_df_ano_efectiva.groupby(['IDEtapa'])['Monto'].apply(lambda x: x / x.sum() * 100).reset_index(drop=True)
    result_df_ano_efectiva['Porcentaje Acumulado'] = result_df_ano_efectiva.groupby(['IDEtapa'])['Monto Acumulado'].apply(lambda x: x / x.max() * 100).reset_index(drop=True)

    # Convertir 'Monto' y 'Monto Acumulado' a millones y redondear a 2 decimales para ambas tablas
    result_df['Monto'] = (result_df['Monto'] / 1000000).round(2)
    result_df['Monto Acumulado'] = (result_df['Monto Acumulado'] / 1000000).round(2)
    result_df_ano_efectiva['Monto'] = (result_df_ano_efectiva['Monto'] / 1000000).round(2)
    result_df_ano_efectiva['Monto Acumulado'] = (result_df_ano_efectiva['Monto Acumulado'] / 1000000).round(2)

    return result_df, result_df_ano_efectiva

# Función para convertir DataFrame a Excel
def dataframe_to_excel_bytes(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Resultados', index=False)
    output.seek(0)
    return output

# Función para crear una gráfica de líneas con etiquetas
def line_chart_with_labels(data, x_col, y_col, title, color):
    chart = alt.Chart(data).mark_line(point=True, color=color).encode(
        x=alt.X(f'{x_col}:O', axis=alt.Axis(title='Año', labelAngle=0)),
        y=alt.Y(f'{y_col}:Q', axis=alt.Axis(title=y_col)),
        tooltip=[x_col, y_col]
    ).properties(
        title=title,
        width=600,
        height=400
    )
    text = chart.mark_text(
        align='left',
        baseline='middle',
        dx=18,
        dy=-18
    ).encode(
        text=alt.Text(f'{y_col}:Q', format='.2f')
    )
    return chart + text

#Funcion
def run():
    # Cargar y procesar los datos
    df_proyectos = load_data(sheet_url_proyectos)
    df_operaciones = load_data(sheet_url_operaciones)
    df_operaciones_desembolsos = load_data(sheet_url_desembolsos)
    result_df, result_df_ano_efectiva = process_data(df_proyectos, df_operaciones, df_operaciones_desembolsos)

    # Define los colores para cada gráfico
    color_monto = 'steelblue'
    color_acumulado = 'goldenrod'
    color_porcentaje = 'salmon'

    # Crear y mostrar gráficos para result_df
    chart_monto = line_chart_with_labels(result_df, 'Ano', 'Monto', 'Monto por Año en Millones', color_monto)
    chart_monto_acumulado = line_chart_with_labels(result_df, 'Ano', 'Monto Acumulado', 'Monto Acumulado por Año en Millones', color_acumulado)
    chart_porcentaje_acumulado = line_chart_with_labels(result_df, 'Ano', 'Porcentaje Acumulado', 'Porcentaje Acumulado del Monto por Año', color_porcentaje)

    st.altair_chart(chart_monto, use_container_width=True)
    st.altair_chart(chart_monto_acumulado, use_container_width=True)
    st.altair_chart(chart_porcentaje_acumulado, use_container_width=True)
    
    # Mostrar la tabla "Tabla por Año"
    st.write("Tabla por Año:", result_df)
  
    # Crear y mostrar gráficos para result_df_ano_efectiva
    chart_monto_efectiva = line_chart_with_labels(result_df_ano_efectiva, 'Ano_FechaEfectiva', 'Monto', 'Monto por Año de Fecha Efectiva en Millones', color_monto)
    chart_monto_acumulado_efectiva = line_chart_with_labels(result_df_ano_efectiva, 'Ano_FechaEfectiva', 'Monto Acumulado', 'Monto Acumulado por Año de Fecha Efectiva en Millones', color_acumulado)
    chart_porcentaje_acumulado_efectiva = line_chart_with_labels(result_df_ano_efectiva, 'Ano_FechaEfectiva', 'Porcentaje Acumulado', 'Porcentaje Acumulado del Monto por Año de Fecha Efectiva', color_porcentaje)

    st.altair_chart(chart_monto_efectiva, use_container_width=True)
    st.altair_chart(chart_monto_acumulado_efectiva, use_container_width=True)
    st.altair_chart(chart_porcentaje_acumulado_efectiva, use_container_width=True)

    # Mostrar la tabla "Tabla por Año de Fecha Efectiva"
    st.write("Tabla por Año de Fecha Efectiva:", result_df_ano_efectiva)

if __name__ == "__main__":
    run()

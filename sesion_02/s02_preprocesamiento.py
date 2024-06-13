# -*- coding: utf-8 -*-

# Instalar el conector de MySQL
! pip install mysql-connector-python

# Importar SQLAlchemy para trabajar con bases de datos y ORM
from sqlalchemy import create_engine

import pandas as pd
import numpy as np

import datetime
from datetime import datetime

# Montar Google Drive en el entorno de Colab para acceder
# a archivos almacenados en la unidad de Google Drive
from google.colab import drive
drive.mount('/content/drive')

# Definir la ruta del directorio donde se encuentran los archivos
ruta = '/content/drive/MyDrive/intersem_FES_PACD-2024-II/'

# [01] Cargar el archivo CSV en un DataFrame de pandas
df_csv = pd.read_csv(
    ruta + '20240603_2005_PREP/20240603_2005_PREP_PRES/PRES_2024.csv',
    low_memory=False, # Evitar la carga en trozos pequeños para mejorar el rendimiento
    header=4) # Comenzar a leer desde la línea 4 para saltar las cabeceras no deseadas

# Limpiar columnas específicas de caracteres no deseados
df_csv['CLAVE_CASILLA'] = df_csv['CLAVE_CASILLA'].str.replace("'", "")
df_csv['CLAVE_ACTA'] = df_csv['CLAVE_ACTA'].str.replace("'", "")

# Crear una copia del DataFrame original
df_original = df_csv.copy

# Mostrar el DataFrame
df_csv

# Visualización de los tipos de datos del DataFrame
df_csv.dtypes

# Selección y visualización de la columna 'FECHA_HORA_ACOPIO'
df_csv['FECHA_HORA_ACOPIO']

# Asignación de la columna 'FECHA_HORA_ACOPIO' a una serie
s_FECHA_HORA_ACOPIO = df_csv['FECHA_HORA_ACOPIO']
s_FECHA_HORA_ACOPIO[0]

# [01] Convertir las columnas de fechas a objetos datetime
# si hay algún error, se convierte en valor nulo

df_csv['FECHA_HORA_ACOPIO'] = \
pd.to_datetime(df_csv['FECHA_HORA_ACOPIO'], format='%d/%m/%Y %H:%M:%S', errors='coerce')

df_csv['FECHA_HORA_CAPTURA'] = \
pd.to_datetime(df_csv['FECHA_HORA_CAPTURA'], format='%d/%m/%Y %H:%M:%S', errors='coerce')

df_csv['FECHA_HORA_VERIFICACION'] = \
pd.to_datetime(df_csv['FECHA_HORA_VERIFICACION'], format='%d/%m/%Y %H:%M:%S', errors='coerce')

# Revisión de los tipos de datos después de la conversión
df_csv.dtypes

# Revisión de valores nulos en todo el DataFrame
df_csv.isnull()

# Contar el número de valores nulos por columna
df_csv.isnull().sum()

# Revisión de valores nulos en la columna 'ID_DISTRITO_FEDERAL'
df_csv['ID_DISTRITO_FEDERAL'].isnull()

# Contar el número de valores nulos en la columna 'ID_DISTRITO_FEDERAL'
df_csv['ID_DISTRITO_FEDERAL'].isnull().sum()

# Detección de filas duplicadas
df_csv.duplicated()

# Contar el número de filas duplicadas
df_csv.duplicated().sum()

# Selección de las filas duplicadas para su revisión
df_duplicados = df_csv[df_csv.duplicated()]
df_duplicados

# Revisión de los valores únicos en la columna 'NULOS'
df_csv['NULOS'].unique()

# [1.1] Revisión de los valores únicos para columnas categóricas específicas

columnas_categoricas = ['TIPO_CASILLA',
                        'EXT_CONTIGUA',
                        'TIPO_ACTA',
                        'ENTIDAD',
                        'ORIGEN',
                        'DIGITALIZACION',
                        'TIPO_DOCUMENTO',
                        'COTEJADA',
                        'MECANISMOS_TRASLADO']

for columna in columnas_categoricas:

  print(f"Valores unicos para {columna}")
  print(df_csv[columna].unique())
  print()

# Selección de filas con valores nulos en 'ID_DISTRITO_FEDERAL'
df_csv[df_csv['ID_DISTRITO_FEDERAL'].isnull()]

# Selección de filas sin valores nulos en 'ID_DISTRITO_FEDERAL'
df_csv[df_csv['ID_DISTRITO_FEDERAL'].notnull()]

# [02] Rellenar valores nulos con 0 y cambiar el tipo de datos a enteros

df_csv['ID_DISTRITO_FEDERAL'] = df_csv['ID_DISTRITO_FEDERAL'].fillna(0)
df_csv['SECCION'] = df_csv['SECCION'].fillna(0)
df_csv['ID_CASILLA'] = df_csv['ID_CASILLA'].fillna(0)

df_csv['ID_DISTRITO_FEDERAL'] = df_csv['ID_DISTRITO_FEDERAL'].astype(np.int64)
df_csv['SECCION'] = df_csv['SECCION'].astype(np.int64)
df_csv['ID_CASILLA'] = df_csv['ID_CASILLA'].astype(np.int64)

# Revisión de los tipos de datos después de la conversión
df_csv.dtypes

# Visualización del DataFrame actualizado
df_csv

# Revisión de filas con valores nulos en 'ID_DISTRITO_FEDERAL'
df_csv[df_csv['ID_DISTRITO_FEDERAL'].isnull()]

# Visualización de todas las columnas del DataFrame
df_csv.columns

# [2.1] Determinación de filas con más del 50% de valores nulos

limite_renglon = len(df_csv.columns) * 0.5
print(limite_renglon)

renglones_mas_50_nulos = df_csv[df_csv.isnull().sum(axis=1) > limite_renglon]
renglones_mas_50_nulos

# [2.1] Determinación de columnas con más del 50% de valores nulos

limite_columna = len(df_csv) * 0.5
print(limite_columna)

columnas_mas_50_nulos = df_csv.columns[df_csv.isnull().sum(axis=0) > limite_columna]
columnas_mas_50_nulos

# [2.2] Conteo de valores específicos ('Ilegible', 'Sin dato', '-')

valores_contar = ['Ilegible', 'Sin dato', '-']
df_conteo_sin_dato = pd.DataFrame()

for valor in valores_contar:

  conteo_valor = df_csv.apply(lambda x: x[x == valor].count())
  df_conteo_sin_dato[valor] = conteo_valor

df_conteo_sin_dato

# Revisión de los tipos de datos después de la conversión
df_csv.dtypes

# [03] Reemplazo de valores específicos ('Ilegible', 'Sin dato', '-') con NaN y conversión a tipo numérico

valores_reemplazar = ['Ilegible', 'Sin dato']

# Conteo de valores específicos antes de reemplazar
conteo_valores = {
    'Ilegible' : df_csv.apply(lambda x: (x == 'Ilegible').sum()),
    'Sin dato' : df_csv.apply(lambda x: (x == 'Sin dato').sum())
}
conteo_df = pd.DataFrame(conteo_valores)

# Identificación de columnas que contienen los valores específicos
columnas_convertir = conteo_df[(conteo_df["Ilegible"] > 0) | (conteo_df["Sin dato"] > 0)].index.tolist()
columnas_convertir

# Reemplazo de valores específicos con NaN

# Primera forma
valores_reemplazar = ['Ilegible', 'Sin dato', '-']
df_csv[columnas_convertir].replace(valores_reemplazar, np.nan, inplace=True)

# Segunda forma
valores_reemplazar = ['Ilegible', 'Sin dato', '-']
df_csv[columnas_convertir] = df_csv[columnas_convertir].replace(valores_reemplazar, np.nan)

# Conversión de las columnas a tipo numérico
df_csv[columnas_convertir] = df_csv[columnas_convertir].\
apply(pd.to_numeric, errors='coerce').astype(pd.Int64Dtype())

# Revisión de los tipos de datos después de la conversión
df_csv.dtypes

# Cálculo de diferencias de tiempo entre fechas en minutos

df_csv['DIF_ACOPIO_CAPTURA'] = \
np.round((df_csv['FECHA_HORA_CAPTURA'] - df_csv['FECHA_HORA_ACOPIO']).dt.total_seconds() / 60)

df_csv['DIF_CAPTURA_VERIFICACION'] = \
np.round((df_csv['FECHA_HORA_VERIFICACION'] - df_csv['FECHA_HORA_CAPTURA']).dt.total_seconds() / 60)

df_csv['DURACION_TOTAL'] = \
np.round((df_csv['FECHA_HORA_VERIFICACION'] - df_csv['FECHA_HORA_ACOPIO']).dt.total_seconds() / 60)

# Conversión de las diferencias de tiempo a horas

df_csv['DIF_ACOPIO_CAPTURA_HORAS'] = \
np.round(df_csv['DIF_ACOPIO_CAPTURA'] / 60, 2)

df_csv['DIF_CAPTURA_VERIFICACION_HORAS'] = \
np.round(df_csv['DIF_CAPTURA_VERIFICACION'] / 60, 2)

df_csv['DURACION_TOTAL_HORAS'] = \
np.round(df_csv['DURACION_TOTAL'] / 60, 2)

# Identificación de procesos rápidos (menos de 60 minutos)

umbral_minutos = 60

df_csv['ACOPIO_CAPTURA_RAPIDO'] = (df_csv['DIF_ACOPIO_CAPTURA'] <= umbral_minutos).astype(int)
df_csv['CAPTURA_VERIFICACION_RAPIDO'] = (df_csv['DIF_CAPTURA_VERIFICACION'] <= umbral_minutos).astype(int)
df_csv['DURACION_TOTAL_RAPIDO'] = (df_csv['DURACION_TOTAL'] <= umbral_minutos).astype(int)

# Visualización del DataFrame actualizado con las nuevas columnas
df_csv

# Información adicional sobre el alumno y registro de fecha y hora

alumno = "ANGEL PEREZ RODRIGUEZ RODRIGUEZ"

df_conteo_sin_dato.reset_index(inplace=True)
df_conteo_sin_dato.rename(columns={'index':'columna'}, inplace=True)

df_conteo_sin_dato['fecha_hora_actual'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
df_conteo_sin_dato['alumno'] = alumno

df_conteo_sin_dato

# Guardar el DataFrame en diferentes formatos
import os

# Definición de la ruta donde se guardarán los archivos procesados
ruta_guardar = ruta + "20240603_2005_PREP/procesado/"
nombre_archivo = 'PRES_PROCESADO_2024'

# Creación del directorio si no existe
os.makedirs(ruta_guardar, exist_ok=True)
print(ruta_guardar)

# Guardar el DataFrame en un archivo CSV
df_csv.to_csv(ruta_guardar + nombre_archivo + '.csv', index=False)

# Guardar el DataFrame en un archivo Excel
df_csv.to_excel(ruta_guardar + nombre_archivo + '.xlsx', index=False, sheet_name=nombre_archivo)

# Guardar el DataFrame en un archivo JSON
df_csv.to_json(ruta_guardar + nombre_archivo + '.json', orient='records')

# Importación de la biblioteca pickle para guardar objetos en archivos binarios
import pickle

# Guardar el DataFrame en un archivo pickle
with open(os.path.join(ruta_guardar, nombre_archivo + '.pkl'), 'wb') as archivo:
  pickle.dump(df_csv, archivo)

# Definición del nombre de la tabla en la base de datos, reemplazando espacios por guiones bajos
nombre_tabla = f"PACD_{alumno}".replace(" ", "_")

# Información de conexión a la base de datos MySQL
user='usabierto02'
password='datos21%'
host='bd.arcelia.net'
database='datosabiertos'
puerto = '3306'
auth = 'mysql_native_password'

# Cadena de conexión para SQLAlchemy
str_conn = f'mysql+mysqlconnector://{user}:{password}@{host}:{puerto}/{database}?auth_plugin={auth}'
engine = create_engine(str_conn)

# Guardar el DataFrame de conteo de valores sin dato en la base de datos MySQL
df_conteo_sin_dato.to_sql(nombre_tabla, con=engine, if_exists='replace', index=False)
print(f"Datos guardados en la tabla {nombre_tabla} de MySQL")
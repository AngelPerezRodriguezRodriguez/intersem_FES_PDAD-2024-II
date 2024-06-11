# -*- coding: utf-8 -*-

import pandas as pd

# Montar Google Drive en el entorno de Colab para acceder
# a archivos almacenados en la unidad de Google Drive
from google.colab import drive
drive.mount('/content/drive')

# Definir la ruta del directorio donde se encuentran los archivos
ruta = '/content/drive/MyDrive/intersem_FES_PACD-2024-II/'

# Listar archivos en el directorio especificado para verificar su existencia y ubicación
! ls -la /content/drive/MyDrive/intersem_FES_PACD-2024-II/20240603_2005_PREP

# Mostrar las primeras líneas del archivo CSV para inspección preliminar
! head /content/drive/MyDrive/intersem_FES_PACD-2024-II/20240603_2005_PREP/20240603_2005_PREP_PRES/PRES_2024.csv

# [01] Cargar el archivo CSV en un DataFrame de pandas
df_csv = pd.read_csv(
    ruta + '20240603_2005_PREP/20240603_2005_PREP_PRES/PRES_2024.csv',
    low_memory=False, # Evitar la carga en trozos pequeños para mejorar el rendimiento
    header=4) # Comenzar a leer desde la línea 4 para saltar las cabeceras no deseadas

# Mostrar el DataFrame cargado
df_csv

# Limpiar columnas específicas de caracteres no deseados
df_csv['CLAVE_CASILLA'] = df_csv['CLAVE_CASILLA'].str.replace("'", "")
df_csv['CLAVE_ACTA'] = df_csv['CLAVE_ACTA'].str.replace("'", "")

# Mostrar el DataFrame después de la limpieza
df_csv

# Mostrar los tipos de datos de cada columna en el DataFrame
df_csv.dtypes

# [02] Leer un DataFrame desde una conexión directa con MySQL

# Instalar el conector de MySQL
! pip install mysql-connector-python

import mysql.connector

# Importar SQLAlchemy para trabajar con bases de datos y ORM
from sqlalchemy import create_engine, text

import datetime

# Definir credenciales de conexión a la base de datos
usuario = 'usabierto02'
passwd = 'datos21%'
host = 'bd.arcelia.net'
base_datos = 'datosabiertos'

# Intentar establecer la conexión con la base de datos
try:

  coneccion = mysql.connector.connect(
      user=usuario,
      password=passwd,
      host=host,
      database=base_datos)

except mysql.connector.Error as err:

  print(f"Error: {err}")

# Definir una función para ejecutar consultas SQL
def executeSQL(consulta_sql, coneccion, parametros=None):

  # El cursor es una estructura de control para el recorrido
  # de los registros del resultado de una consulta
  cursor = None

  try:

    # Usar cursor con buffering para lecturas más rápidas
    cursor = coneccion.cursor(buffered=True)

    # Ejecutar la consulta SQL
    cursor.execute(consulta_sql, parametros)

    # Obtener todos los resultados
    recordset = cursor.fetchall()

    # Obtener nombres de columnas
    columnas = [desc[0] for desc in cursor.description]

    return recordset, columnas

  except mysql.connector.Error as err:

    print(f"Error: {err}")
    return None, None

  finally:

    if cursor:
      cursor.close()

# Mostrar las tablas en la base de datos
recordset, columnas = executeSQL("SHOW TABLES;", coneccion)

print(columnas)

for registro in recordset:
  print(registro)

# Describir la tabla 'presidencia'
recordset, columnas = executeSQL("DESC presidencia;", coneccion)

print(columnas)

for registro in recordset:
  print(registro)

# Seleccionar los primeros 10 registros de la tabla 'presidencia'
recordset, columnas = executeSQL(
    "SELECT * FROM presidencia LIMIT 10;",
    coneccion)

print(columnas)

for registro in recordset:
  print(registro)

# Consulta para contar el número de casillas por entidad
consulta_sql = '''
SELECT entidad, COUNT(*) AS no_casillas
FROM presidencia
GROUP BY entidad
ORDER BY entidad;
'''

recordset, columnas = executeSQL(consulta_sql, coneccion)

print(columnas)

for registro in recordset:
  print(registro)

hora_ini = datetime.time(22,0,0)
hora_fin = datetime.time(22,59,59)

# Consulta para contar el número de casillas verificadas en un intervalo de tiempo específico
consulta_sql = f'''
SELECT entidad, COUNT(*) AS no_casillas
FROM presidencia
WHERE TIME(STR_TO_DATE(fecha_hora_verificacion, '%d/%m/%Y %H:%i:%s'))
BETWEEN '{hora_ini}' AND '{hora_fin}'
GROUP BY entidad
ORDER BY entidad;
'''

recordset, columnas = executeSQL(consulta_sql, coneccion)

print(columnas)

for registro in recordset:
  print(registro)

# Crear una tabla pivote con sumas de votos por partido y entidad
consulta_sql = """
SELECT ENTIDAD,
SUM(CAST(NULLIF(PAN, '') AS DECIMAL)) AS PAN,
SUM(CAST(NULLIF(PRI, '') AS DECIMAL)) AS PRI,
SUM(CAST(NULLIF(PRD, '') AS DECIMAL)) AS PRD,
SUM(CAST(NULLIF(PVEM, '') AS DECIMAL)) AS PVEM,
SUM(CAST(NULLIF(PT, '') AS DECIMAL)) AS PT,
SUM(CAST(NULLIF(MC, '') AS DECIMAL)) AS MC,
SUM(CAST(NULLIF(MORENA, '') AS DECIMAL)) AS MORENA,
SUM(CAST(NULLIF(PAN_PRI_PRD, '') AS DECIMAL)) AS PAN_PRI_PRD,
SUM(CAST(NULLIF(PAN_PRI, '') AS DECIMAL)) AS PAN_PRI,
SUM(CAST(NULLIF(PAN_PRD, '') AS DECIMAL)) AS PAN_PRD,
SUM(CAST(NULLIF(PRI_PRD, '') AS DECIMAL)) AS PRI_PRD,
SUM(CAST(NULLIF(PVEM_PT_MORENA, '') AS DECIMAL)) AS PVEM_PT_MORENA,
SUM(CAST(NULLIF(PVEM_PT, '') AS DECIMAL)) AS PVEM_PT,
SUM(CAST(NULLIF(PVEM_MORENA, '') AS DECIMAL)) AS PVEM_MORENA,
SUM(CAST(NULLIF(PT_MORENA, '') AS DECIMAL)) AS PT_MORENA,
SUM(CAST(NULLIF(NO_REGISTRADAS, '') AS DECIMAL)) AS NO_REGISTRADAS,
SUM(CAST(NULLIF(NULOS, '') AS DECIMAL)) AS NULOS
FROM presidencia
GROUP BY entidad
ORDER BY entidad;
"""

recordset, columnas = executeSQL(consulta_sql, coneccion)

print(columnas)

for registro in recordset:
  print(registro)

# [03] Cargar el resultado de la consulta SQL en un DataFrame de pandas
df_mysql = pd.read_sql_query(sql=consulta_sql, con=coneccion)
df_mysql.dtypes

df_mysql

# [04] Leer un DataFrame desde una conexión a MySQL
# utilizando un nivel de abstracción superior con SQLAlchemy

puerto = '3306'
auth = 'mysql_native_password'

str_coneccion = f'mysql+mysqlconnector://{usuario}:{passwd}@{host}:{puerto}/{base_datos}?auth_plugin={auth}'

consulta_sql = '''SELECT nulos, COUNT(*) AS numero
FROM presidencia
WHERE entidad = 'CAMPECHE'
GROUP BY nulos
ORDER BY 1;
'''

conneccion_alchemy = create_engine(str_coneccion)
print(conneccion_alchemy)

# Ejecutar la consulta SQL y mostrar resultados usando SQLAlchemy
with conneccion_alchemy.connect() as coneccion:

  # Ejecutar la consulta SQL
  cursor = coneccion.execute(text(consulta_sql))

  # Obtener todos los resultados
  registros = cursor.fetchall()

  # Obtener nombres de columnas
  columnas = cursor.keys()

  for registro in registros:
    print(dict(zip(columnas, registro)))

# [05] Leer un DataFrame desde una conexión directa con MongoDB

# Instalar pymongo para trabajar con MongoDB
# incluyendo soporte para el protocolo de conexión 'srv'
! pip install "pymongo[srv]"

import pymongo

# Definir la URI de conexión a MongoDB
uri = "mongodb://fesalu:nosqlfes123@cluster0-shard-00-00.8pyt4.mongodb.net:27017,cluster0-shard-00-01.8pyt4.mongodb.net:27017,cluster0-shard-00-02.8pyt4.mongodb.net:27017/?ssl=true&replicaSet=atlas-nok9e6-shard-0&authSource=admin&retryWrites=true&w=majority"

# Conectar al cliente de MongoDB
mongo_client = pymongo.MongoClient(uri)
mongo_client

# Mostrar las bases de datos disponibles en MongoDB
mongo_client.list_database_names()

# Seleccionar la base de datos 'presidencia'
db = mongo_client[base_datos]
db

# Mostrar las colecciones en la base de datos 'presidencia'
db.list_collection_names()

# Contar el número de documentos en la colección 'presidencia'
db['presidencia'].count_documents({})

# Seleccionar y mostrar los primeros 10 documentos de la colección 'presidencia'
documentos = db['presidencia'].find({}).limit(10)

print(documentos)

for documento in documentos:
  print(documento)
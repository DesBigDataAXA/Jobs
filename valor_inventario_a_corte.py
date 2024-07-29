import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import  timedelta
from sqlalchemy  import create_engine, text
import datetime
import sys
import traceback
from logBi import logBi
from logException import logException

import os
from dotenv import load_dotenv
load_dotenv()

print('Inicio del proceso')

# Función para parsear la tabla HTML
def parse_html_table(html):
   print('Inicia Proceso para parsear el HTML')
   soup = BeautifulSoup(html, 'html.parser')
   table = soup.find('table')
   datos = pd.read_html(str(table))[0]
   df = pd.DataFrame(datos)
   
   df.columns = [
                  'location_id',
                  'location', 
                  'on_hand',
                  'inventory_value', 
                  'line',
                  'maker', 
                  'barcode', 
                  'article_id', 
                  'article', 
                  'negotiator',
                  'maker_id',
                  'maker_nit'
               ]

   df = df.iloc[1:]
   return df
# Función para validar los datos
def validate_data(df):

   print('Inicia Proceso para Validar el dataframe')
 
   df['on_hand'] = df['on_hand'].str.strip()
   df['on_hand'] = df['on_hand'].str.replace('=', '')
  
   df['inventory_value'] = df['inventory_value'].str.strip() 
   df['inventory_value'] = df['inventory_value'].str.replace('=', '')

   df['report_date'] =  datetime.datetime.now().date()-timedelta(days=1) 
   print(len(df))

   return df

# Función para cargar los datos en la base de datos MySQL
def load_data_to_mysql(df):
   print('Inicia Proceso para cargar df a la base de datos')
  
   engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')

   column_order = [
                  'location_id',
                  'location', 
                  'on_hand',
                  'inventory_value', 
                  'line',
                  'maker', 
                  'barcode', 
                  'article_id', 
                  'article', 
                  'negotiator',
                  'maker_id',
                  'report_date'
                  ]
   df = df[column_order]
   df.to_sql('edades_valor_inventario_a_corte', engine, if_exists='append', index=False) 
   print('Proceso finalizado') 

# Función para validar si el registro existe en la base de datos
def registro_existe():
    
   engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')

   # Obtener la fecha actual
   fecha_actual = datetime.datetime.now().date()-timedelta(days=1) 
   fecha_actual_str = fecha_actual.strftime('%Y-%m-%d')
   query = text(f"SELECT COUNT(*) FROM edades_valor_inventario_a_corte  WHERE DATE(report_date) = '{fecha_actual_str}'")
   print(query)
   with engine.connect() as connection:
      result = connection.execute(query)
      count = result.scalar()
      return count == 0

try:
  
   print( datetime.datetime.now().date()-timedelta(days=1))
   
   host = os.getenv("DB_HOST")
   user = os.getenv("DB_USERNAME")
   password = os.getenv("DB_PASSWORD")
   database = os.getenv("DB_DATABASE") 
   if(registro_existe()):
      print('Inicia descarga de reporte.')
      # URL del web query
      url = "https://4572765.app.netsuite.com/app/reporting/webquery.nl?compid=4572765&entity=71318&email=jose.bustos@distriaxa.co&role=3&cr=1932&hash=AAEJ7tMQYfLjV-mtb_YYobkOPLeoLvGfVG-3M2u-iTQhQgVt6Pw"
      
      # Obtención de los datos del web query
      response = requests.get(url)
      # Parseo de la respuesta HTML
      soup = BeautifulSoup(response.text, 'html.parser')

      # Extracción de la tabla
      table = soup.find('table')
     
      # Creación del DataFrame
      df = parse_html_table(str(table))
    
      # Validación de los datos
      df = validate_data(df)
  
      # Carga de los datos en la base de datos MySQL
      load_data_to_mysql(df)

   else:
      print('Ya existe el reorte para el dia de hoy.')
   logBi('Extraccion reporte inventario a corte ok.','')
except Exception as e:
   logBi('Extraccion reporte inventario a corte fallo.','')
   traceback.print_exc()
   exc_type, exc_obj, exc_tb = sys.exc_info()
   file_name = exc_tb.tb_frame.f_code.co_filename
   line_number = exc_tb.tb_lineno

   print(f"Archivo: {file_name}, Línea: {line_number}")
   print("Se produjo un error: %s", e)
   logException('Descarga reporte inventario a corte fallo',str(e))
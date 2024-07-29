import requests
import pandas as pd
from logBi import logBi
from logException import logException
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv
load_dotenv()

try:
    host = os.getenv("DB_HOST")
    user = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    database = os.getenv("DB_DATABASE")
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}/{database}")

    print("Inicio proceso")
    print("Inicia descarga de reporte.")

    #url = "https://4572765.app.netsuite.com/app/reporting/webquery.nl?compid=4572765&entity=118483&email=carlos.leon@distriaxa.co&role=3&cr=1917&hash=AAEJ7tMQAE7QJrg7QKPUBIXlr6ATONXNDPxJAVbB8Z2XCnmuzBA"
    url="https://4572765.app.netsuite.com/app/reporting/webquery.nl?compid=4572765&entity=71318&email=jose.bustos@distriaxa.co&role=3&cr=1917&hash=AAEJ7tMQvn6MThVDHdZqdgiLjMCCdeCd5d1qm_h1v3CTXqyOo7Y"
    response = requests.get(url)  # Obtención de los datos del web query.
    
    print("Inicia Proceso para extraer tabla del HTML")
    soup = BeautifulSoup(str(response.text), "html.parser")
    table = soup.find("table")
    datos = pd.read_html(str(table))[0]
    df = pd.DataFrame(datos)

    df.columns = [
        "fecha_creacion",
        "fecha_vencimiento",
        "dias_vencimiento",
        "documento",
        "saldo_total",
        "categoria",
        "clase_nombre",
        "location_name",
        "origen_pedido",
        "cuenta",
        "nombre_cliente",
        "referencia_pago",
        "metodo_pago",
        "tipo_transaccion",
        "cliente_trabajo",
        "b"
    ]

    df = df.iloc[1:]
   
    print("Inicia proceso de validacion del dataframe.")

    df["saldo_total"] = df["saldo_total"].str.replace("=", "")
    df["dias_vencimiento"] = df["dias_vencimiento"].str.replace("=", "")
    df["origen_pedido"]=df["origen_pedido"].fillna("")

    df['fecha_creacion'] = pd.to_datetime(df['fecha_creacion'], format='%d/%m/%Y')
    df['fecha_vencimiento'] = pd.to_datetime(df['fecha_vencimiento'], format='%d/%m/%Y')
  
    print("Inicia proceso de validacion del dataframe.")

    if  not len(df["origen_pedido"])==0:
          df["origen_pedido"]=df["clase_nombre"]

    print("Truncando tabla")
    table = "tb_ecomerx_reporte_de_cartera_por_edades_netsuit_fin"
    with engine.connect() as connection:
        query = text(f"TRUNCATE TABLE {table}")
        connection.execute(query)
    print("Tabla truncada")

    print("Inicia proceso para cargar df a la base de datos")
   
    column_order = [
        "fecha_creacion",
        "fecha_vencimiento",
        "dias_vencimiento",
        "documento",
        "saldo_total",
        "categoria",
        "clase_nombre",
        "location_name",
        "origen_pedido",
        "cuenta",
        "nombre_cliente",
        "referencia_pago",
        "metodo_pago",
        "tipo_transaccion",
        "cliente_trabajo",
        
    ]
    df = df[column_order]
    df.to_sql(
        "tb_ecomerx_reporte_de_cartera_por_edades_netsuit_fin",
        engine,
        if_exists="append",
        index=False,
    )
    print("Carga finalizada.")

    logBi('Extracion reporte_cartera_por_edades_ecomerx ok.','')
except Exception as e:
   print(str(e))
   logBi('Extracion cartera ecomerx  fallo.','')
   logException('Extracion reporte_cartera_por_edades_ecomerx  fallo',str(e))
from alpha_vantage.timeseries import TimeSeries
from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd
import os

load_dotenv()

DB_HOST = os.getenv('DB_HOST_PROD')
DB_PORT = os.getenv('DB_PORT_PROD')
DB_NAME = os.getenv('DB_NAME_PROD')
DB_USER = os.getenv('DB_USER_PROD')
DB_PASS = os.getenv('DB_PASS_PROD')
DB_SCHEMA = os.getenv('DB_SCHEMA_PROD')

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL)

api_key = 'RYNRE7HRJN84W6L0'

empresas = ['AAPL', 'MSFT', 'GOOGL']

dados_empresas = {}

def buscar_dados_stocks(api_key, empresas, outputsize='compact'):
    ts = TimeSeries(key=api_key, output_format='pandas')
    for empresa in empresas:
        data, metadata = ts.get_daily(symbol=empresa, outputsize=outputsize)
        
        data.columns = [f'{empresa}_{col}' for col in data.columns]
        
        dados_empresas[empresa] = data

    df_completo = pd.concat(dados_empresas.values(), axis=1)
    df_completo.to_csv("data.csv", index=False)
    return df_completo

def salvar_no_postgres(df, schema='public'):
    df.to_sql("stocks", engine, if_exists='replace', index=True, index_label='Date', schema=schema)

df = buscar_dados_stocks(api_key, empresas)
salvar_no_postgres(df)
from alpha_vantage.timeseries import TimeSeries
import pandas as pd

api_key = 'RYNRE7HRJN84W6L0'

ts = TimeSeries(key=api_key, output_format='pandas')

empresas = ['AAPL', 'MSFT', 'GOOGL']

dados_empresas = {}

for empresa in empresas:
    data, meta_data = ts.get_daily(symbol=empresa, outputsize='compact')
    dados_empresas[empresa] = data

df_completo = pd.concat(dados_empresas, axis=1)
df_completo.to_csv("data.csv", index=False)

print(df_completo.head())
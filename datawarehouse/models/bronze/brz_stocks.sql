-- models/bronze/stock_prices_bronze.sql

WITH source AS (
    SELECT
        "Date",
        "AAPL_1. open",
        "AAPL_2. high",
        "AAPL_3. low",
        "AAPL_4. close",
        "AAPL_5. volume",
        "MSFT_1. open",
        "MSFT_2. high",
        "MSFT_3. low",
        "MSFT_4. close",
        "MSFT_5. volume",
        "GOOGL_1. open",
        "GOOGL_2. high",
        "GOOGL_3. low",
        "GOOGL_4. close",
        "GOOGL_5. volume"
    FROM {{ source('dwhstocks', 'stocks') }}
),

renamed as (
    SELECT
        "Date" as date,
        "AAPL_1. open" as AAPL_open,
        "AAPL_2. high" as AAPL_high,
        "AAPL_3. low" as AAPL_low,
        "AAPL_4. close" as AAPL_close,
        "AAPL_5. volume" as AAPL_volume,
        "MSFT_1. open" as MSFT_open,
        "MSFT_2. high" as MSFT_high,
        "MSFT_3. low" as MSFT_low,
        "MSFT_4. close" as MSFT_close,
        "MSFT_5. volume" as MSFT_volume,
        "GOOGL_1. open" as GOOGL_open,
        "GOOGL_2. high" as GOOGL_high,
        "GOOGL_3. low" as GOOGL_low,
        "GOOGL_4. close" as GOOGL_close,
        "GOOGL_5. volume" as GOOGL_volume
    FROM
        source
)

SELECT * 
FROM renamed

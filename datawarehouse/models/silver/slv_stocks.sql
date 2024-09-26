WITH stock_data AS (
    SELECT 
        date, 
        'AAPL' AS symbol, 
        AAPL_open AS open_price, 
        AAPL_close AS close_price 
    FROM {{ ref('brz_stocks') }}
    UNION ALL
    SELECT 
        date, 
        'MSFT' AS symbol, 
        MSFT_open AS open_price, 
        MSFT_close AS close_price 
    FROM {{ ref('brz_stocks') }}
    UNION ALL
    SELECT 
        date, 
        'GOOGL' AS symbol, 
        GOOGL_open AS open_price, 
        GOOGL_close AS close_price 
    FROM {{ ref('brz_stocks') }}
)

SELECT * 
FROM stock_data

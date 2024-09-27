WITH ranked_trades AS (
    SELECT 
        t.trade_id,
        t.symbol,
        t.date,
        t.operation,
        t.quantity,
        s.open_price AS opening_price,
        s.close_price AS closing_price,
        ROW_NUMBER() OVER (PARTITION BY t.trade_id ORDER BY t.date) AS row_num
    FROM {{ ref('trades') }} t
    JOIN {{ ref('slv_stocks') }} s
    ON t.symbol = s.symbol
    AND t.date = s.date
)
SELECT
    buy.trade_id,
    buy.symbol,
    buy.date AS buy_date,
    buy.quantity,
    buy.opening_price AS buy_price,
    sell.date AS sell_date,
    sell.closing_price AS sell_price,
    (sell.closing_price - buy.opening_price) * buy.quantity AS profit_loss
FROM ranked_trades buy
JOIN ranked_trades sell
ON buy.trade_id = sell.trade_id
AND buy.operation = 'compra'
AND sell.operation = 'venda'
AND buy.row_num = 1
AND sell.row_num = 2

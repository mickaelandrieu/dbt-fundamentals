WITH payments AS (
    SELECT * FROM {{ ref('stg_payments') }}
)

SELECT
    ORDER_ID,
    SUM(AMOUNT) AS TOTAL_AMOUNT
FROM payments
GROUP BY ORDER_ID
HAVING TOTAL_AMOUNT < 0
WITH payments AS (
    SELECT * FROM {{ ref('stg_payments') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

order_payments AS (
    SELECT
        ORDER_ID,
        SUM(CASE WHEN STATUS = 'success' THEN amount END) AS AMOUNT
    FROM payments
    GROUP BY ORDER_ID
),

final AS (
    SELECT
        orders.ORDER_ID,
        orders.CUSTOMER_ID,
        orders.ORDER_DATE,
        COALESCE(order_payments.AMOUNT, 0) AS AMOUNT
    FROM orders
    LEFT JOIN order_payments USING (ORDER_ID)
)

SELECT * FROM final
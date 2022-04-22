WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customer_orders AS (
    SELECT
        CUSTOMER_ID,
        MIN(ORDER_DATE) AS FIRST_ORDER_DATE,
        MAX(ORDER_DATE) AS MOST_RECENT_ORDER_DATE,
        COUNT(ORDER_ID) AS NUMBER_OF_ORDERS
    FROM orders
    GROUP BY CUSTOMER_ID
),

payment_informations AS (
    SELECT
        CUSTOMER_ID,
        SUM(AMOUNT) AS LIFETIME_VALUE
    FROM {{ ref('fct_orders') }}
    GROUP BY CUSTOMER_ID
),

final AS (
    SELECT
        customers.CUSTOMER_ID,
        customers.FIRST_NAME,
        customers.LAST_NAME,
        customer_orders.FIRST_ORDER_DATE,
        customer_orders.MOST_RECENT_ORDER_DATE,
        COALESCE(customer_orders.NUMBER_OF_ORDERS, 0) AS NUMBER_OF_ORDERS,
        payment_informations.LIFETIME_VALUE
    FROM customers
    LEFT JOIN customer_orders USING (CUSTOMER_ID)
    LEFT JOIN payment_informations USING (CUSTOMER_ID)
)

SELECT * FROM final
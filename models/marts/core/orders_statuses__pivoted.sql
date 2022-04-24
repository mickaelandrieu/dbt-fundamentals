-- JINJA VERSION
{% set order_statuses = ['placed', 'shipped', 'returned', 'completed', 'return_pending'] -%}
WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

pivoted AS (
    SELECT
        CUSTOMER_ID,
        {% for order_status in order_statuses -%}
            SUM(CASE WHEN status = '{{ order_status }}' THEN 1 ELSE 0 END) AS {{ order_status|upper }}_TOTAL,
        {% endfor -%}
        COUNT(ORDER_ID) AS ORDERS_TOTAL
    FROM orders
    GROUP BY CUSTOMER_ID
)

select * from pivoted
WITH orders AS (
    SELECT
        ID AS ORDER_ID,
        USER_ID AS CUSTOMER_ID,
        ORDER_DATE,
        STATUS
    FROM `dbt-tutorial.jaffle_shop.orders`
)

select * FROM orders
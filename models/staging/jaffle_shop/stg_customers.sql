WITH customers AS (
    SELECT
        ID as CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME
    FROM `dbt-tutorial.jaffle_shop.customers`
)

select * from customers
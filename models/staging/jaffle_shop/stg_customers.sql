SELECT
    ID as CUSTOMER_ID,
    FIRST_NAME,
    LAST_NAME
FROM {{ source('jaffle_shop', 'customers')}}

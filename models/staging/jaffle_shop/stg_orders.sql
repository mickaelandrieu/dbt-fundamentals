SELECT
    ID AS ORDER_ID,
    USER_ID AS CUSTOMER_ID,
    ORDER_DATE,
    STATUS
FROM {{ source('jaffle_shop', 'orders')}}

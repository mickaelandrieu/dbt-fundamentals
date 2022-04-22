SELECT
    ID AS PAYMENT_ID,
    ORDERID AS ORDER_ID,
    PAYMENTMETHOD AS PAYMENT_METHOD,
    STATUS,

    -- amount is stored in cents, convert it to dollars
    AMOUNT / 100 AS AMOUNT,
    CREATED AS CREATED_AT
FROM {{ source('stripe', 'payment') }}

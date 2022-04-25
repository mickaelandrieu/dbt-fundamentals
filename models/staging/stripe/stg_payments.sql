SELECT
    ID AS PAYMENT_ID,
    ORDERID AS ORDER_ID,
    PAYMENTMETHOD AS PAYMENT_METHOD,
    STATUS,

    -- The amount is stored in cents, convert it to dollars
    {{ cents_to_dollars('amount') }} AS AMOUNT,
    CREATED AS CREATED_AT
FROM {{ source('stripe', 'payment') }}

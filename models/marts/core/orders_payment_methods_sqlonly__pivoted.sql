-- SQL VERSION
WITH payments AS (
    SELECT * FROM {{ ref('stg_payments') }}
),

pivoted AS (
    SELECT
        ORDER_ID,
        SUM(CASE WHEN payment_method = 'bank_transfer' THEN AMOUNT ELSE 0 END) AS BANK_TRANSFER_AMOUNT,
        SUM(CASE WHEN payment_method = 'credit_card' THEN AMOUNT ELSE 0 END) AS CREDIT_CARD_AMOUNT,
        SUM(CASE WHEN payment_method = 'coupon' THEN AMOUNT ELSE 0 END) AS COUPON_AMOUNT,
        SUM(CASE WHEN payment_method = 'gift_card' THEN AMOUNT ELSE 0 END) AS GIFT_CARD_AMOUNT
    FROM payments
    WHERE STATUS = 'success'
    GROUP BY ORDER_ID
)

select * from pivoted
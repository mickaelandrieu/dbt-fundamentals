-- Refunds have a negative amount, so the total amount should always be >= 0.
-- Therefore return records where this isn't true to make the test fail (which is weird from a developer point of view)
SELECT
    ORDER_ID,
    SUM(AMOUNT) AS TOTAL_AMOUNT
FROM {{ ref('stg_payments') }}
GROUP BY ORDER_ID
HAVING NOT(TOTAL_AMOUNT >= 0)
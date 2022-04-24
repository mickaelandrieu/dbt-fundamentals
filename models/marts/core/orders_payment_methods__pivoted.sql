-- JINJA VERSION
{% set payment_methods = ['bank_transfer', 'credit_card', 'coupon', 'gift_card'] -%}
WITH payments AS (
    SELECT * FROM {{ ref('stg_payments') }}
),

pivoted AS (
    SELECT
        ORDER_ID,
        {% for payment_method in payment_methods -%}
            SUM(CASE WHEN payment_method = '{{ payment_method }}' THEN AMOUNT ELSE 0 END) AS {{ payment_method|upper }}_AMOUNT{% if not loop.last %},{% endif %}
        {% endfor %}
    FROM payments
    WHERE STATUS = 'success'
    GROUP BY ORDER_ID
)

select * from pivoted
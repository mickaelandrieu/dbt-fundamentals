{%- macro template_example() -%}
    {% set query %}
        select true as bool
    {% endset %}

    {% if execute %}
        {# See @link https://agate.readthedocs.io/en/latest/api/table.html #}
        {% set results=run_query(query).columns[0].values()[0] %}

        {{ log('SQL Results' ~ results, info=True) }}

        select
            {{ results }} as is_real
        from a_real_table
    {% endif %}    
{%- endmacro -%}

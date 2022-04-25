{#
    This macro only works for SnowFlake as target.role variable is not available for others providers
    See @link https://docs.getdbt.com/reference/dbt-jinja-functions/target#adapter-specific
    See @usage dbt run-operation grant_select
#}
{% macro grant_select(schema=target.schema, role=target.dataset) %}

    {% set query %}
        grant usage on schema {{ schema }} to role {{ role }};
        grant select on all tables in schema {{ schema }} to role {{ role }};
        grant select on all views in schema {{ schema }} to role {{ role }};
    {% endset %}

    {{ log('Granting select on all tables and views in schema ' ~ target.schema ~ ' to role ' ~ role, info=True) }}
    {% do run_query(query) %}
    {{ log('Privileges granted', info=True) }}

{% endmacro %}

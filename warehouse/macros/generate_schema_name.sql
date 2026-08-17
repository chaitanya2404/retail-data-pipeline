{#
    Use the custom schema verbatim instead of dbt's default `<target>_<custom>` concatenation.

    The default exists to stop developers sharing one warehouse from overwriting each other's
    models — everyone builds into their own prefixed schema. That trade is worth taking here:
    this warehouse is per-developer already (a local container), and the layer names are part of
    the contract. A BI tool, a notebook and the documentation all reference `marts.fct_sales`,
    and `public_marts.fct_sales` would leak the target name into every one of them.

    A shared deployment would set target.schema per developer and revert this.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {#
        Override the default schema generation to use custom schemas in production
        but keep the target schema in other environments
    #}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
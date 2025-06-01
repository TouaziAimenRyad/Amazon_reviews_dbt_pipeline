{% macro log_dbt_metadata(results) %}
  {% set dbt_run_id = invocation_id %}
  {% set dbt_run_timestamp = run_started_at.strftime('%Y-%m-%d %H:%M:%S') %}

  {% set models_run = 0 %}
  {% set models_skipped = 0 %}
  {% set models_errored = 0 %}
  {% set tests_run = 0 %}
  {% set tests_failed = 0 %}

  {# Iterate through each result in the list #}
  {% for result in results %}
    {% if result.node.resource_type == 'model' %}
      {% if result.status == 'success' %}
        {% set models_run = models_run + 1 %}
      {% elif result.status == 'skipped' %}
        {% set models_skipped = models_skipped + 1 %}
      {% elif result.status == 'error' %}
        {% set models_errored = models_errored + 1 %}
      {% endif %}
    {% elif result.node.resource_type == 'test' %}
      {% if result.status == 'pass' or result.status == 'fail' %}
        {% set tests_run = tests_run + 1 %}
      {% endif %}
      {% if result.status == 'fail' %}
        {% set tests_failed = tests_failed + 1 %}
      {% endif %}
    {% endif %}
  {% endfor %}

  {# Determine the overall dbt run status #}
  {% set overall_status = 'success' %}
  {% if models_errored > 0 or tests_failed > 0 %}
    {% set overall_status = 'error' %}
  {% endif %}

  {% set sql %}
    CREATE SCHEMA IF NOT EXISTS data_wearhouse;
    
    CREATE TABLE IF NOT EXISTS data_wearhouse.dbt_run_metadata (
      dbt_run_id VARCHAR(255) PRIMARY KEY,
      dbt_run_timestamp TIMESTAMP,
      dbt_target VARCHAR(50),
      dbt_schema VARCHAR(255),
      dbt_status VARCHAR(50),
      dbt_models_run INT,
      dbt_models_skipped INT,
      dbt_models_errored INT,
      dbt_tests_run INT,
      dbt_tests_failed INT,
      dbt_invocation_args TEXT
    );

    INSERT INTO data_wearhouse.dbt_run_metadata (
      dbt_run_id,
      dbt_run_timestamp,
      dbt_target,
      dbt_schema,
      dbt_status,
      dbt_models_run,
      dbt_models_skipped,
      dbt_models_errored,
      dbt_tests_run,
      dbt_tests_failed,
      dbt_invocation_args
    ) VALUES (
      '{{ dbt_run_id }}',
      '{{ dbt_run_timestamp }}',
      '{{ target.name }}',
      '{{ target.schema }}',
      '{{ overall_status }}',
      {{ models_run }},
      {{ models_skipped }},
      {{ models_errored }},
      {{ tests_run }},
      {{ tests_failed }},
      '{{ invocation_args_dict | tojson }}'
    )
    ON CONFLICT (dbt_run_id) DO UPDATE SET
      dbt_run_timestamp = EXCLUDED.dbt_run_timestamp,
      dbt_target = EXCLUDED.dbt_target,
      dbt_schema = EXCLUDED.dbt_schema,
      dbt_status = EXCLUDED.dbt_status,
      dbt_models_run = EXCLUDED.dbt_models_run,
      dbt_models_skipped = EXCLUDED.dbt_models_skipped,
      dbt_models_errored = EXCLUDED.dbt_models_errored, {# FIX IS HERE #}
      dbt_tests_run = EXCLUDED.dbt_tests_run,
      dbt_tests_failed = EXCLUDED.dbt_tests_failed,
      dbt_invocation_args = EXCLUDED.dbt_invocation_args;
  {% endset %}

  {% do run_query(sql) %}
  {% do log("Logged dbt run metadata for " ~ dbt_run_id, info=True) %}
{% endmacro %}
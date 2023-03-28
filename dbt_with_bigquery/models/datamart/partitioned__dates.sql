{{
    config(
        materialized='view',
        partition_by={
            'field': 'DATE_APPLY',
            'data_type': 'date',
            'granularity': 'day'
        }
    )
}}

  SELECT
    *
  FROM
    {{ ref('fact__tables') }}
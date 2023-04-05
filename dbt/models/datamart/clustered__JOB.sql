{{
    config(
        materialized='view',
        partition_by={
            'field': 'DATE_APPLY',
            'data_type': 'date',
            'granularity': 'day'
        },
        cluster_by = 'ID_JOB'
    )
}}

  SELECT
    *
  FROM
    {{ ref('fact__tables') }}
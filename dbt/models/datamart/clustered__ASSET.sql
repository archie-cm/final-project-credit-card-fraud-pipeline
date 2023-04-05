{{
    config(
        materialized='view',
        partition_by={
            'field': 'DATE_APPLY',
            'data_type': 'date',
            'granularity': 'month'
        },
        cluster_by = 'FLAG_OWN_REALTY'
    )
}}

  SELECT
    *
  FROM
    {{ ref('fact__tables') }}
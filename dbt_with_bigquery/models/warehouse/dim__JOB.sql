{{ config(materialized='view') }}

WITH dim__JOB AS(
    SELECT DISTINCT
        OCCUPATION_TYPE
    FROM {{ ref('stg__credit_card') }}
)

SELECT 
    {{ encode_JOB('OCCUPATION_TYPE') }} as ID_JOB, *
FROM 
    dim__JOB
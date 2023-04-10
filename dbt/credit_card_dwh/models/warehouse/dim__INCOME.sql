{{ config(materialized='view') }}

WITH dim__INCOME AS(
    SELECT DISTINCT
        NAME_INCOME_TYPE
    FROM {{ ref('stg__credit_card') }}
)

SELECT 
    {{ encode_INCOME('NAME_INCOME_TYPE') }} as ID_INCOME, *
FROM 
    dim__INCOME
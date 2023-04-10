{{ config(materialized='view') }}

WITH dim__HOUSING AS(
    SELECT DISTINCT
        NAME_HOUSING_TYPE
    FROM {{ ref('stg__credit_card') }}
)

SELECT 
    {{ encode_HOUSING('NAME_HOUSING_TYPE') }} as ID_HOUSING, *
FROM 
    dim__HOUSING
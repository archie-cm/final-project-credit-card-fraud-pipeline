{{ config(materialized='view') }}

WITH dim__EDUCATION AS(
    SELECT DISTINCT
        NAME_EDUCATION_TYPE
    FROM {{ ref('stg__credit_card') }}
)

SELECT 
    {{ encode_EDUCATION('NAME_EDUCATION_TYPE') }} as ID_EDUCATION, NAME_EDUCATION_TYPE
FROM 
    dim__EDUCATION
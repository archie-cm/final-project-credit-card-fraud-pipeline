{{ config(materialized='view') }}

WITH dim__MARITAL AS(
    SELECT DISTINCT
        NAME_FAMILY_STATUS
    FROM {{ ref('stg__credit_card') }}
)

SELECT 
    {{ encode_MARITAL('NAME_FAMILY_STATUS') }} as ID_MARITAL, *
FROM 
    dim__MARITAL
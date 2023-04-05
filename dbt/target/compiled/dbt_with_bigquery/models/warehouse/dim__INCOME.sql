

WITH dim__INCOME AS(
    SELECT DISTINCT
        NAME_INCOME_TYPE
    FROM `data-fellowship-9-project`.`final_project`.`stg__credit_card`
)

SELECT 
    case NAME_INCOME_TYPE
       when 'Working' then 1
       when 'Commercial associate' then 2
       when 'Pensioner' then 3
       when 'State servant' then 4
       when 'Student' then 5
   end as ID_INCOME, *
FROM 
    dim__INCOME
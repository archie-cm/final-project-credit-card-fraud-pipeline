

  create or replace view `data-fellowship-9-project`.`final_project`.`dim__MARITAL`
  OPTIONS()
  as 

WITH dim__MARITAL AS(
    SELECT DISTINCT
        NAME_FAMILY_STATUS
    FROM `data-fellowship-9-project`.`final_project`.`stg__credit_card`
)

SELECT 
    case NAME_FAMILY_STATUS
       when 'Married' then 1
       when 'Single / not married' then 2
       when 'Civil marriage' then 3
       when 'Separated' then 4
       when 'Widow' then 5
   end as ID_MARITAL, *
FROM 
    dim__MARITAL;


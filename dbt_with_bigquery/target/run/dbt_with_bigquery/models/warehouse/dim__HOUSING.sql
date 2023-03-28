

  create or replace view `data-fellowship-9-project`.`final_project`.`dim__HOUSING`
  OPTIONS()
  as 

WITH dim__HOUSING AS(
    SELECT DISTINCT
        NAME_HOUSING_TYPE
    FROM `data-fellowship-9-project`.`final_project`.`stg__credit_card`
)

SELECT 
    case NAME_HOUSING_TYPE
       when 'House / apartment' then 1
       when 'With parents' then 2
       when 'Municipal apartment' then 3
       when 'Rented apartment' then 4
       when 'Office apartment' then 5
       when 'Co-op apartment' then 6
   end as ID_HOUSING, *
FROM 
    dim__HOUSING;


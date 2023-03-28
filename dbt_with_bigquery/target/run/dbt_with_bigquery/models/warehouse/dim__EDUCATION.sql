

  create or replace view `data-fellowship-9-project`.`final_project`.`dim__EDUCATION`
  OPTIONS()
  as 

WITH dim__EDUCATION AS(
    SELECT DISTINCT
        NAME_EDUCATION_TYPE
    FROM `data-fellowship-9-project`.`final_project`.`stg__credit_card`
)

SELECT 
    case NAME_EDUCATION_TYPE
       when 'Lower secondary' then 1
       when 'Secondary / secondary special' then 2
       when 'Incomplete higher' then 3
       when 'Higher education' then 4
       when 'Academic degree' then 5
   end as ID_EDUCATION, NAME_EDUCATION_TYPE
FROM 
    dim__EDUCATION;


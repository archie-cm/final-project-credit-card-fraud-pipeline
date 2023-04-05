
    
    

with dbt_test__target as (

  select ID as unique_field
  from `data-fellowship-9-project`.`final_project`.`stg__credit_card`
  where ID is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1



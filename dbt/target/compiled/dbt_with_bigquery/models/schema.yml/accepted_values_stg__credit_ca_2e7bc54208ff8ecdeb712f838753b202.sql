
    
    

with all_values as (

    select
        NAME_HOUSING_TYPE as value_field,
        count(*) as n_records

    from `data-fellowship-9-project`.`final_project`.`stg__credit_card`
    group by NAME_HOUSING_TYPE

)

select *
from all_values
where value_field not in (
    'House / apartment','With parents','Municipal apartment','Rented apartment','Office apartment','Co-op apartment'
)



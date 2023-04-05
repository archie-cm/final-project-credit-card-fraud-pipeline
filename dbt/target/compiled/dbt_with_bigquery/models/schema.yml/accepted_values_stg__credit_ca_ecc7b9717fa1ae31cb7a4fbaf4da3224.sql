
    
    

with all_values as (

    select
        NAME_INCOME_TYPE as value_field,
        count(*) as n_records

    from `data-fellowship-9-project`.`final_project`.`stg__credit_card`
    group by NAME_INCOME_TYPE

)

select *
from all_values
where value_field not in (
    'Working','Commercial associate','Pensioner','State servant','Student'
)




    
    

with all_values as (

    select
        NAME_FAMILY_TYPE as value_field,
        count(*) as n_records

    from `data-fellowship-9-project`.`final_project`.`stg__credit_card`
    group by NAME_FAMILY_TYPE

)

select *
from all_values
where value_field not in (
    'Married','Single / not married','Civil marriage','Separated','Widow'
)



 {#
    This macro returns the description of the job 
#}

{% macro encode_JOB(column_name) -%}

    case {{ column_name }}
       when 'Laborers' then 1
       when 'Core staff' then 2
       when 'Sales staff' then 3
       when 'Managers' then 4
       when 'Drivers' then 5
       when 'High skill tech staff' then 6
       when 'Accountants' then 7
       when 'Medicine staff' then 8
       when 'Cooking staff' then 9
       when 'Security staff' then 10
       when 'Cleaning staff' then 11
       when 'Private service staff' then 12
       when 'Low-skill Laborers' then 13
       when 'Secretaries' then 14
       when 'Waiters/barmen staff' then 15
       when 'Realty agents' then 16
       when 'HR staff' then 17
       when 'IT staff' then 18
       WHEN null then 0
   end


{%- endmacro %}
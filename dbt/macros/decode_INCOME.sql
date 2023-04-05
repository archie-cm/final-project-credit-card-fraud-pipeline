 {#
    This macro returns the description of the job 
#}

{% macro encode_INCOME(column_name) -%}

    case {{ column_name }}
       when 'Working' then 1
       when 'Commercial associate' then 2
       when 'Pensioner' then 3
       when 'State servant' then 4
       when 'Student' then 5
   end


{%- endmacro %}
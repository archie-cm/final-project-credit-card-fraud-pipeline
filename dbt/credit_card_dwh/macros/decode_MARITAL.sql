 {#
    This macro returns the description of the job 
#}

{% macro encode_MARITAL(column_name) -%}

    case {{ column_name }}
       when 'Married' then 1
       when 'Single / not married' then 2
       when 'Civil marriage' then 3
       when 'Separated' then 4
       when 'Widow' then 5
   end


{%- endmacro %}
 {#
    This macro returns the description of the job 
#}

{% macro encode_EDUCATION(column_name) -%}

    case {{ column_name }}
       when 'Lower secondary' then 1
       when 'Secondary / secondary special' then 2
       when 'Incomplete higher' then 3
       when 'Higher education' then 4
       when 'Academic degree' then 5
   end


{%- endmacro %}
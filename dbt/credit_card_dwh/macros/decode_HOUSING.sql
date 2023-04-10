 {#
    This macro returns the description of the job 
#}

{% macro encode_HOUSING(column_name) -%}

    case {{ column_name }}
       when 'House / apartment' then 1
       when 'With parents' then 2
       when 'Municipal apartment' then 3
       when 'Rented apartment' then 4
       when 'Office apartment' then 5
       when 'Co-op apartment' then 6
   end


{%- endmacro %}
 {#
    This macro returns the description of the fixture status
#}

{% macro get_fixture_status_description(fixture_status_short) -%}

    case {{ fixture_status_short }}
        when 'FT' then 'Match Finished'
        when 'AET' then 'Match Finished After Extra Time'
        when 'PEN' then 'Match Finished After Penalty'
    end

{%- endmacro %}
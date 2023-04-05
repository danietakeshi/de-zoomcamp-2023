 {#
    This macro returns the description of the games position status
#}

{% macro get_games_position_description(games_position) -%}

    case {{ games_position }}
        when 'G' then 'Goalkeeper'
        when 'D' then 'Defender'
        when 'M' then 'Midfielder'
        when 'F' then 'Forward'
    end

{%- endmacro %}
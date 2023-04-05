{{ config(materialized='view') }}

with fixture_data as 
(
  select *,
    CASE 
      WHEN fixture_venue_city is null and fixture_venue_name = 'Loftus Versfeld Stadium (Pretoria (Tshwane))' THEN 'Pretoria (Tshwane)'
      WHEN fixture_venue_city is null and INSTR(fixture_venue_name, '(', 1, 2) > 0 THEN REPLACE(RIGHT(fixture_venue_name, LENGTH(fixture_venue_name) - INSTR(fixture_venue_name, '(', 1, 2)), ')', '')
      WHEN fixture_venue_city is null and INSTR(fixture_venue_name, '(', 1, 2) = 0 THEN REPLACE(RIGHT(fixture_venue_name, LENGTH(fixture_venue_name) - INSTR(fixture_venue_name, '(')), ')', '')
      ELSE fixture_venue_city
    END fixture_venue_city_treated,
    CASE 
      WHEN fixture_venue_city is null and fixture_venue_name = 'Loftus Versfeld Stadium (Pretoria (Tshwane))' THEN 'Loftus Versfeld Stadium'
      WHEN fixture_venue_city is null and INSTR(fixture_venue_name, '(', 1, 2) > 0 THEN LEFT(fixture_venue_name, INSTR(fixture_venue_name, '(', 1, 2) - 2)
      WHEN fixture_venue_city is null and INSTR(fixture_venue_name, '(', 1, 2) = 0 THEN LEFT(fixture_venue_name, INSTR(fixture_venue_name, '(') - 2)
      ELSE fixture_venue_name
    END fixture_venue_name_treated,
    CASE
      WHEN teams_home_winner = true then teams_home_name
      WHEN teams_away_winner = true then teams_away_name
      ELSE 'Draw'
    END winners_name
  from {{ source('staging','ods_football_fixtures') }}
  where league_name = 'World Cup'
)
select
    
    -- identifiers
    fixture_id,
    fixture_referee,
    fixture_venue_name_treated as fixture_venue_name,
    fixture_venue_city_treated as fixture_venue_city,
    fixture_status_short,
    {{ get_fixture_status_description('fixture_status_short') }} as fixture_status_description,
    fixture_status_elapsed,
    league_name,
    league_logo,
    league_season,
    league_round,
    teams_home_name,
    teams_home_logo,
    teams_away_name,
    teams_away_logo,

    -- timestamps
    fixture_date,

    -- fixture info
    goals_home,
    goals_away,
    score_halftime_home,
    score_halftime_away,
    score_fulltime_home,
    score_fulltime_away,
    score_extratime_home,
    score_extratime_away,
    score_penalty_home,
    score_penalty_away,
    winners_name
    
from fixture_data

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
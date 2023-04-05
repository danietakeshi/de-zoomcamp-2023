{{ config(materialized='table') }}

with fixture_data as (
    select *
    from {{ ref('stg_football_wc_fixtures') }}
)
select 
    fixture_data.*,
    
    -- fixture info aggregated
    (goals_home + goals_away) goals_total,
    (score_halftime_home + score_halftime_away) score_first_halftime_total,
    (score_fulltime_home + score_fulltime_away) - (score_halftime_home + score_halftime_away) score_second_halftime_total,
    (score_extratime_home + score_extratime_away) score_extratime_total

from fixture_data

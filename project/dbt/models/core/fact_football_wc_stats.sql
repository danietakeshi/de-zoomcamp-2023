{{ config(materialized='table') }}

with stats_data as (
    select *,
    {{ get_games_position_description('games_position') }} as games_position_description,
    from {{ ref('stg_football_stats') }}
),
fixture_data as (
    select *
    from {{ ref('fact_football_wc_fixtures') }}
)
select 
    sd.*,
    
    -- fixture info
    fd.fixture_referee,
    fd.fixture_venue_name,
    fd.fixture_venue_city,
    fd.fixture_status_short,
    fd.fixture_status_description,
    fd.league_name,
    fd.league_season,
    fd.league_round,
    fd.winners_name

from stats_data sd
inner join fixture_data fd on sd.fixture_id = fd.fixture_id
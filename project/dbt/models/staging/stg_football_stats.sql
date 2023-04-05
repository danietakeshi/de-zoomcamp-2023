{{ config(materialized='view') }}

with stats_data as 
(
  select *
    from
    {{ source('staging','ods_football_stats') }} st
    where exists (select 1 from {{ source('staging','ods_football_fixtures') }} fx where fx.fixture_id = st.fixture_id)
)
select
    -- identifiers
    fixture_id,
    team_name,
    team_logo,
    player_name,
    player_photo,

    -- player info
    offsides,
    games_minutes,
    games_number,
    games_position,
    games_rating,
    games_captain,
    games_substitute,
    shots_total,
    shots_on,
    goals_total,
    goals_conceded,
    goals_assists,
    passes_total,
    passes_key,
    passes_accuracy,
    tackles_total,
    tackles_blocks,
    tackles_interceptions,
    duels_total,
    duels_won,
    dribbles_attempts,
    dribbles_success,
    dribbles_past,
    fouls_drawn,
    fouls_committed,
    cards_yellow,
    cards_red,
    penalty_won,
    penalty_commited,
    penalty_scored,
    penalty_missed,
    penalty_saved
    
from stats_data

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}
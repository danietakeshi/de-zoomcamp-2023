CREATE TABLE `coherent-bliss-275820.dezoomcamp.ods_football_stats` (
	fixture_id INT,
	team_id INT,
	team_name STRING,
	team_logo STRING,
	player_id INT,
	player_name STRING,
	player_photo STRING,
	offsides INT,
	games_minutes INT,
	games_number INT,
	games_position STRING,
	games_rating FLOAT64,
	games_captain BOOL,
	games_substitute BOOL,
	shots_total INT,
	shots_on INT,
	goals_total INT,
	goals_conceded INT,
	goals_assists INT,
	goals_saves INT,
	passes_total INT,
	passes_key INT,
	passes_accuracy INT,
	tackles_total INT,
	tackles_blocks INT,
	tackles_interceptions INT,
	duels_total INT,
	duels_won INT,
	dribbles_attempts INT,
	dribbles_success INT,
	dribbles_past INT,
	fouls_drawn INT,
	fouls_committed INT,
	cards_yellow INT,
	cards_red INT,
	penalty_won INT,
	penalty_commited INT,
	penalty_scored INT,
	penalty_missed INT,
	penalty_saved INT
)
CLUSTER BY fixture_id;
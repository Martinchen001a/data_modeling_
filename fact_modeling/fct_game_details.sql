CREATE TABLE fct_game_details (
	dim_game_date_est				DATE,
	dim_season						INT,
	dim_team_id						INT,
	dim_is_playing_at_home		BOOLEAN,
	team_wins					BOOLEAN,
	home_team_wins				BOOLEAN,
	dim_player_id					INT,
	dim_player_name					TEXT,
	dim_start_position				TEXT,
	dim_did_not_play			BOOLEAN,
	dim_did_not_dress			BOOLEAN,
	dim_not_with_team			BOOLEAN,
	minutes						REAL,
	fgm							REAL,
	fga							REAL,
	fg3m						REAL,
	fg3a						REAL,
	ftm							REAL,
	fta							REAL,
	oreb						REAL,
	dreb						REAL,
	reb							REAL,
	ast							REAL,
	stl							REAL,
	blk							REAL,
	turnovers					REAL,
	pf							REAL,
	pts							REAL,
	plus_minus					INT,
	PRIMARY KEY(dim_team_id, dim_player_id, dim_game_date_est)
)



INSERT INTO fct_game_details
WITH deduped AS(
	SELECT 
	g.game_date_est,
	g.season,
	g.team_id_home,
	g.home_team_wins,
	CASE 
		WHEN gd.team_id = g.home_team_id and home_team_wins = 1 THEN 1
		WHEN gd.team_id = g.visitor_team_id and home_team_wins = 0 THEN 1
		ELSE 0
	END AS team_wins,
	gd.*,
	ROW_NUMBER()OVER(PARTITION BY gd.player_id, gd.game_id, gd.team_id ORDER BY g.game_date_est DESC) AS rn
	FROM game_details gd
	JOIN games g ON gd.game_id = g.game_id
)
 

select
	game_date_est AS dim_game_date_est,
	season AS dim_season,
	team_id AS dim_team_id,
	team_id = team_id_home AS dim_is_playing_at_home,
	team_wins = 1 AS team_wins,
	home_team_wins = 1 AS home_team_wins,
	player_id AS dim_player_id,
	player_name AS dim_player_name,
	start_position AS dim_start_position,
	COALESCE(POSITION('DNP' in comment), 0) > 0 AS dim_did_not_play,
	COALESCE(POSITION('DND' in comment), 0) > 0 AS dim_did_not_dress,
	COALESCE(POSITION('NWT' in comment), 0) > 0 AS dim_not_with_team,
	CAST(SPLIT_PART(min, ':', 1) AS REAL) + CAST(SPLIT_PART(min, ':', 2) AS REAL)/60 AS minutes,
	fgm,
	fga,
	fg3m,
	fg3a,
	ftm,
	fta,
	oreb,
	dreb,
	reb,
	ast,
	stl,
	blk,
	"TO" AS turnovers,
	pf,
	pts,
	plus_minus
FROM deduped
where rn = 1



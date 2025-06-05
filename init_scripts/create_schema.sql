CREATE TABLE fantasy_score_per_game (
    game_date DATE NOT NULL,
    matchup TEXT,
    name TEXT,
    player_id VARCHAR(50) NOT NULL,
    season_id VARCHAR(7) NOT NULL,
    points INTEGER,
    assists INTEGER,
    rebounds INTEGER,
    steals INTEGER,
    blocks INTEGER,
    turnovers INTEGER,
    fantasy_points NUMERIC(10,1),
    PRIMARY KEY (player_id, game_date)
);

  CREATE TABLE player_career_stats (
    player_id VARCHAR(50) NOT NULL,
    season_id VARCHAR(7) NOT NULL,
    name TEXT,
    points INTEGER ,
    assists INTEGER,
    rebounds INTEGER,
    steals INTEGER,
    blocks INTEGER,
    turnovers INTEGER,
    fantasy_points NUMERIC(10,1),
    PRIMARY KEY (player_id, season_id)
);
CREATE TABLE player (
    player_id BIGINT  PRIMARY KEY,
    name TEXT NOT NULL,
    team_id BIGINT,
    position TEXT,
    height TEXT,
    weight FLOAT,
    birth_date DATE
);

CREATE TABLE team (
    team_id BIGINT PRIMARY KEY,
    team_name TEXT,
    abbreviation CHAR(3),
    conference TEXT,
    division TEXT
);

CREATE TABLE game (
    game_id BIGINT PRIMARY KEY,
    game_date DATE,
    home_team_id BIGINT,
    away_team_id BIGINT,
    season_id VARCHAR(7)
);


CREATE TABLE fantasy_score_per_game (
    player_id BIGINT  NOT NULL,
    game_id BIGINT NOT NULL,
    season_id VARCHAR(7) NOT NULL,
    points INTEGER,
    assists INTEGER,
    rebounds INTEGER,
    steals INTEGER,
    blocks INTEGER,
    turnovers INTEGER,
    fantasy_points NUMERIC(10,1),
    PRIMARY KEY (player_id, game_id),
    FOREIGN KEY (player_id) REFERENCES player(player_id),
    FOREIGN KEY (game_id) REFERENCES game(game_id)
);

CREATE TABLE player_career_stats (
    player_id BIGINT  NOT NULL,
    season_id VARCHAR(7) NOT NULL,
    points INTEGER,
    assists INTEGER,
    rebounds INTEGER,
    steals INTEGER,
    blocks INTEGER,
    turnovers INTEGER,
    fantasy_points NUMERIC(10,1),
    PRIMARY KEY (player_id, season_id),
    FOREIGN KEY (player_id) REFERENCES player(player_id)
);

--CREATE TABLE salary (
--    player_id INTEGER,
--    game_date DATE,
--    site TEXT, -- e.g., 'DraftKings'
--    salary INTEGER,
--    PRIMARY KEY (player_id, game_date, site)
--);
--
--CREATE TABLE player_injury (
--    player_id INTEGER REFERENCES player(player_id),
--    injury_status TEXT,
--    injury_description TEXT,
--    last_updated DATE
--);
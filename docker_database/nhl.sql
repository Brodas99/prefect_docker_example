
CREATE TABLE IF NOT EXISTS nhl_teams 
(team_name VARCHAR, 
short_name VARCHAR, 
team_abbrev VARCHAR, 
team_id int,
conference VARCHAR, 
division VARCHAR, 
city VARCHAR,
venue VARCHAR,
franchise_id INT,
nhl_team_link VARCHAR,
nhl_franchise_link VARCHAR,
first_year INT );


CREATE TABLE IF NOT EXISTS nhl_schedule(
home_team VARCHAR, 
home_wins INT,
home_losses INT,
home_ot INT,
away_team VARCHAR,
away_wins INT,
away_losses INT ,
away_ot INT,
home_score INT,
away_score INT,
venue VARCHAR,
game_date DATE,
season VARCHAR,
game_type VARCHAR,
game_id INT,
game_winner VARCHAR,
h_game_number INT,
a_game_number INT,
processed_date TIMESTAMP);
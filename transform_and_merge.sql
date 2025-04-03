MERGE `vital-cathode-454012-k0.football_dataset.matches_production` AS target
USING (
  SELECT
    match_id,
    utc_date,
    status,
    matchday,
    stage,
    home_team_id,
    home_team_name,
    away_team_id,
    away_team_name,
    score_fulltime_home,
    score_fulltime_away,
    score_halftime_home,
    score_halftime_away,
    winner,
    last_updated,
    ingestion_timestamp
  FROM `vital-cathode-454012-k0.football_dataset.matches_raw`
  WHERE ingestion_timestamp > (SELECT MAX(ingestion_timestamp) FROM `vital-cathode-454012-k0.football_dataset.matches_production`)
) AS source
ON target.match_id = source.match_id
WHEN MATCHED THEN
  UPDATE SET
    target.utc_date = source.utc_date,
    target.status = source.status,
    target.matchday = source.matchday,
    target.stage = source.stage,
    target.home_team_id = source.home_team_id,
    target.home_team_name = source.home_team_name,
    target.away_team_id = source.away_team_id,
    target.away_team_name = source.away_team_name,
    target.score_fulltime_home = source.score_fulltime_home,
    target.score_fulltime_away = source.score_fulltime_away,
    target.score_halftime_home = source.score_halftime_home,
    target.score_halftime_away = source.score_halftime_away,
    target.winner = source.winner,
    target.last_updated = source.last_updated,
    target.ingestion_timestamp = source.ingestion_timestamp
WHEN NOT MATCHED THEN
  INSERT (
    match_id,
    utc_date,
    status,
    matchday,
    stage,
    home_team_id,
    home_team_name,
    away_team_id,
    away_team_name,
    score_fulltime_home,
    score_fulltime_away,
    score_halftime_home,
    score_halftime_away,
    winner,
    last_updated,
    ingestion_timestamp
  )
  VALUES (
    source.match_id,
    source.utc_date,
    source.status,
    source.matchday,
    source.stage,
    source.home_team_id,
    source.home_team_name,
    source.away_team_id,
    source.away_team_name,
    source.score_fulltime_home,
    source.score_fulltime_away,
    source.score_halftime_home,
    source.score_halftime_away,
    source.winner,
    source.last_updated,
    source.ingestion_timestamp
  );
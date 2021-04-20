-- File contains DDL statements that should be run on startup.
-- Each DDL statement must be on its own line.
-- A line that starts with '--' is considered to be a comment.

-- This logs the clusters and queries belonging to the cluster for a forecast inference.
CREATE TABLE noisepage_forecast_clusters(ts BIGINT, cluster_id INT, query_id INT, db_id INT);
-- Forecast inference arrival rate for each query id broken up by segment interval
CREATE TABLE noisepage_forecast_forecasts(ts BIGINT, query_id INT, interval INT, rate REAL);
-- Observed history of query arrival rate by segment interval
CREATE TABLE noisepage_forecast_frequencies(ts BIGINT, query_id INT, seen REAL);

-- We need to separate these. This is because QueryTrace metric logs every time
-- a query is executed but may not have the textual metadata on hand. Textual
-- metadata is logged only by QueryText. Due to this, we have to read out the
-- entire noisepage_forecast_texts during forecasting.
CREATE TABLE noisepage_forecast_texts(db_id INT, query_id INT, query_text VARCHAR, types VARCHAR);
CREATE TABLE noisepage_forecast_parameters(ts BIGINT, query_id INT, parameters VARCHAR);

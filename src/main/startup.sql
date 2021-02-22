CREATE TABLE noisepage_forecast_clusters(iteration INT, cluster_id INT, query_id INT, db_id INT);
CREATE TABLE noisepage_forecast_forecasts(iteration INT, query_id INT, interval INT, rate REAL);
CREATE TABLE noisepage_forecast_frequencies(iteration INT, query_id INT, interval INT, seen REAL);
CREATE TABLE noisepage_forecast_parameters(iteration INT, db_id INT, query_id INT, query_text VARCHAR, types VARCHAR, parameters VARCHAR);

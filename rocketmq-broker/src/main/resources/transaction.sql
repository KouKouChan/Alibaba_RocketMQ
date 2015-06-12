CREATE TABLE IF NOT EXISTS t_transaction (
	offset				NUMERIC(20) PRIMARY KEY,
	producerGroup		VARCHAR(64)
)

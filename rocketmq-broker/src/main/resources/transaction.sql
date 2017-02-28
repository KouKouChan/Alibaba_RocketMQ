CREATE TABLE t_transaction(
	offset				NUMERIC(20) PRIMARY KEY,
	producerGroup		VARCHAR(64) NOT NULL,
	brokerName VARCHAR(64) NOT NULL
)

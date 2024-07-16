CREATE TABLE IF NOT EXISTS MD_EXCHANGE_RATE_D (
	DATA_ACTUAL_DATE DATE NOT NULL,
	DATA_ACTUAL_END_DATE DATE,
	CURRENCY_RK INTEGER NOT NULL,
	REDUCED_COURCE FLOAT,
	CODE_ISO_NUM VARCHAR(3),
	PRIMARY KEY (DATA_ACTUAL_DATE, CURRENCY_RK)
);
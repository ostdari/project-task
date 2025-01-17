CREATE TABLE IF NOT EXISTS DS.MD_ACCOUNT_D (
	DATA_ACTUAL_DATE DATE NOT NULL,
	DATA_ACTUAL_END_DATE DATE NOT NULL,
	ACCOUNT_RK INTEGER NOT NULL,
	ACCOUNT_NUMBER VARCHAR(20) NOT NULL,
	CHAR_TYPE VARCHAR(1) NOT NULL,
	CURRENCY_RK INTEGER NOT NULL,
	CURRENCY_CODE 	VARCHAR(3) NOT NULL,
	PRIMARY KEY (DATA_ACTUAL_DATE, ACCOUNT_RK)
);
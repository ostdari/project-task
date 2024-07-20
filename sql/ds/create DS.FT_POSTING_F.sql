CREATE TABLE IF NOT EXISTS DS.FT_POSTING_F (
	oper_date DATE not null,
	credit_account_rk integer not null,
	debet_account_rk integer not null,
	credit_amount FLOAT,
	debet_amount FLOAT
);
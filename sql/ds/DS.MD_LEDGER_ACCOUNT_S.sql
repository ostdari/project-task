CREATE TABLE IF NOT EXISTS DS.MD_LEDGER_ACCOUNT_S (
	CHAPTER CHAR(1),
	CHAPTER_NAME VARCHAR(16),
	SECTION_NUMBER INTEGER,
	SECTION_NAME VARCHAR(22),
	SUBSECTION_NAME VARCHAR(21),
	LEDGER1_ACCOUNT INTEGER,
	LEDGER1_ACCOUNT_NAME VARCHAR(47),
	LEDGER_ACCOUNT INTEGER NOT NULL,
	LEDGER_ACCOUNT_NAME VARCHAR(153),
	CHARACTERISTIC CHAR(1),
	IS_RESIDENT INTEGER,
	IS_RESERVE INTEGER,
	IS_RESERVED INTEGER,
	IS_LOAN INTEGER,
	IS_RESERVED_ASSETS INTEGER,
	IS_OVERDUE INTEGER,
	IS_INTEREST INTEGER,
	PAIR_ACCOUNT VARCHAR(5),
	START_DATE DATE NOT NULL,
	END_DATE DATE,
	IS_RUB_ONLY INTEGER,
	MIN_TERM VARCHAR(1),
	MIN_TERM_MEASURE VARCHAR(1),
	MAX_TERM VARCHAR(1),
	MAX_TERM_MEASURE VARCHAR(1),
	LEDGER_ACC_FULL_NAME_TRANSLIT VARCHAR(1),
	IS_REVALUATION VARCHAR(1),
	IS_CORRECT VARCHAR(1),
	PRIMARY KEY (LEDGER_ACCOUNT, START_DATE)
);
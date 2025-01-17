INSERT INTO
	DM.DM_ACCOUNT_BALANCE_F (ON_DATE, ACCOUNT_RK, BALANCE_OUT, BALANCE_OUT_RUB)
SELECT
	B.ON_DATE,
	B.ACCOUNT_RK,
	B.BALANCE_OUT,
	B.BALANCE_OUT * COALESCE(EX.REDUCED_COURCE, 1) AS BALANCE_OUT_RUB
FROM
	DS.FT_BALANCE_F B
	LEFT JOIN DS.MD_EXCHANGE_RATE_D EX ON B.CURRENCY_RK = EX.CURRENCY_RK
	AND B.ON_DATE BETWEEN EX.DATA_ACTUAL_DATE AND EX.DATA_ACTUAL_END_DATE
WHERE
	B.ON_DATE = '2017-12-31'
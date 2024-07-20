DO $$
DECLARE
    curr_date DATE;
BEGIN
    -- Начало цикла для каждой даты в январе
    FOR curr_date IN
        SELECT generate_series('2018-01-01'::DATE, '2018-01-31'::DATE, '1 day'::interval)
    LOOP
        -- Вызываем процедуру fill_account_balance_f для текущей даты
        CALL ds.fill_account_balance_f(curr_date);
    END LOOP; -- Завершение цикла
END $$;
CREATE TABLE logs.logs (
    file_name VARCHAR(50),
    table_name VARCHAR(50),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    row_count INTEGER,
	status varchar(20)
);
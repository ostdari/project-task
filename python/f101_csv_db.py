from pathlib import Path
import psycopg2
from datetime import datetime
import time

# Константы для подключения к базе
HOST = 'localhost'
DATABASE = 'project_task'
USER = 'postgres'
PASSWORD = '123'


# Запись в логи
def log_export(cur, log_table, csv, table, row_count, start_time, end_time):
    cur.execute(
        f"""
        INSERT INTO {log_table} (file_name, table_name, start_time, end_time, row_count)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (Path(csv).name + ' export', table.split('.')[-1],
         start_time, end_time, row_count)
    )


def f101_flow():
    # Подключения к базе данных
    conn = psycopg2.connect(
        host=HOST,
        database=DATABASE,
        user=USER,
        password=PASSWORD
    )

    # Путь к CSV файлу
    csv_file = r'C:\Users\Dasha\Desktop\project\csv files\dm _f101_round_f.csv'

    # Таблица экспорта данных
    data_table = 'DM.DM_F101_ROUND_F'

    # Таблица для логирования
    log_table = 'logs.import'
    cur = conn.cursor()
    try:
        # Время начала загрузки
        start_time = datetime.now()

        # Экспорт данных в CSV фай
        with open(csv_file, 'w', encoding='utf-8', errors='replace') as f:
            cur.copy_expert(
                f"COPY {data_table} TO STDOUT WITH CSV HEADER DELIMITER ';'", f)
            cur.execute(
                f"""
                    SELECT COUNT(1)
                    FROM {data_table}
                    """
            )
            row_count = cur.fetchone()[0]

        # Пауза 5 секунд
        time.sleep(5)

        # Время окончания загрузки
        end_time = datetime.now()

        # Запись данных о процессе экспорта в таблицу логирования
        log_export(cur, log_table, csv_file, data_table,
                   row_count, start_time, end_time)

        # Подтверждение всех изменений в базе данных
        conn.commit()

        print(
            f"Данные из таблицы {data_table } успешно импортированы в CSV файл {Path(csv_file).name}")
    except Exception as e:
        print(f"Произошла ошибка: {e}")
        conn.rollback()

    # Закрытие курсора и соединения
    cur.close()
    conn.close()


if __name__ == "__main__":
    f101_flow()

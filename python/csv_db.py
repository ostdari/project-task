from pathlib import Path
import psycopg2
import csv
from datetime import datetime
import time

# Константы для подключения к базе
HOST = 'localhost'
DATABASE = 'project_task'
USER = 'postgres'
PASSWORD = '123'


# Подсчет количества строк в csv файле без заголовка
def count_rows(csv_file):
    with open(csv_file, 'r', encoding='unicode_escape') as f:
        reader = csv.reader(f)
        return sum(1 for row in reader) - 1  # -1 для исключения заголовка


# Поиск колонок с датами
def find_date_columns(header):
    return [i for i, col in enumerate(header) if 'date' in col.lower()]


# Функция для преобразования даты в стандартный формат
def convert_to_standard_format(date_str):
    formats_to_try = ['%d.%m.%Y',  '%d-%m-%Y', '%Y-%m-%d']

    for fmt in formats_to_try:
        try:
            date_obj = datetime.strptime(date_str, fmt)
            return date_obj.strftime('%Y-%m-%d')
        except ValueError:
            continue

    raise ValueError("Невозможно преобразовать дату в нужный формат")


def import_csv_to_db_delete(cur, csv_file, table_name):
    cur.execute(
        f"""
            DELETE FROM {table_name}
            """
    )
    with open(csv_file, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.reader(f, delimiter=';')
        header = next(reader)  # Чтение заголовка
        date_columns = find_date_columns(header)  # Поиск колонок с датой

        for row in reader:
            # Замена пустых строк на None для обработки NULL значений
            row = [None if val == '' else val for val in row]

            # Преобразование строк даты в формат datetime
            for idx in date_columns:
                date_str = row[idx]
                if date_str:
                    # Приводим дату к формату yyyy-MM-dd
                    # print(row[idx], table_name)
                    row[idx] = convert_to_standard_format(date_str)

            cur.execute(
                f"""
                INSERT INTO {table_name} ({', '.join(header)}) 
                VALUES ({', '.join(['%s'] * len(row))})
                """,
                row
            )


# Импорт данных из CSV файла в таблицу базы данных
def import_csv_to_db(cur, csv_file, table_name, pk):
    with open(csv_file, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.reader(f, delimiter=';')
        header = next(reader)  # Чтение заголовка
        date_columns = find_date_columns(header)  # Поиск колонок с датой

        for row in reader:
            # Замена пустых строк на None для обработки NULL значений
            row = [None if val == '' else val for val in row]

            # Преобразование строк даты в формат datetime
            for idx in date_columns:
                date_str = row[idx]
                if date_str:
                    # Приводим дату к формату yyyy-MM-dd
                    # print(row[idx], table_name)
                    row[idx] = convert_to_standard_format(date_str)

            cur.execute(
                f"""
                INSERT INTO {table_name} ({', '.join(header)}) 
                VALUES ({', '.join(['%s'] * len(row))})
                ON CONFLICT ({', '.join(pk)}) 
                DO UPDATE SET 
                    {', '.join([f'{col} = EXCLUDED.{col}' for col in header])}
                """,
                row
            )


def import_csv_to_db_coding(cur, csv_file, table_name, pk):
    with open(csv_file, 'r', encoding='unicode_escape', errors='replace') as f:
        reader = csv.reader(f, delimiter=';')
        header = next(reader)  # Чтение заголовка
        date_columns = find_date_columns(header)  # Поиск колонок с датой

        for row in reader:
            # Замена пустых строк на None для обработки NULL значений
            row = [None if val == '' else val for val in row]

            # Преобразование строк даты в формат datetime
            for idx in date_columns:
                date_str = row[idx]
                if date_str:
                    # Приводим дату к формату yyyy-MM-dd
                    # print(row[idx], table_name)
                    row[idx] = convert_to_standard_format(date_str)

            cur.execute(
                f"""
                INSERT INTO {table_name} ({', '.join(header)}) 
                VALUES ({', '.join(['%s'] * len(row))})
                ON CONFLICT ({', '.join(pk)}) 
                DO UPDATE SET 
                    {', '.join([f'{col} = EXCLUDED.{col}' for col in header])}
                """,
                row
            )


# Запись в логи
def log_import(cur, log_table, csv, table, row_count, start_time, end_time):
    cur.execute(
        f"""
        INSERT INTO {log_table} (file_name, table_name, start_time, end_time, row_count)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (Path(csv).name, table.split('.')[-1],
         start_time, end_time, row_count)
    )


def basic_flow():
    # Подключения к базе данных
    conn = psycopg2.connect(
        host=HOST,
        database=DATABASE,
        user=USER,
        password=PASSWORD
    )

    # Пути к CSV файлам
    csv_files = [r'C:\Users\Dasha\Desktop\project\files\ft_balance_f.csv',
                 r'C:\Users\Dasha\Desktop\project\files\md_account_d.csv',
                 r'C:\Users\Dasha\Desktop\project\files\md_exchange_rate_d.csv',
                 r'C:\Users\Dasha\Desktop\project\files\md_ledger_account_s.csv',
                 r'C:\Users\Dasha\Desktop\project\files\md_currency_d.csv',
                 r'C:\Users\Dasha\Desktop\project\files\ft_posting_f.csv']

    # Таблицы для импорта данных
    data_tables = ['DS.FT_BALANCE_F', 'DS.MD_ACCOUNT_D',
                   'DS.MD_EXCHANGE_RATE_D', 'DS.MD_LEDGER_ACCOUNT_S', 'DS.MD_CURRENCY_D', 'DS.FT_POSTING_F']

    # Таблица для логирования
    log_table = 'logs.import'
    pks = [['ON_DATE', 'ACCOUNT_RK'], ['DATA_ACTUAL_DATE', 'ACCOUNT_RK'], [
        'DATA_ACTUAL_DATE', 'CURRENCY_RK'], ['LEDGER_ACCOUNT', 'START_DATE'], ['CURRENCY_RK', 'DATA_ACTUAL_DATE'], []]
    cur = conn.cursor()
    for data_table, csv_file, pk in zip(data_tables, csv_files, pks):
        try:
            # Подсчет количества строк в CSV файле
            row_count = count_rows(csv_file)

            # Время начала загрузки
            start_time = datetime.now()

            # Импорт данных из CSV файла в базу данных
            if Path(csv_file).name == 'ft_posting_f.csv':
                import_csv_to_db_delete(cur, csv_file, data_table)
            elif Path(csv_file).name == 'md_currency_d.csv':
                import_csv_to_db_coding(cur, csv_file, data_table, pk)
            else:
                import_csv_to_db(cur, csv_file, data_table, pk)

            # Пауза 5 секунд
            time.sleep(5)

            # Время окончания загрузки
            end_time = datetime.now()

            # Запись данных о процессе импорта в таблицу логирования
            log_import(cur, log_table, csv_file, data_table,
                       row_count, start_time, end_time)

            # Подтверждение всех изменений в базе данных
            conn.commit()

            print(
                f"Данные из файла {Path(csv_file).name} успешно импортированы в базу данных PostgreSQL в таблицу {data_table}")
        except Exception as e:
            print(f"Произошла ошибка: {e}")
            conn.rollback()

    # Закрытие курсора и соединения
    cur.close()
    conn.close()


if __name__ == "__main__":
    basic_flow()

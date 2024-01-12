
import psycopg2
import pandas as pd
from datetime import datetime
from config import host, user, password, db_name, port
import time


try:
    # подключение к базе Postgres
    connection = psycopg2.connect(
        host=host,
        user=user,
        password=password,
        database=db_name,
        port = port    
    )
    connection.autocommit = True
    
    with connection.cursor() as cursor: #в этом случае не надо закрывать курсор в конце
        cursor.execute(
            "SELECT version();"
        )
        
        print(f"[INFO] Соединение установлено. Server version: {cursor.fetchone()}")

    
    cursor = connection.cursor()


    table_name = "dds.md_currency_d"
    csv_filename = "md_currency_d.csv"

    df = pd.read_csv('C:/Users/veronika.ivanova/1.1/' + csv_filename, delimiter=';', encoding='ansi')


    start_time = datetime.now()
    cursor.execute("""
        INSERT INTO logs.info (operation_start, table_name, success)
        VALUES (%s, %s, %s)
    """, (start_time, table_name, '1'))
    
    time.sleep(5)

    cursor.execute("""SELECT COUNT(*) FROM dds.md_currency_d""")
    count_before_insert = cursor.fetchone()[0]

    for index, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT INTO dds.md_currency_d (currency_rk, data_actual_date, data_actual_end_date, currency_code, code_iso_char)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (CURRENCY_RK, DATA_ACTUAL_DATE)
                DO UPDATE SET data_actual_end_date = EXCLUDED.data_actual_end_date,
                currency_code = EXCLUDED.currency_code,
                code_iso_char = EXCLUDED.code_iso_char,
            """, (row['CURRENCY_RK'], row['DATA_ACTUAL_DATE'], row['DATA_ACTUAL_END_DATE'], row['CURRENCY_CODE'],row['CODE_ISO_CHAR'] ))
        except Exception as e:
            # Логирование ошибки при вставке
            cursor.execute("""
                UPDATE logs.info
                SET error_message = %s, success = FALSE, operation_end = %s
                WHERE table_name = %s AND operation_end IS NULL and id = (select max(id) from logs.info)
            """, (str(e), datetime.now(), table_name))
        else:
            cursor.execute("""
                UPDATE logs.info
                SET error_message = %s, success = TRUE, operation_end = %s
                WHERE table_name = %s AND operation_end IS NULL and id = (select max(id) from logs.info)
            """, ("Ok", datetime.now(), table_name))


    cursor.execute("""SELECT COUNT(*) FROM dds.md_currency_d""")
    count_after_insert = cursor.fetchone()[0]
    cursor.execute("""
                UPDATE logs.info
                SET count_before_insert = %s, count_after_insert = %s
                WHERE table_name = %s and id = (select max(id) from logs.info)
            """, (count_before_insert, count_after_insert, table_name))
    

    # with connection.cursor() as cursor: #в этом случае не надо закрывать курсор в конце
    #     cursor.execute(
    #          """INSERT INTO dds.ft_balance_f (on_date,account_rk,currency_rk,balance_out) SELECT
    #          ('ON_DATE', 'ACCOUNT_RK', 'CURRENCY_RK', 'BALANCE_OUT');"""
    #     )
        
except Exception as _ex:
    print("[INFO] Возникшая ошибка при работе с PostgreSQL", _ex)
finally:
    if connection:
        cursor.close()
        connection.close()
        print("[INFO] PostgreSQL соедениение закрыто")

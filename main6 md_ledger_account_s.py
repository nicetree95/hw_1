
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


    table_name = "dds.md_ledger_account_s"
    csv_filename = "md_ledger_account_s.csv"

    df = pd.read_csv('C:/Users/veronika.ivanova/1.1/' + csv_filename, delimiter=';')

    start_time = datetime.now()
    cursor.execute("""
        INSERT INTO logs.info (operation_start, table_name, success)
        VALUES (%s, %s, %s)
    """, (start_time, table_name, '1'))
    
    time.sleep(5)

    cursor.execute("""SELECT COUNT(*) FROM dds.md_ledger_account_s""")
    count_before_insert = cursor.fetchone()[0]

    for index, row in df.iterrows():
        try:
            cursor.execute("""
                INSERT INTO dds.md_ledger_account_s (chapter, chapter_name, section_number, section_name, subsection_name, ledger1_account, ledger1_account_name, ledger_account, ledger_account_name, characteristic, is_resident, is_reserve, is_reserved, is_loan, is_reserved_assets, is_overdue, is_interest, pair_account, start_date, end_date, is_rub_only, min_term, min_term_measure, max_term, max_term_measure, ledger_acc_full_name_translit, is_revaluation, is_correct)
                VALUES (%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s, %s, %s)
                ON CONFLICT (LEDGER_ACCOUNT, START_DATE)
                DO UPDATE SET chapter = EXCLUDED.chapter,
                chapter_name = EXCLUDED.chapter_name,
                section_number = EXCLUDED.section_number,
                subsection_name = EXCLUDED.subsection_name,          
                ledger1_account = EXCLUDED.ledger1_account,
                ledger1_account_name = EXCLUDED.ledger1_account_name,
                ledger_account_name = EXCLUDED.ledger_account_name, 
                characteristic = EXCLUDED.characteristic,
                is_resident = EXCLUDED.is_resident,
                is_reserve = EXCLUDED.is_reserve, 
                is_reserved = EXCLUDED.is_reserved,
                is_loan = EXCLUDED.is_loan,
                is_reserved_assets = EXCLUDED.is_reserved_assets, 
                is_overdue = EXCLUDED.is_overdue,
                is_interest = EXCLUDED.is_interest,
                pair_account = EXCLUDED.pair_account, 
                start_date = EXCLUDED.start_date,
                end_date = EXCLUDED.end_date,
                is_rub_only = EXCLUDED.is_rub_only, 
                min_term = EXCLUDED.min_term,
                min_term_measure = EXCLUDED.min_term_measure,
                max_term = EXCLUDED.max_term, 
                max_term_measure = EXCLUDED.max_term_measure, 
                ledger_acc_full_name_translit = EXCLUDED.ledger_acc_full_name_translit,
                is_revaluation = EXCLUDED.is_revaluation,
                is_correct = EXCLUDED.is_correct







            """, (row['CHAPTER'],	row['CHAPTER_NAME'],	row['SECTION_NUMBER'],	row['SECTION_NAME'],	row['SUBSECTION_NAME'],	row['LEDGER1_ACCOUNT'],	row['LEDGER1_ACCOUNT_NAME'],	row['LEDGER_ACCOUNT'],	row['LEDGER_ACCOUNT_NAME'],	row['CHARACTERISTIC'],	row['IS_RESIDENT'],	row['IS_RESERVE'],	row['IS_RESERVED'],	row['IS_LOAN'],	row['IS_RESERVED_ASSETS'],	row['IS_OVERDUE'],	row['IS_INTEREST'],	row['PAIR_ACCOUNT'],	row['START_DATE'],	row['END_DATE'],	row['IS_RUB_ONLY'],	row['MIN_TERM'],	row['MIN_TERM_MEASURE'],	row['MAX_TERM'],	row['MAX_TERM_MEASURE'],	row['LEDGER_ACC_FULL_NAME_TRANSLIT'],	row['IS_REVALUATION'],	row['IS_CORRECT']
 ))
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


    cursor.execute("""SELECT COUNT(*) FROM dds.md_ledger_account_s""")
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

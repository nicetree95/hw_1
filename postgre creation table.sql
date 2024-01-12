

CREATE SCHEMA IF NOT exists DDS;
CREATE SCHEMA IF NOT exists LOGS;

create user etl with password '1234';
grant connect on database "postgres" to etl;
--grant select, insert, update,delete on all tables in schema "dds","logs"  to etl;
GRANT ALL PRIVILEGES ON                  SCHEMA "dds","logs" TO etl;
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA "dds","logs" TO etl;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA "dds","logs" TO etl;


truncate table DDS.FT_BALANCE_F;
truncate table DDS.FT_POSTING_F;
truncate table DDS.MD_ACCOUNT_D;
truncate table DDS.MD_CURRENCY_D;
truncate table DDS.MD_EXCHANGE_RATE_D;
truncate table DDS.MD_LEDGER_ACCOUNT_S;
truncate table logs.info;


drop table if exists DDS.FT_BALANCE_F;
drop table if exists DDS.FT_POSTING_F;
drop table if exists DDS.MD_ACCOUNT_D;
drop table if exists DDS.MD_CURRENCY_D;
drop table if exists DDS.MD_EXCHANGE_RATE_D;
drop table if exists DDS.MD_LEDGER_ACCOUNT_S;
drop table if exists logs.info;



create table IF NOT exists DDS.FT_BALANCE_F
(
 	on_date date not null,
	account_rk numeric  not null,
	currency_rk numeric ,
	balance_out real,


   constraint FT_BALANCE_F_pk primary key (ON_DATE, ACCOUNT_RK)
);



create table IF NOT exists DDS.FT_POSTING_F
(
   oper_date date not null,
	credit_account_rk numeric  not null,
	debet_account_rk numeric  not null,
	credit_amount real,
	debet_amount real,
	constraint FT_POSTING_F_pk primary key (OPER_DATE, CREDIT_ACCOUNT_RK, DEBET_ACCOUNT_RK)
	

);

create table IF NOT exists DDS.MD_ACCOUNT_D
(
  data_actual_date date not null,
	data_actual_end_date date not null,
	account_rk numeric  not null,
	account_number VARCHAR(20) not null,
	char_type VARCHAR(1) not null,
	currency_rk numeric  not null,
	currency_code VARCHAR(3) not null,
	
	constraint MD_ACCOUNT_D_pk primary key (DATA_ACTUAL_DATE, ACCOUNT_RK)
);

create table IF NOT exists DDS.MD_CURRENCY_D
(
   currency_rk numeric  not null,
data_actual_date date not null,
data_actual_end_date date,
currency_code VARCHAR(3),
code_iso_char VARCHAR(3),

constraint MD_CURRENCY_D_pk primary key (CURRENCY_RK, DATA_ACTUAL_DATE)
);

create table IF NOT exists DDS.MD_EXCHANGE_RATE_D
(
data_actual_date date not null,
data_actual_end_date date,
currency_rk numeric  not null,
reduced_cource real,
code_iso_num VARCHAR(3),

constraint MD_EXCHANGE_RATE_D_pk primary key (DATA_ACTUAL_DATE, CURRENCY_RK)

);

create table IF NOT exists DDS.MD_LEDGER_ACCOUNT_S
(
chapter CHAR(1),
chapter_name VARCHAR(16),
section_number INTEGER,
section_name VARCHAR(22),
subsection_name VARCHAR(21),
ledger1_account INTEGER,
ledger1_account_name VARCHAR(47),
ledger_account INTEGER not null,
ledger_account_name VARCHAR(153),
characteristic CHAR(1),
is_resident INTEGER,
is_reserve INTEGER,
is_reserved INTEGER,
is_loan INTEGER,
is_reserved_assets INTEGER,
is_overdue INTEGER,
is_interest INTEGER,
pair_account VARCHAR(5),
start_date DATE not null,
end_date DATE,
is_rub_only INTEGER,
min_term VARCHAR(1),
min_term_measure VARCHAR(1),
max_term VARCHAR(1),
max_term_measure VARCHAR(1),
ledger_acc_full_name_translit VARCHAR(1),
is_revaluation VARCHAR(1),
is_correct VARCHAR(1),

constraint MD_LEDGER_ACCOUNT_S_pk primary key(LEDGER_ACCOUNT, START_DATE)
);

  CREATE TABLE IF NOT EXISTS logs.info (
        id SERIAL PRIMARY KEY,
        operation_start TIMESTAMP,
        operation_end TIMESTAMP,
        success BOOLEAN,
        error_message TEXT,
        table_name text,
        count_before_insert int,
        count_after_insert int
    )
--select * from DDS.MD_ACCOUNT_D 
    --select * from DDS.FT_BALANCE_F where account_rk = 36237725
    --select * from logs.info

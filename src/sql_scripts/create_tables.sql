BEGIN;
DROP TABLE IF EXISTS public.dq_assertions;
DROP TABLE IF EXISTS public.dq_tests;
DROP TABLE IF EXISTS public.dq_tables;


CREATE TABLE public.dq_tables
(
    is_deleted boolean NOT NULL DEFAULT false,
    last_modified timestamp NOT NULL DEFAULT NOW(),
    table_id serial NOT NULL,
    environment varchar(50) NOT NULL,
    db_name varchar(50) NOT NULL,
    schema_name varchar(50) NOT NULL,
    table_name varchar(100) NOT NULL,
    group_id integer NOT NULL,
    schema_json text,
    PRIMARY KEY (table_id),
    UNIQUE (db_name, schema_name, table_name, environment)
);

CREATE TABLE public.dq_columns
(
    is_deleted boolean NOT NULL DEFAULT false,
    last_modified timestamp NOT NULL DEFAULT NOW(),
    column_id serial NOT NULL,
    table_id integer NOT NULL,
    column_name varchar(100) NOT NULL,
    data_type text,
    PRIMARY KEY (column_id)
);

CREATE TABLE public.dq_assertions
(
    assertion_id serial NOT NULL,
    is_active boolean NOT NULL DEFAULT true,
    is_critical boolean NOT NULL DEFAULT true,
    check_all_rows boolean NOT NULL DEFAULT true,
    test_name varchar(1000) NOT NULL,
    table_full_name varchar(2000) NOT NULL,
    test_columns varchar(2000)[],
    last_modified timestamp NOT NULL DEFAULT NOW(),
    modified_by varchar(200) NOT NULL,
    min_value varchar(1000),
    max_value varchar(1000),
    date_formatter varchar(100),
    sql_query text,
    schema_json_expected text,
    source_table_full_name varchar(2000),
    connection_name varchar(50),
    delay_days integer DEFAULT 0,
    where_condition varchar(500),
    regexp text,
    PRIMARY KEY (assertion_id),
    UNIQUE (test_name, table_full_name, test_columns, sql_query)
);

CREATE TABLE public.dq_tests
(
    test_id serial NOT NULL,
    test_name varchar(200) NOT NULL,
    description varchar(500),
    last_modified timestamp NOT NULL DEFAULT NOW(),
    modified_by varchar(200) NOT NULL,
    is_deleted boolean NOT NULL DEFAULT false,
    category varchar(200) NOT NULL,
    scope varchar(200),
    std_operator varchar(200),
    std_aggregation varchar(200),
    PRIMARY KEY (test_id),
    UNIQUE (test_name)
);

ALTER TABLE public.dq_tables
    ADD FOREIGN KEY (source_table_id)
    REFERENCES public.dq_tables (table_id)
    NOT VALID;

ALTER TABLE public.dq_assertions
    ADD FOREIGN KEY (test_name)
    REFERENCES public.dq_tests (test_name)
    NOT VALID;

END;
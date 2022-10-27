import time
import psycopg2
import json
import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.com.linkedin.pegasus2avro.assertion import (
    AssertionResult,
    AssertionRunEvent,
    AssertionRunStatus,
)
from datahub.metadata.com.linkedin.pegasus2avro.events.metadata import ChangeType
from datahub.metadata.com.linkedin.pegasus2avro.timeseries import PartitionSpec


def get_data_via_jdbc(spark_session, connection, sql_query):
    """Get data from source table via jdbc with Spark"""
    if not connection:
        print('Can not get params for JDBC connection')
        return None
    if connection.conn_id.startswith('pg') or connection.conn_id.startswith('dq'):
        conn_type = 'postgresql'
        driver = 'org.postgresql.Driver'
        url = f"jdbc:{conn_type}://{connection.host}:{connection.port}/{connection.schema}"
    elif connection.conn_id.startswith('mssql'):
        conn_type = 'sqlserver'
        driver = 'com.microsoft.sqlserver.jdbc.SQLServerDriver'
        url = f"jdbc:{conn_type}://{connection.host}:{connection.port};databaseName={connection.schema}"
    elif connection.conn_id.startswith('mysql'):
        conn_type = 'mysql'
        driver = 'com.mysql.cj.jdbc.Driver'
        url = f"jdbc:{conn_type}://{connection.host}:{connection.port}/{connection.schema}"
    else:
        print('DQ System can not connect to the source DB. Please contact to DQ-developers')
        return None
    print(url)
    df = spark_session.read \
        .format('jdbc') \
        .options(url=url,
                 user=connection.login,
                 password=connection.password,
                 driver=driver,
                 query=sql_query) \
        .load()
    return df


def setup_pg_connection(user, password, host, port, database):
    """Set connection to PostgreSQL database via psycopg2 library"""
    conn = psycopg2.connect(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database)
    return conn


def get_data_from_parquet(spark_session, hdfs_location, sql_query, table_name, load_date=None):
    """Get data from parquet file with Spark"""
    if load_date:
        hdfs_location = f"{hdfs_location}/load_date={load_date}"
    try:
        # Check if TempView already exists in local Spark Metastore
        result_df = spark_session.sql(sql_query)
        count_rows = int(spark_session.sql(f"COUNT(*) FROM {table_name}").collect()[0][0])
    except Exception:
        try:
            target_df = spark_session.read.parquet(hdfs_location)
        except Exception:
            print(f"Can't read data from hdfs:///{hdfs_location}")
            return None, None
        target_df.createOrReplaceTempView(table_name)
        count_rows = target_df.count()
        spark_session.sql(f"CACHE TABLE {table_name}")
        result_df = spark_session.sql(sql_query)
    print(f"{count_rows} rows have been read from hdfs:///{hdfs_location}")
    result_df.show()
    return result_df, count_rows


def get_tests_from_dq_metastore(connection, table_full_name):
    """Get tests list for target table"""
    pg_connection = setup_pg_connection(database=connection.schema,
                                        user=connection.login,
                                        password=connection.password,
                                        host=connection.host,
                                        port=connection.port)
    target_columns = ['ass.assertion_id',
                      'ass.table_full_name',
                      'ass.date_formatter',
                      'ass.min_value',
                      'ass.max_value',
                      'ass.test_name',
                      'ass.sql_query',
                      'ass.delay_days',
                      'ass.is_critical',
                      'ass.schema_json_expected',
                      'ass.source_table_full_name',
                      'ass.connection_name',
                      'ass.check_all_rows',
                      'ass.check_all_rows',
                      'ass.test_columns',
                      'ass.regexp',
                      'ass.variation_percent',
                      'ass.averaging_interval',
                      'ass.reference_column',
                      'ass.description',
                      'ass.source_error',
                      'tst.category',
                      'tst.scope',
                      'tst.std_operator',
                      'tst.std_aggregation']
    sql_get_tests = f"""
        SELECT {', '.join(target_columns)}
        FROM dq_assertions ass  
        LEFT JOIN dq_tests tst ON ass.test_name = tst.test_name        
        WHERE ass.table_full_name = '{table_full_name}'
        AND ass.is_active = true        
    """
    cursor = pg_connection.cursor()
    try:
        cursor.execute(sql_get_tests)
        tests_info = cursor.fetchall()
        cursor.close()
    except Exception as e:
        cursor.close()
        pg_connection.close()
        raise Exception(e)
    # prepare all params for tests
    tests = []
    if tests_info:
        for row in tests_info:
            test = dict()
            for i, value in enumerate(row):
                test[target_columns[i][4:]] = value  # remove prefixes
            test['hdfs_location'] = f"/{test['table_full_name'].replace('.', '/')}"
            test['table_name'] = test['table_full_name'].split('.')[-1]
            if test['source_table_full_name']:
                test['source_schema_name'] = test['source_table_full_name'].split('.')[2]
                test['source_table_name'] = test['source_table_full_name'].split('.')[-1]
                test['source_hdfs_location'] = f"/{test['source_table_full_name'].replace('.', '/')}"
            tests.append(test)
    return tests


def get_metrics_from_dq_metastore(connection, table_full_name):
    """Get metrics list for target table"""
    pg_connection = setup_pg_connection(database=connection.schema,
                                        user=connection.login,
                                        password=connection.password,
                                        host=connection.host,
                                        port=connection.port)
    target_columns = ['prf.profiling_id',
                      'prf.metric_name',
                      'prf.table_full_name',
                      'prf.is_source',
                      'prf.deduplication_column',
                      'prf.test_columns',
                      'prf.delay_days',
                      'prf.where_condition',
                      'prf.connection_name',
                      'prf.is_incremental']
    sql_query = f"""
        SELECT {', '.join(target_columns)}
        FROM dq_profiling prf  
        WHERE prf.table_full_name = '{table_full_name}'
        AND prf.is_active = true        
    """
    cursor = pg_connection.cursor()
    try:
        cursor.execute(sql_query)
        metrics_info = cursor.fetchall()
        cursor.close()
        pg_connection.close()
    except Exception as e:
        cursor.close()
        pg_connection.close()
        raise Exception(e)
    # Prepare all params for metrics
    metrics = []
    if metrics_info:
        for row in metrics_info:
            metric = dict()
            for i, value in enumerate(row):
                metric[target_columns[i][4:]] = value
            metric['hdfs_location'] = f"/{metric['table_full_name'].replace('.', '/')}"
            metrics.append(metric)
    return metrics


def get_count_rows_from_pg(connection, schema):
    """Get count rows from target db and schema in PostgreSQL"""
    pg_connection = setup_pg_connection(database=connection.schema,
                                        user=connection.login,
                                        password=connection.password,
                                        host=connection.host,
                                        port=connection.port)
    sql = f"""
       select '{connection.schema}' as db_name, table_schema as schema_name, table_name, 
              (xpath('/row/cnt/text()', xml_count))[1]::text::int as row_count
       from (select table_name, 
             table_schema, 
             query_to_xml(format('select count(*) as cnt from %I.%I', table_schema, table_name), false, true, '') as xml_count
             from information_schema.tables
             where table_schema = '{schema}'
            ) t        
    """
    cursor = pg_connection.cursor()
    try:
        cursor.execute(sql)
        metrics_info = cursor.fetchall()
        cursor.close()
        pg_connection.close()
    except Exception as e:
        cursor.close()
        pg_connection.close()
        raise Exception(e)
    # Prepare all params for metrics
    metrics = []
    if metrics_info:
        for row in metrics_info:
            metric = dict()
            metric['table_full_name'] = f'{row[0]}.{row[1]}.{row[2]}'
            metric['metric_value'] = int(row[3])
            metrics.append(metric)
    return metrics


def save_results_to_hdfs(spark_session, data, path, schema, count_rows=10, write_mode='append'):
    """Write results from List to HDFS"""
    result_df = spark_session.createDataFrame(data=data, schema=schema)
    result_df.show(count_rows, False)
    result_df.coalesce(1) \
        .write.format("parquet") \
        .mode(write_mode) \
        .partitionBy("load_date") \
        .save(path)
    print(f'Results have been saved to: {path}')
    return result_df


def create_hive_table(spark, source_df, data_path, database, table_name):
    f_def = ""
    for f in source_df.dtypes:
        if f[0] != "load_date":
            f_def = f_def + ", " + f[0] + " " + \
                    f[1].replace(":", "`:").replace("<", "<`").replace(",", ",`").replace("array<`", "array<")
    table_define = "CREATE EXTERNAL TABLE IF NOT EXISTS " + database + "." + table_name + \
        " (" + f_def[1:] + ") " + "PARTITIONED BY (load_date string) STORED AS PARQUET " \
        "LOCATION 'hdfs://hacluster/" + data_path + "'"
    print(table_define)
    spark.sql(table_define)
    spark.sql("MSCK REPAIR TABLE " + database + "." + table_name)


def assertion_urn(assertion_id: str) -> str:
    return f"urn:li:assertion:{assertion_id}"


def dataset_urn(platform: str, tbl: str, env: str) -> str:
    return builder.make_dataset_urn(platform=platform, name=tbl, env=env.upper())


def emit_assertion_run_event(datahub_gms_server, log_time, assertion_id, column_name, platform,
                             table_full_name_with_env, status, count_rows_with_errors, sql_query, check_date,
                             check_all_rows):
    emitter = DatahubRestEmitter(datahub_gms_server)
    if not column_name or column_name == 'None':
        column_name = ''
    result_params = {'count_rows_with_errors': str(count_rows_with_errors)}
    runtime_context = {'sql_query': str(sql_query)}
    status = 'SUCCESS' if status or status is None else 'FAILURE'
    check_date = check_date.replace('-', '')
    partition_spec = PartitionSpec(partition=json.dumps([{"load_date": check_date}])) if not check_all_rows else None
    log_time_msec = time.mktime(time.strptime(log_time, "%Y-%m-%d %H:%M:%S")) * 1000
    env = table_full_name_with_env[:table_full_name_with_env.index('.')]
    table_full_name = table_full_name_with_env[table_full_name_with_env.index('.') + 1:]

    assertion_run_event = AssertionRunEvent(
        timestampMillis=int(log_time_msec),
        assertionUrn=assertion_urn(f"{assertion_id}{column_name}"),
        asserteeUrn=dataset_urn(platform=platform, tbl=table_full_name, env=env),
        partitionSpec=partition_spec,
        runId=f"{assertion_id}_{log_time_msec}",
        status=AssertionRunStatus.COMPLETE,
        result=AssertionResult(
            type=status,
            nativeResults=result_params,
        ),
        runtimeContext=runtime_context,
    )
    dataset_assertion_run_event_mcp = MetadataChangeProposalWrapper(
        entityType="assertion",
        changeType=ChangeType.UPSERT,
        entityUrn=assertion_run_event.assertionUrn,
        aspectName="assertionRunEvent",
        aspect=assertion_run_event,
    )
    # Emit BatchAssertion Result
    emitter.emit_mcp(dataset_assertion_run_event_mcp)

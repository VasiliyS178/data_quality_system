import time
import psycopg2
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
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
    """
    Get data from source table via jdbc with Spark
    :param connection:
    :param spark_session:
    :param sql_query:
    :return: DataFrame
    """
    if not connection:
        print('Can not get params for JDBC connection')
        return None

    if 'postgres' in connection.conn_type:
        conn_type = 'postgresql'
        driver = f'org.{conn_type}.Driver'
    else:
        print('DQ can connect only to PostgreSQL yet. Please contact to developers')
        return None
    url = f"jdbc:{conn_type}://{connection.host}:{connection.port}/{connection.schema}"
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
    """
    Set connection to PostgreSQL database via psycopg2 library
    :return: connection obj
    """
    conn = psycopg2.connect(
        user=user,
        password=password,
        host=host,
        port=port,
        database=database)
    return conn


def get_data_from_parquet(spark_session, hdfs_location, sql_query, table_name, datekey=None):
    """
    Get data from parquet file with Spark
    :param spark_session:
    :param hdfs_location:
    :param sql_query:
    :param table_name:
    :param datekey:
    :return: DataFrame with data
    """
    if datekey:
        hdfs_location = f"{hdfs_location}/datekey={datekey}"
    try:
        # Check if TempView already exists in local Spark Metastore
        result_df = spark_session.sql(sql_query)
    except Exception:
        try:
            target_df = spark_session.read.parquet(hdfs_location)
        except Exception:
            print(f"Can't read data from hdfs:///{hdfs_location}")
            return None
        target_df.createOrReplaceTempView(table_name)
        spark_session.sql(f"CACHE TABLE {table_name}")
        result_df = spark_session.sql(sql_query)
    result_df.show()
    return result_df


def get_tests_from_dq_metastore(connection, table_full_name):
    """Get tests list for target table"""
    pg_connection = setup_pg_connection(database=connection.schema,
                                        user=connection.login,
                                        password=connection.password,
                                        host=connection.host,
                                        port=connection.port)

    target_columns = ['ass.assertion_id', 'ass.table_full_name', 'ass.date_formatter', 'ass.min_value', 'ass.max_value',
                      'ass.test_name', 'ass.sql_query', 'ass.delay_days', 'ass.is_critical', 'ass.schema_json_expected',
                      'ass.check_all_rows', 'tst.category', 'tst.scope', 'tst.std_operator', 'tst.std_aggregation',
                      'ass.check_all_rows', 'ass.test_columns', 'ass.regexp']
    sql_get_tests = f"""
        SELECT {', '.join(target_columns)}
        FROM dq_assertions ass  
        LEFT JOIN dq_tests tst ON ass.test_name = tst.test_name        
        WHERE ass.table_full_name = '{table_full_name}'
        AND ass.is_active = true        
    """
    cursor = pg_connection.cursor()
    cursor.execute(sql_get_tests)
    tests_info = cursor.fetchall()
    cursor.close()

    # Getting info about source table for the target table
    sql_get_source_info = f"""
        SELECT table_full_name, where_condition, connection_name 
        FROM dq_assertions
        WHERE table_full_name = (SELECT table_full_name FROM dq_assertions 
                                 WHERE source_table_full_name = '{table_full_name}')        
    """
    cursor = pg_connection.cursor()
    cursor.execute(sql_get_source_info)
    source_info = cursor.fetchall()
    cursor.close()

    # Prepare all params for tests
    tests = []
    if tests_info:
        for row in tests_info:
            test = dict()
            for i, value in enumerate(row):
                test[target_columns[i][4:]] = value
            # Form HDFS path for table from: /environment/db_name/schema_name/table_name/
            test['hdfs_location'] = f"/{row[1].replace('.', '/')}"
            if source_info:
                test['source_schema_name'] = source_info[0][0].split('.')[-2]
                test['source_table_name'] = source_info[0][0].split('.')[-1]
                test['source_where_condition'] = source_info[0][1]
                test['source_connection_name'] = source_info[0][2]
                test['source_hdfs_location'] = \
                    f"/{source_info[0][0].replace('.', '/')}"
            tests.append(test)
    pg_connection.close()
    return tests


def save_results_to_hdfs(spark_session, data, path, count_rows=10, write_mode='append'):
    """
    Write results from List to HDFS as csv-file
    :param count_rows:
    :param data:
    :param spark_session:
    :param path:
    :param write_mode:
    :return: None
    """
    schema = StructType([StructField("assertion_id", StringType(), True),
                         StructField("test_name", StringType(), True),
                         StructField("table_full_name", StringType(), True),
                         StructField("column", StringType(), True),
                         StructField("is_critical", StringType(), True),
                         StructField("check_date", StringType(), True),
                         StructField("log_time", StringType(), True),
                         StructField("error", StringType(), True),
                         StructField("test_passed", StringType(), True),
                         StructField("sql_query", StringType(), True),
                         StructField("datekey", StringType(), True)
                         ])

    result_df = spark_session.createDataFrame(data=data, schema=schema)
    result_df.show(count_rows, False)
    result_df.coalesce(1) \
        .write.format("parquet") \
        .mode(write_mode) \
        .partitionBy("datekey") \
        .save(path)
    print(f'DQ tests results have been saved to: {path}')
    return result_df


def create_hive_table(spark, source_df, data_path, database, table_name):
    f_def = ""
    for f in source_df.dtypes:
        if f[0] != "datekey":
            f_def = f_def + ", " + f[0] + " " + \
                    f[1].replace(":", "`:").replace("<", "<`").replace(",", ",`").replace("array<`", "array<")
    table_define = "CREATE EXTERNAL TABLE IF NOT EXISTS " + database + "." + table_name + \
        " (" + f_def[1:] + ") " + "PARTITIONED BY (datekey string) STORED AS PARQUET " \
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
    partition_spec = PartitionSpec(partition=json.dumps([{"datekey": check_date}])) if not check_all_rows else None
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

import sys
import json
from datetime import datetime as dt, timedelta
from airflow.hooks.base import BaseHook
from pyspark.sql import SparkSession
from all_tests import run_data_quality_test
from all_tests import DQTest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, FloatType
from utils import save_results_to_hdfs, get_tests_from_dq_metastore, emit_assertion_run_event, create_hive_table


def main():
    # get tests to run
    try:
        tests = get_tests_from_dq_metastore(connection=DQ_CONNECTION,
                                            table_full_name=TABLE_FULL_NAME)
    except Exception as e:
        print("Error while connecting to DQ database")
        print(str(e))
        return 1

    if not tests:
        print(f"\nThere are no tests for table {TABLE_FULL_NAME} in DQ metastore\n")
        return 0

    # Create Spark session
    spark = SparkSession \
        .builder \
        .appName(f"Checking DQ for {TABLE_FULL_NAME}") \
        .config("hive.metastore.uris", HIVE_URIS) \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Prepare parameters to run test
    results = []
    cnt_fld_critical, cnt_fld, cnt_skipped, cnt_success = 0, 0, 0, 0
    for test in tests:
        # prepare parameters to run test
        dq_test = DQTest(**test)
        dq_test.log_time = dt.now().strftime('%Y-%m-%d %H:%M:%S')
        dq_test.check_date = str((dt.strptime(EXECUTION_DATE, '%Y-%m-%d') -
                                  timedelta(days=int(test.get('delay_days')))).date())
        dq_test.load_date = LOAD_DATE
        dq_test.filter_datekey = None if dq_test.check_all_rows else dq_test.check_date.replace('-', '')
        dq_test.metrics_table = METRICS_HIVE_TABLE
        dq_test.metrics_hdfs_path = METRICS_HDFS_PATH
        dq_test.schema_name = TABLE_FULL_NAME.split('.')[2]
        dq_test.spark_session = spark
        if not dq_test.test_columns or dq_test.test_columns == 'None':
            dq_test.test_columns = [None]
        for column in dq_test.test_columns:
            try:
                dq_test.status = None
                dq_test.count_rows_with_errors = None
                dq_test.percent_errors = None
                dq_test.clear_query = None
                dq_test.column_name = column
                run_data_quality_test(dq_test)  # run test for target table and column
                print(f"{dq_test.test_name} for {dq_test.column_name} status: {dq_test.status}")
                print(f"{dq_test.count_rows} rows checked, "
                      f"{dq_test.count_rows_with_errors} rows with errors have been found")
                if dq_test.count_rows_with_errors is not None and dq_test.count_rows is not None:
                    if dq_test.count_rows != 0:
                        percent_errors = round(int(dq_test.count_rows_with_errors) / float(dq_test.count_rows) * 100, 2)
                    else:
                        percent_errors = 0.0
                    print(f"{percent_errors}% rows with errors have been found\n")
                    dq_test.percent_errors = percent_errors
                clear_query = " ".join([x for x in str(dq_test.sql_query)
                                       .replace('\n', '').replace('\r', '').replace('\t', '')
                                       .replace('{check_date}', dq_test.check_date)
                                       .replace('{table_name}', dq_test.table_name)
                                       .replace('{column_name}', str(column))
                                       .split(" ") if len(x) > 0])
                dq_test.clear_query = clear_query
                if dq_test.status is None:
                    cnt_skipped += 1
                elif dq_test.status:
                    cnt_success += 1
                elif not dq_test.status and not dq_test.is_critical:
                    cnt_fld += 1
                else:
                    cnt_fld_critical += 1
            except Exception as e:
                cnt_skipped += 1
                dq_test.error = str(e)[:200]
                print(e)
            finally:
                results.append((
                    dq_test.category,
                    dq_test.description,
                    dq_test.is_critical,
                    dq_test.source_error,
                    dq_test.schema_name,
                    dq_test.table_full_name,
                    dq_test.column_name,
                    dq_test.check_date,
                    dq_test.status,
                    dq_test.count_rows,
                    dq_test.count_rows_with_errors,
                    dq_test.percent_errors,
                    dq_test.clear_query,
                    dq_test.assertion_id,
                    dq_test.test_name,
                    dq_test.error,
                    dq_test.log_time,
                    dq_test.load_date
                ))
            # emit assertion event to DataHub
            if dq_test.status is not None:
                try:
                    emit_assertion_run_event(datahub_gms_server=DATAHUB_GMS_SERVER,
                                             log_time=dq_test.log_time,
                                             assertion_id=dq_test.assertion_id,
                                             column_name=dq_test.column_name,
                                             platform=PLATFORM_NAME,
                                             table_full_name_with_env=dq_test.table_full_name,
                                             status=dq_test.status,
                                             count_rows_with_errors=str(dq_test.count_rows_with_errors),
                                             sql_query=str(dq_test.sql_query),
                                             check_date=dq_test.check_date,
                                             check_all_rows=dq_test.check_all_rows)
                except Exception as e:
                    print("Can't emit assertion event to DataHub", e)

    # prepare Spark DataFrame with all results and write it to HDFS
    total_tests = cnt_success + cnt_fld + cnt_fld_critical + cnt_skipped
    schema = StructType([StructField("test_category", StringType(), True),
                         StructField("test_description", StringType(), True),
                         StructField("is_critical", BooleanType(), True),
                         StructField("source_error", StringType(), True),
                         StructField("source_name", StringType(), True),
                         StructField("table_full_name", StringType(), True),
                         StructField("column_name", StringType(), True),
                         StructField("check_date", StringType(), True),
                         StructField("test_status", BooleanType(), True),
                         StructField("count_rows", IntegerType(), True),
                         StructField("count_errors", IntegerType(), True),
                         StructField("percent_errors", FloatType(), True),
                         StructField("sql_query", StringType(), True),
                         StructField("ass_id", IntegerType(), True),
                         StructField("test_name", StringType(), True),
                         StructField("error_msg", StringType(), True),
                         StructField("log_time", StringType(), True),
                         StructField("load_date", StringType(), True)])
    results_df = save_results_to_hdfs(spark_session=spark, 
                                      data=results, 
                                      path=DQ_RESULTS_HDFS_PATH,
                                      count_rows=total_tests, 
                                      schema=schema)
    create_hive_table(spark=spark, 
                      source_df=results_df, 
                      data_path=DQ_RESULTS_HDFS_PATH, 
                      database=TARGET_HIVE_DB,
                      table_name=TARGET_TABLE)

    # analyse results of data quality tests
    print(f"Passed tests: {cnt_success}\n"
          f"Failed critical tests: {cnt_fld_critical}\n"
          f"Failed not critical tests: {cnt_fld}\n"
          f"Skipped tests: {cnt_skipped}\n")
    if cnt_fld_critical > 0:
        print(f'\n\n{cnt_fld_critical} critical test(s) has been failed!\n\n')
    spark.stop()


if __name__ == '__main__':
    TABLE_FULL_NAME = sys.argv[1]
    EXECUTION_DATE = sys.argv[2][:10]
    with open("dq_config.json", encoding='utf-8') as file:
        dq_config = json.load(file)
    HIVE_URIS = dq_config.get('hive_metastore_uris')
    METRICS_HIVE_TABLE = dq_config.get('METRICS_HIVE_TABLE')
    METRICS_HDFS_PATH = dq_config.get('METRICS_HDFS_PATH')
    DQ_RESULTS_HDFS_PATH = dq_config.get('DQ_RESULTS_HDFS_PATH')
    DQ_CONNECTION_ID = dq_config.get('DQ_CONNECTION_ID')
    APP_NAME = dq_config.get('APP_NAME')
    TARGET_HIVE_DB = dq_config.get('TARGET_HIVE_DB')
    TARGET_TABLE = dq_config.get('TARGET_TABLE')
    DATAHUB_GMS_SERVER = dq_config.get('DATAHUB_GMS_SERVER')
    PLATFORM_NAME = dq_config.get('PLATFORM_NAME')
    LOAD_DATE = dt.now().strftime('%Y%m%d')
    DQ_CONNECTION = BaseHook.get_connection(conn_id=DQ_CONNECTION_ID)
    
    main()

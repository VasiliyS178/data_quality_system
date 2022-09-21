import sys
import json
from datetime import datetime as dt, timedelta
from pyspark.sql import SparkSession
from airflow.hooks.base import BaseHook
from all_tests import run_data_quality_test
from all_tests import DQTest
from utils import save_results_to_hdfs, get_tests_from_dq_metastore, emit_assertion_run_event, create_hive_table

if __name__ == '__main__':
    TABLE_FULL_NAME = sys.argv[1]
    EXECUTION_DATE = sys.argv[2]
    # Get target columns to bild mart and columns' types to convert data types
    with open("dq_config.json", encoding='utf-8') as file:
        dq_config = json.load(file)
    HIVE_URIS = dq_config.get('hive_metastore_uris')
    DQ_RESULTS_HDFS_PATH = dq_config.get('DQ_RESULTS_HDFS_PATH')
    DQ_CONNECTION_ID = dq_config.get('DQ_CONNECTION_ID')
    APP_NAME = dq_config.get('APP_NAME')
    TARGET_HIVE_DB = dq_config.get('TARGET_HIVE_DB')
    TARGET_TABLE = dq_config.get('TARGET_TABLE')
    DATAHUB_GMS_SERVER = dq_config.get('DATAHUB_GMS_SERVER')
    PLATFORM_NAME = dq_config.get('PLATFORM_NAME')
    DATEKEY = dt.now().strftime('%Y%m%d')
    DQ_CONNECTION = BaseHook.get_connection(conn_id=DQ_CONNECTION_ID)

    # get tests to run
    tests = []
    try:
        tests = get_tests_from_dq_metastore(connection=DQ_CONNECTION,
                                            table_full_name=TABLE_FULL_NAME)
    except Exception as e:
        print("Error while connecting to DQ database")
        print(str(e))
        sys.exit(1)

    if not tests:
        print(f"\nThere are no tests for table {TABLE_FULL_NAME} in DQ metastore\n")
        sys.exit(0)

    # Create Spark session
    spark = SparkSession \
        .builder \
        .appName("load_sberindex_data") \
        .config("hive.metastore.uris", HIVE_URIS) \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # Prepare parameters to run test
    results = []
    cnt_fld_critical = 0
    cnt_fld = 0
    cnt_skipped = 0
    cnt_success = 0
    for test in tests:
        test_result = []
        log_time = dt.now().strftime('%Y-%m-%d %H:%M:%S')
        check_date = str((dt.strptime(EXECUTION_DATE, '%Y-%m-%d') -
                          timedelta(days=int(test.get('delay_days')))).date())
        test['check_date'] = check_date
        test['table_name'] = TABLE_FULL_NAME.split('.')[-1]
        dq_test = DQTest(**test)
        dq_test.spark_session = spark
        dq_test.dq_connection = DQ_CONNECTION
        dq_test.source_connection = BaseHook.get(conn_id=test.get('source_connection_name'))
        if not dq_test.test_columns or dq_test.test_columns == 'None':
            dq_test.test_columns = ['None']
        for column in dq_test.test_columns:
            try:
                dq_test.column_name = column
                # Run data quality test for target table and column
                run_data_quality_test(dq_test)
                test['status'] = dq_test.status
                test['error'] = dq_test.error
                print("test['is_critical']: ", test['is_critical'])
                print("test['status']: ", test['status'])
                # Prepare SQL-query to write in result table
                clear_query = str(dq_test.sql_query).replace('\n', '').replace('\r', '').replace('\t', '').strip()
                if test['status'] is None:
                    cnt_skipped += 1
                    test['sql_query'] = clear_query
                elif test['status']:
                    cnt_success += 1
                    test['sql_query'] = None
                elif not test['status'] and not test['is_critical']:
                    cnt_fld += 1
                    test['sql_query'] = clear_query
                else:
                    cnt_fld_critical += 1
                    test['sql_query'] = clear_query
            except Exception as e:
                cnt_skipped += 1
                test['status'] = None
                test['error'] = str(e)
                test['sql_query'] = str(dq_test.sql_query).replace('\n', '').replace('\r', '').replace('\t', '').strip()
                print(e)
            finally:
                results.append((test.get('assertion_id'), test.get('test_name'), test.get('table_full_name'),
                                column, test.get('is_critical'), check_date, log_time, test.get('error'),
                                test.get('status'), test.get('sql_query'), DATEKEY))
            # Emit assertion event to DataHub
            if test['status'] is not None:
                try:
                    emit_assertion_run_event(datahub_gms_server=DATAHUB_GMS_SERVER,
                                             log_time=log_time,
                                             assertion_id=dq_test.assertion_id,
                                             column_name=column,
                                             platform=PLATFORM_NAME,
                                             table_full_name_with_env=TABLE_FULL_NAME,
                                             status=test['status'],
                                             count_rows_with_errors=str(dq_test.count_rows_with_errors),
                                             sql_query=str(dq_test.sql_query),
                                             check_date=check_date,
                                             check_all_rows=dq_test.check_all_rows)
                except Exception as e:
                    print("Can't emit assertion event to DataHub\n", e)

    # Prepare Spark DataFrame with all results and write it to HDFS
    total_tests = cnt_success + cnt_fld + cnt_fld_critical + cnt_skipped
    results_df = save_results_to_hdfs(spark_session=spark, data=results, path=DQ_RESULTS_HDFS_PATH,
                                      count_rows=total_tests)
    create_hive_table(spark=spark, source_df=results_df, data_path=DQ_RESULTS_HDFS_PATH, database=TARGET_HIVE_DB,
                      table_name=TARGET_TABLE)

    # Analyse results of data quality tests
    print(f"Passed tests: {cnt_success}\n"
          f"Failed critical tests: {cnt_fld_critical}\n"
          f"Failed not critical tests: {cnt_fld}\n"
          f"Skipped tests: {cnt_skipped}\n")
    if cnt_fld_critical > 0:
        spark.stop()
        raise Exception(f'\n{cnt_fld_critical} critical test(s) has been failed. ETL stopped\n')
    spark.stop()

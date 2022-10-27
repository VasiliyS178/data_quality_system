import sys
import json
from datetime import datetime as dt, timedelta
from airflow.hooks.base import BaseHook
from pyspark.sql import SparkSession
from all_metrics import run_profiling
from all_metrics import ProfilingMetric
from pyspark.sql.types import StructType, StructField, StringType
from utils import save_results_to_hdfs, get_metrics_from_dq_metastore, create_hive_table


def main():
    # get profiling metrics to run
    try:
        metrics = get_metrics_from_dq_metastore(connection=DQ_CONNECTION,
                                                table_full_name=TABLE_FULL_NAME)
    except Exception as e:
        print("Error while connecting to DQ database")
        print(str(e))
        sys.exit(1)

    if not metrics:
        print(f"\nThere are no profiling metrics for table {TABLE_FULL_NAME} in DQ metastore\n")
        sys.exit(0)

    # create Spark session
    spark = SparkSession \
        .builder \
        .appName(f"Profiling {TABLE_FULL_NAME}") \
        .config("hive.metastore.uris", HIVE_URIS) \
        .enableHiveSupport() \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # prepare parameters to run test
    results = []
    cnt_skipped = 0
    cnt_success = 0
    # run all profiling metrics for target table
    for metric in metrics:
        profiling_metric = ProfilingMetric(**metric)
        profiling_metric.log_time = dt.now().strftime('%Y-%m-%d %H:%M:%S')
        profiling_metric.check_date = str((dt.strptime(EXECUTION_DATE, '%Y-%m-%d') -
                                           timedelta(days=int(metric.get('delay_days')))).date())
        profiling_metric.load_date = LOAD_DATE
        profiling_metric.table_name = TABLE_FULL_NAME.split('.')[-1]
        profiling_metric.schema_name = TABLE_FULL_NAME.split('.')[2]
        profiling_metric.spark_session = spark
        profiling_metric.connection = BaseHook.get_connection(conn_id=metric.get('connection_name'))
        if profiling_metric.metric_name == 'count_rows_in_source_tables':
            try:
                tables_info = run_profiling(profiling_metric)
                if tables_info:
                    for table in tables_info:
                        results.append((profiling_metric.metric_name,
                                        f"{table['db_name']}.{table['schema_name']}.{table['table_name']}",
                                        None,
                                        profiling_metric.check_date,
                                        profiling_metric.log_time,
                                        str(table['row_count']),
                                        profiling_metric.load_date))
                    cnt_success += 1
                else:
                    cnt_skipped += 1
            except Exception as e:
                cnt_skipped += 1
                print(e)
            continue
        if not profiling_metric.test_columns or profiling_metric.test_columns == 'None':
            profiling_metric.test_columns = [None]
        for column in profiling_metric.test_columns:
            try:
                profiling_metric.column_name = column
                profiling_metric.metric_value = None
                run_profiling(profiling_metric)
                if profiling_metric.metric_value is None:
                    print(f"{profiling_metric.metric_name} has been skipped\n")
                    cnt_skipped += 1
                else:
                    results.append((
                        profiling_metric.metric_name,
                        profiling_metric.table_full_name,
                        profiling_metric.column_name,
                        profiling_metric.check_date,
                        profiling_metric.log_time,
                        profiling_metric.metric_value,
                        profiling_metric.load_date
                    ))
                    cnt_success += 1
            except Exception as e:
                cnt_skipped += 1
                print(e)
    # analyse results of calculating metrics
    total_metrics = cnt_success + cnt_skipped
    print(f"Calculated metrics: {cnt_success}\n"
          f"Skipped metrics: {cnt_skipped}\n")
    if not results:
        print("There are no results of metrics to save")
        spark.stop()
        sys.exit(0)

    # prepare Spark DataFrame with all results and write it to HDFS
    schema = StructType([StructField("metric_name", StringType(), True),
                         StructField("table_full_name", StringType(), True),
                         StructField("column_name", StringType(), True),
                         StructField("check_date", StringType(), True),
                         StructField("log_time", StringType(), True),
                         StructField("metric_value", StringType(), True),
                         StructField("load_date", StringType(), True)])
    results_df = save_results_to_hdfs(spark_session=spark, 
                                      data=results, 
                                      path=METRICS_HDFS_PATH,
                                      count_rows=total_metrics, 
                                      schema=schema)
    create_hive_table(spark=spark, 
                      source_df=results_df, 
                      data_path=METRICS_HDFS_PATH, 
                      database=TARGET_HIVE_DB,
                      table_name=METRICS_HIVE_TABLE)
    spark.stop()


if __name__ == '__main__':
    TABLE_FULL_NAME = sys.argv[1]
    EXECUTION_DATE = sys.argv[2][:10]
    with open("dq_config.json", encoding='utf-8') as file:
        dq_config = json.load(file)
    HIVE_URIS = dq_config.get('hive_metastore_uris')
    METRICS_HDFS_PATH = dq_config.get('METRICS_HDFS_PATH')
    DQ_CONNECTION_ID = dq_config.get('DQ_CONNECTION_ID')
    TARGET_HIVE_DB = dq_config.get('TARGET_HIVE_DB')
    METRICS_HIVE_TABLE = dq_config.get('METRICS_HIVE_TABLE')
    LOAD_DATE = dt.now().strftime('%Y%m%d')
    DQ_CONNECTION = BaseHook.get_connection(conn_id=DQ_CONNECTION_ID)

    main()

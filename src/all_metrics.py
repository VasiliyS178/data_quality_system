"""
Profiling Metrics Repository
"""
from utils import get_data_via_jdbc, get_data_from_parquet


class ProfilingMetric:
    def __init__(self, **kwargs):
        self.check_date = None
        self.column_name = None
        self.connection = None
        self.count_rows = None
        self.date_formatter = None
        self.load_date = None
        self.deduplication_column = None
        self.error = None
        self.hdfs_location = None
        self.is_source = None
        self.is_incremental = None
        self.log_time = None
        self.metric_name = None
        self.metric_value = None
        self.profiling_id = None
        self.schema_name = None
        self.spark_session = None
        self.table_name = None
        self.table_full_name = None
        self.test_columns = None
        self.where_condition = None
        for key, value in kwargs.items():
            setattr(self, key, value)

    def count_rows_in_source_table(self):
        """Count rows in the source table"""
        source_sql_query = f"""
            SELECT COUNT(*) AS cnt_rows
            FROM {self.schema_name}.{self.table_name}
            {'' if not self.where_condition else 'WHERE ' + self.where_condition} 
        """
        print('source_sql_query:\n', source_sql_query)
        source_df = get_data_via_jdbc(spark_session=self.spark_session,
                                      connection=self.connection,
                                      sql_query=source_sql_query)
        collected_df = source_df.collect() if source_df else []
        self.metric_value = str(collected_df[0][0]) if collected_df else None

    def count_rows_in_source_tables(self):
        """Count rows in all source tables in the specific schema"""
        if self.connection.conn_id.startswith('pg') or self.connection.conn_id.startswith('dq'):
            sql = f"""
                SELECT '{self.connection.schema}' AS db_name, 
                    LOWER(table_schema) AS schema_name, 
                    LOWER(table_name) AS table_name, 
                    (xpath('/row/cnt/text()', xml_count))[1]::text::int AS row_count
                FROM (SELECT table_name, table_schema, 
                      query_to_xml(format('select count(*) as cnt from %I.%I', table_schema, table_name), false, true, '') AS xml_count
                      FROM information_schema.tables
                      WHERE lower(table_schema) = '{self.schema_name}'
                     ) t        
            """
        elif self.connection.conn_id.startswith('mssql'):
            sql = f"""
                SELECT '{self.connection.schema.lower()}' AS db_name, LOWER(sc.name) AS schema_name, 
                    LOWER(t.name) AS table_name, s.row_count
                FROM sys.tables t
                JOIN sys.dm_db_partition_stats s ON t.object_id = s.object_id
                JOIN sys.schemas sc ON t.schema_id = sc.schema_id
                WHERE LOWER(sc.name) = '{self.schema_name}'
                AND t.type_desc = 'USER_TABLE'
                AND t.name NOT LIKE '%dss%'
                AND s.index_id IN (0,1)               
            """
        else:
            print('DQ System can not connect to the source DB. Please contact to DQ-developers')
            return []
        count_rows_by_tables_df = get_data_via_jdbc(spark_session=self.spark_session,
                                                    connection=self.connection,
                                                    sql_query=sql)
        collected_count_rows = count_rows_by_tables_df.collect() if count_rows_by_tables_df else []
        return collected_count_rows

    def count_rows_in_target_table(self):
        """Count rows in the target table"""
        count_field = f'DISTINCT({self.deduplication_column})' if self.deduplication_column else '*'
        check_date = None if self.is_incremental else self.check_date.replace('-', '')
        sql_query = f"""
            SELECT COUNT({count_field})        
            FROM {self.table_name} 
            {'' if not self.where_condition else 'WHERE ' + self.where_condition}
        """
        print(sql_query)
        result_df, _ = get_data_from_parquet(spark_session=self.spark_session,
                                             hdfs_location=self.hdfs_location,
                                             sql_query=sql_query,
                                             table_name=self.table_name,
                                             load_date=check_date)
        collected_df = result_df.collect() if result_df else []
        self.metric_value = str(collected_df[0][0]) if collected_df else None

    def count_null_values_by_column_in_target_table(self):
        """Count null values in the specific column in the target table"""
        check_date = None if self.is_incremental else self.check_date.replace('-', '')
        sql_query = f"""
            SELECT COUNT(*)      
            FROM {self.table_name} 
            WHERE {'' if not self.where_condition else self.where_condition + ' AND'} 
            {self.column_name} IS NULL
        """
        print(sql_query)
        result_df, self.count_rows = get_data_from_parquet(spark_session=self.spark_session,
                                                           hdfs_location=self.hdfs_location,
                                                           sql_query=sql_query,
                                                           table_name=self.table_name,
                                                           load_date=check_date)
        collected_df = result_df.collect() if result_df else []
        count_nulls = collected_df[0][0] if collected_df else None
        print("COUNT NULL values: ", count_nulls)
        self.metric_value = str(count_nulls) if count_nulls is not None else None

    def count_sum_by_column_in_target_table(self):
        """Count sum in the specific column in the target table"""
        check_date = None if self.is_incremental else self.check_date.replace('-', '')
        sql_query = f"""
            SELECT SUM(CAST({self.column_name} AS DECIMAL(38, 2)))
            FROM {self.table_name} 
            WHERE {'' if not self.where_condition else self.where_condition + ' AND'} 
            {self.column_name} IS NOT NULL
        """
        print(sql_query)
        result_df, self.count_rows = get_data_from_parquet(spark_session=self.spark_session,
                                                           hdfs_location=self.hdfs_location,
                                                           sql_query=sql_query,
                                                           table_name=self.table_name,
                                                           load_date=check_date)
        collected_df = result_df.collect() if result_df else []
        total_sum = collected_df[0][0] if collected_df else None
        print(f"SUM by {self.column_name}: ", total_sum)
        self.metric_value = str(total_sum) if total_sum is not None else None

    def count_sum_by_column_in_source_table(self):
        """Count sum in the specific column in the source table"""
        source_sql_query = f"""
            SELECT SUM({self.column_name})
            FROM {self.schema_name}.{self.table_name}
            WHERE {'' if not self.where_condition else self.where_condition + ' AND'} 
            {self.column_name} IS NOT NULL
        """
        print('source_sql_query:\n', source_sql_query)
        source_df = get_data_via_jdbc(spark_session=self.spark_session,
                                      connection=self.connection,
                                      sql_query=source_sql_query)
        collected_df = source_df.collect() if source_df else []
        total_sum = collected_df[0][0] if collected_df else None
        print(f"SUM by {self.column_name}: ", total_sum)
        self.metric_value = str(total_sum) if total_sum is not None else None

    def count_min_by_column_in_source_table(self):
        """Count min in the specific column in the source table"""
        source_sql_query = f"""
            SELECT MIN({self.column_name})
            FROM {self.schema_name}.{self.table_name}
            WHERE {'' if not self.where_condition else self.where_condition + ' AND'} 
            {self.column_name} IS NOT NULL
        """
        print('source_sql_query:\n', source_sql_query)
        source_df = get_data_via_jdbc(spark_session=self.spark_session,
                                      connection=self.connection,
                                      sql_query=source_sql_query)
        collected_df = source_df.collect() if source_df else []
        min_value = collected_df[0][0] if collected_df else None
        print(f"MIN value in {self.column_name}: ", min_value)
        self.metric_value = str(min_value) if min_value is not None else None

    def count_max_by_column_in_source_table(self):
        """Count max in the specific column in the source table"""
        source_sql_query = f"""
            SELECT MAX({self.column_name})
            FROM {self.schema_name}.{self.table_name}
            WHERE {'' if not self.where_condition else self.where_condition + ' AND'} 
            {self.column_name} IS NOT NULL
        """
        print('source_sql_query:\n', source_sql_query)
        source_df = get_data_via_jdbc(spark_session=self.spark_session,
                                      connection=self.connection,
                                      sql_query=source_sql_query)
        collected_df = source_df.collect() if source_df else []
        max_value = collected_df[0][0] if collected_df else None
        print(f"MAX value in {self.column_name}: ", max_value)
        self.metric_value = str(max_value) if max_value is not None else None

    def count_min_by_column_in_target_table(self):
        """Count min value in the specific column in the target table"""
        check_date = None if self.is_incremental else self.check_date.replace('-', '')
        sql_query = f"""
            SELECT MIN(CAST({self.column_name} AS DECIMAL(38, 2)))      
            FROM {self.table_name} 
            WHERE {'' if not self.where_condition else self.where_condition + ' AND'} 
            {self.column_name} IS NOT NULL
        """
        print(sql_query)
        result_df, self.count_rows = get_data_from_parquet(spark_session=self.spark_session,
                                                           hdfs_location=self.hdfs_location,
                                                           sql_query=sql_query,
                                                           table_name=self.table_name,
                                                           load_date=check_date)
        collected_df = result_df.collect() if result_df else []
        min_value = collected_df[0][0] if collected_df else None
        print(f"MIN value in {self.column_name}: ", min_value)
        self.metric_value = str(min_value) if min_value is not None else None

    def count_max_by_column_in_target_table(self):
        """Count max value in the specific column in the target table"""
        check_date = None if self.is_incremental else self.check_date.replace('-', '')
        sql_query = f"""
            SELECT MAX(CAST({self.column_name} AS DECIMAL(38, 2)))      
            FROM {self.table_name} 
            WHERE {'' if not self.where_condition else self.where_condition + ' AND'} 
            {self.column_name} IS NOT NULL
        """
        print(sql_query)
        result_df, self.count_rows = get_data_from_parquet(spark_session=self.spark_session,
                                                           hdfs_location=self.hdfs_location,
                                                           sql_query=sql_query,
                                                           table_name=self.table_name,
                                                           load_date=check_date)
        collected_df = result_df.collect() if result_df else []
        max_value = collected_df[0][0] if collected_df else None
        print(f"MAX value in {self.column_name}: ", max_value)
        self.metric_value = str(max_value) if max_value is not None else None


def run_profiling(metric_obj: ProfilingMetric):
    """Run counting metrics for target table"""
    print(f'Start count metric: {metric_obj.metric_name}, profiling_id: {metric_obj.profiling_id}\n')
    return getattr(metric_obj, metric_obj.metric_name)()

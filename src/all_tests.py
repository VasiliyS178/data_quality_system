"""
Data Quality Tests Repository
"""
import json
from utils import get_data_from_parquet


class DQTest:
    def __init__(self, **kwargs):
        self.assertion_id = None
        self.averaging_interval = None
        self.category = None
        self.check_all_rows = None
        self.check_date = None
        self.clear_query = None
        self.column_name = None
        self.count_rows = None
        self.count_rows_with_errors = None
        self.load_date = None
        self.date_formatter = None
        self.description = None
        self.error = None
        self.filter_date_load = None
        self.hdfs_location = None
        self.is_critical = None
        self.log_time = None
        self.percent_errors = None
        self.metrics_hdfs_path = None
        self.metrics_table = None
        self.max_value = None
        self.min_value = None
        self.regexp = None
        self.reference_column = None
        self.schema_json = None
        self.schema_json_expected = None
        self.schema_name = None
        self.scope = None
        self.sql_query = None
        self.spark_session = None
        self.source_table_full_name = None
        self.source_hdfs_location = None
        self.source_schema_name = None
        self.source_table_name = None
        self.source_error = None
        self.status = None
        self.std_aggregation = None
        self.std_operator = None
        self.table_name = None
        self.table_full_name = None
        self.test_columns = None
        self.test_name = None
        self.variation_percent = None
        self.where_condition = None
        for key, value in kwargs.items():
            setattr(self, key, value)

    def check_for_column_values_do_not_have_duplicates(self):
        """Check for duplicate entries"""
        sql_query = f"""
            SELECT COUNT(*)
            FROM {self.table_name} {'' if not self.where_condition else 'WHERE ' + self.where_condition} 
            GROUP BY {self.column_name} 
            HAVING COUNT({self.column_name}) > 1
        """
        print(sql_query)
        result_df, self.count_rows = get_data_from_parquet(spark_session=self.spark_session,
                                                           hdfs_location=self.hdfs_location,
                                                           sql_query=sql_query,
                                                           table_name=self.table_name,
                                                           load_date=self.filter_date_load)
        self.sql_query = sql_query
        try:
            self.count_rows_with_errors = result_df.collect()[0][0]
            self.status = False if self.count_rows_with_errors else True
        except AttributeError:
            self.count_rows_with_errors = None
        except IndexError:
            self.count_rows_with_errors = 0
            self.status = True

    def check_for_column_values_to_be_not_null(self):
        """Expect the values in the column do not contain null values"""
        sql_query = f"""
            SELECT COUNT(*)      
            FROM {self.table_name} 
            WHERE {'' if not self.where_condition else self.where_condition + ' AND'} 
            {self.column_name} IS null
        """
        print(sql_query)
        result_df, self.count_rows = get_data_from_parquet(spark_session=self.spark_session,
                                                           hdfs_location=self.hdfs_location,
                                                           sql_query=sql_query,
                                                           table_name=self.table_name,
                                                           load_date=self.filter_date_load)
        self.sql_query = sql_query
        self.count_rows_with_errors = result_df.collect()[0][0] if result_df else None
        self.status = not bool(result_df.collect()[0][0]) if result_df else None

    def check_for_relevance(self):
        """Check for relevance"""
        sql_query = f"""
            SELECT COUNT(*)        
            FROM {self.table_name} 
            WHERE {'' if not self.where_condition else self.where_condition}            
        """
        print(sql_query)
        result_df, self.count_rows = get_data_from_parquet(spark_session=self.spark_session,
                                                           hdfs_location=self.hdfs_location,
                                                           sql_query=sql_query,
                                                           table_name=self.table_name,
                                                           load_date=self.check_date.replace('-', ''))
        self.sql_query = sql_query
        self.count_rows_with_errors = None
        self.status = bool(result_df.collect()[0][0]) if result_df else False

    def check_for_column_values_to_be_gtoet(self):
        """Check for value in the column to be greater than or equal to specific value"""
        column_date_formatter = None
        if self.date_formatter and '{column_name}' in self.date_formatter:
            column_date_formatter = self.date_formatter.replace('{column_name}', self.column_name)

        if self.min_value and '-' in str(self.min_value) and not str(self.min_value).startswith('-'):
            sql_query = f"""
                SELECT COUNT(*) 
                FROM {self.table_name} 
                WHERE {column_date_formatter} < '{self.min_value}' 
                {'' if not self.where_condition else 'AND ' + self.where_condition}        
            """
        else:
            sql_query = f"""
                SELECT COUNT(*) 
                FROM {self.table_name} 
                WHERE DOUBLE({self.column_name}) < {self.min_value} 
                {'' if not self.where_condition else 'AND ' + self.where_condition}        
            """
        print(sql_query)

        result_df, self.count_rows = get_data_from_parquet(spark_session=self.spark_session,
                                                           hdfs_location=self.hdfs_location,
                                                           sql_query=sql_query,
                                                           table_name=self.table_name,
                                                           load_date=self.filter_date_load)
        self.sql_query = sql_query
        self.count_rows_with_errors = result_df.collect()[0][0] if result_df else None
        self.status = not bool(result_df.collect()[0][0]) if result_df else None

    def check_for_column_values_to_be_gt(self):
        """Check for value in the column to be strictly greater than to specific value"""
        column_date_formatter = None
        if self.date_formatter and '{column_name}' in self.date_formatter:
            column_date_formatter = self.date_formatter.replace('{column_name}', self.column_name)

        if self.min_value and '-' in str(self.min_value) and not str(self.min_value).startswith('-'):
            sql_query = f"""
                SELECT COUNT(*) 
                FROM {self.table_name} 
                WHERE {column_date_formatter} <= '{self.min_value}' 
                {'' if not self.where_condition else 'AND ' + self.where_condition}        
            """
        else:
            sql_query = f"""
                SELECT COUNT(*) 
                FROM {self.table_name}
                WHERE DOUBLE({self.column_name}) <= {self.min_value}
                {'' if not self.where_condition else 'AND ' + self.where_condition}
            """
        print(sql_query)

        result_df, self.count_rows = get_data_from_parquet(spark_session=self.spark_session,
                                                           hdfs_location=self.hdfs_location,
                                                           sql_query=sql_query,
                                                           table_name=self.table_name,
                                                           load_date=self.filter_date_load)
        self.sql_query = sql_query
        self.count_rows_with_errors = result_df.collect()[0][0] if result_df else None
        self.status = not bool(result_df.collect()[0][0]) if result_df else None

    def check_for_column_values_to_be_between(self):
        """Check for value in the column to be greater than min and less than max value"""
        column_date_formatter = None
        if self.date_formatter and '{column_name}' in self.date_formatter:
            column_date_formatter = self.date_formatter.replace('{column_name}', self.column_name)

        if self.min_value and '-' in str(self.min_value) and not str(self.min_value).startswith('-'):
            sql_query = f"""
                SELECT COUNT(*) 
                FROM {self.table_name} 
                WHERE ({column_date_formatter} < '{self.min_value}' OR {column_date_formatter} > '{self.max_value}')
                {'' if not self.where_condition else 'AND ' + self.where_condition}        
            """
        else:
            sql_query = f"""
                SELECT COUNT(*) 
                FROM {self.table_name} 
                WHERE (DOUBLE({self.column_name}) < {self.min_value} OR DOUBLE({self.column_name}) > {self.max_value})
                {'' if not self.where_condition else 'AND ' + self.where_condition}        
            """
        print(sql_query)
        result_df, self.count_rows = get_data_from_parquet(spark_session=self.spark_session,
                                                           hdfs_location=self.hdfs_location,
                                                           sql_query=sql_query,
                                                           table_name=self.table_name,
                                                           load_date=self.filter_date_load)
        self.sql_query = sql_query
        self.count_rows_with_errors = result_df.collect()[0][0] if result_df else None
        self.status = not bool(result_df.collect()[0][0]) if result_df else None

    def check_for_column_distinct_values_to_be_in_reference_table(self):
        """Expect the set of distinct column values to be contained by a given set"""
        reference_hdfs_location = self.source_hdfs_location
        reference_table_name = self.source_table_name
        # get data from dimension or dictionary table
        reference_sql_query = f"""
            SELECT DISTINCT {self.reference_column}
            FROM {reference_table_name}
            {'' if not self.where_condition else 'WHERE ' + self.where_condition} 
        """
        print(reference_sql_query)
        reference_df, _ = get_data_from_parquet(spark_session=self.spark_session,
                                                hdfs_location=reference_hdfs_location,
                                                sql_query=reference_sql_query,
                                                table_name=reference_table_name,
                                                load_date=self.filter_date_load)
        # get data from target table
        target_sql_query = f"""
            SELECT DISTINCT {self.column_name}
            FROM {self.table_name}
            {'' if not self.where_condition else 'WHERE ' + self.where_condition}  
        """
        print(target_sql_query)
        target_df, self.count_rows = get_data_from_parquet(spark_session=self.spark_session,
                                                           hdfs_location=self.hdfs_location,
                                                           sql_query=target_sql_query,
                                                           table_name=self.table_name,
                                                           load_date=self.filter_date_load)
        test_sql = f"""
            SELECT COUNT(rt.{self.reference_column})
            FROM {self.table_name} tt
            LEFT JOIN {reference_table_name} rt
            ON tt.{self.column_name} = rt.{self.reference_column}
            WHERE rt.{self.reference_column} IS NULL
        """
        result_df = self.spark_session.sql(test_sql)
        collected_df = result_df.collect() if result_df else []
        if collected_df:
            self.count_rows_with_errors = collected_df[0][0]
            self.status = False if self.count_rows_with_errors else True
        self.sql_query = test_sql

    def check_for_custom_query(self):
        """Check for table with custom SQL-query"""
        sql_query_prepared = self.sql_query\
            .replace('{check_date}', self.check_date)\
            .replace('{table_name}', self.table_name) \
            .replace('{column_name}', self.column_name)
        print('\n', sql_query_prepared, '\n')
        result_df, self.count_rows = get_data_from_parquet(spark_session=self.spark_session,
                                                           hdfs_location=self.hdfs_location,
                                                           sql_query=sql_query_prepared,
                                                           table_name=self.table_name,
                                                           load_date=self.filter_date_load)
        try:
            self.count_rows_with_errors = result_df.collect()[0][0]
            self.status = False if self.count_rows_with_errors else True
        except IndexError:
            self.count_rows_with_errors = 0
            self.status = True
        except AttributeError:
            self.count_rows_with_errors = None

    def check_for_schema_data_types(self):
        """Check for data types in the target table"""
        schema_dict = json.loads(self.schema_json)
        schema_dict_expected = json.loads(self.schema_json_expected)
        for expected_field in schema_dict_expected['fields']:
            is_found = False
            expected_field_name = expected_field.get('name')
            expected_field_type = expected_field.get('type')
            for field in schema_dict['fields']:
                if field.get('name') == expected_field_name:
                    is_found = True
                    self.status = field.get('type').lower() == expected_field_type.lower()
                    if not self.status:
                        self.error += f'Types of fields "{expected_field_name}" are not equal\n'
            if not is_found:
                self.error += f'Field "{expected_field_name}" is not found in the actual schema\n'
                self.status = False

    def check_for_column_values_to_match_regexp_pattern(self):
        """Check for column values to match regular expression pattern"""
        where_condition = '' if not self.where_condition else '\nAND ' + self.where_condition
        # WARNING: do not use f-string with regular expressions (self.regexp)
        sql_query = f"""
            SELECT COUNT(*)        
            FROM {self.table_name} 
            WHERE {self.column_name} NOT RLIKE""" + ' "' + rf"{self.regexp}" + '" ' + where_condition
        print(sql_query)
        result_df, self.count_rows = get_data_from_parquet(spark_session=self.spark_session,
                                                           hdfs_location=self.hdfs_location,
                                                           sql_query=sql_query,
                                                           table_name=self.table_name,
                                                           load_date=self.filter_date_load)
        self.sql_query = sql_query
        collected_df = result_df.collect() if result_df else []
        if collected_df:
            self.count_rows_with_errors = collected_df[0][0]
            self.status = False if self.count_rows_with_errors else True

    def check_for_count_rows_to_be_equal_to_source(self):
        """Expect the count rows in HDFS is equal to count rows in the source"""
        source_sql_query = f"""
            SELECT CAST(metric_value AS INTEGER) AS metric_value, check_date 
            FROM {self.metrics_table}     
            WHERE table_full_name = '{self.source_table_full_name}'
            AND metric_name LIKE 'count_rows_in_source_table%'
            ORDER BY check_date, log_time DESC
            LIMIT 1 
        """
        print('source_sql_query:\n', source_sql_query)
        source_df, _ = get_data_from_parquet(spark_session=self.spark_session,
                                             hdfs_location=self.metrics_hdfs_path,
                                             sql_query=source_sql_query,
                                             table_name=self.metrics_table)
        collected_source = source_df.collect() if source_df else []
        if collected_source:
            source_count_rows = collected_source[0]['metric_value']
            source_check_date = collected_source[0]['check_date']
            print("source_count_rows: ", source_count_rows)
            print("check_date: ", source_check_date)
        else:
            print("There are no metric values in the metrics table for this test. Test skipped\n")
            return
        target_sql_query = f"""
            SELECT CAST(metric_value AS INTEGER) AS metric_value, check_date
            FROM {self.metrics_table}     
            WHERE table_full_name = '{self.table_full_name}'
            AND metric_name = 'count_rows_in_target_table'
            ORDER BY check_date, log_time DESC
            LIMIT 1       
        """
        print('target_sql_query:\n', target_sql_query)
        target_df, _ = get_data_from_parquet(spark_session=self.spark_session,
                                             hdfs_location=self.metrics_hdfs_path,
                                             sql_query=target_sql_query,
                                             table_name=self.metrics_table)
        collected_target = target_df.collect() if target_df else []
        if collected_target:
            target_count_rows = collected_target[0]['metric_value']
            target_check_date = collected_target[0]['check_date']
            print("target_count_rows: ", target_count_rows)
            print("check_date: ", target_check_date)
        else:
            print("There are no metric values in the metrics table for this test. Test skipped\n")
            return
        self.count_rows = source_count_rows

        if source_check_date != target_check_date:
            print("check_date in the source is not the same as in the target table. Test skipped\n")
            return

        if source_count_rows == 0:
            if target_count_rows == 0:
                self.status = True
            else:
                self.status = False
                self.count_rows_with_errors = target_count_rows
        else:
            self.count_rows_with_errors = source_count_rows - target_count_rows
            self.status = \
                abs(round(self.count_rows_with_errors / source_count_rows * 100, 2)) <= self.variation_percent

    def check_for_sum_by_column_to_be_equal_to_source(self):
        """Expect the total sum by column in HDFS is equal to sum by the same column in the source"""
        source_sql_query = f"""
            SELECT CAST(metric_value AS DECIMAL(38, 2)) AS metric_value, check_date  
            FROM {self.metrics_table}     
            WHERE table_full_name = '{self.source_table_full_name}'
            AND metric_name == 'count_sum_by_column_in_source_table'
            ORDER BY check_date, log_time DESC
            LIMIT 1 
        """
        print('source_sql_query:\n', source_sql_query)
        source_df, _ = get_data_from_parquet(spark_session=self.spark_session,
                                             hdfs_location=self.metrics_hdfs_path,
                                             sql_query=source_sql_query,
                                             table_name=self.metrics_table)
        collected_source = source_df.collect() if source_df else []
        if collected_source:
            source_sum_value = collected_source[0]['metric_value']
            source_check_date = collected_source[0]['check_date']
            print("source_sum_value: ", source_sum_value)
            print("check_date: ", collected_source[0]['check_date'])
        else:
            print("There are no metric values in the metrics table for this test. Test skipped\n")
            return
        target_sql_query = f"""
            SELECT CAST(metric_value AS DECIMAL(38, 2)) AS metric_value, check_date
            FROM {self.metrics_table}     
            WHERE table_full_name = '{self.table_full_name}'
            AND metric_name = 'count_sum_by_column_in_target_table'
            ORDER BY check_date, log_time DESC
            LIMIT 1       
        """
        print('target_sql_query:\n', target_sql_query)
        target_df, _ = get_data_from_parquet(spark_session=self.spark_session,
                                             hdfs_location=self.metrics_hdfs_path,
                                             sql_query=target_sql_query,
                                             table_name=self.metrics_table)
        collected_target = target_df.collect() if target_df else []
        if collected_target:
            target_sum_value = collected_target[0]['metric_value']
            target_check_date = collected_target[0]['check_date']
            print("target_sum_value: ", target_sum_value)
            print("check_date: ", collected_source[0]['check_date'])
        else:
            print("There are no metric values in the metrics table for this test. Test skipped\n")
            return

        if source_check_date != target_check_date:
            print("check_date in the source is not the same as in the target table. Test skipped\n")
            return

        if source_sum_value == 0:
            if target_sum_value == 0:
                self.status = True
            else:
                self.status = False
        else:
            difference = abs(source_sum_value) - abs(target_sum_value)
            self.status = \
                abs(round(difference / source_sum_value * 100, 2)) <= self.variation_percent

    def check_for_min_by_column_to_be_equal_to_source(self):
        """Expect the min value by column in HDFS is equal to min value by the same column in the source"""
        source_sql_query = f"""
            SELECT CAST(metric_value AS DECIMAL(38, 2)) AS metric_value, check_date  
            FROM {self.metrics_table}     
            WHERE table_full_name = '{self.source_table_full_name}'
            AND metric_name == 'count_min_by_column_in_source_table'
            ORDER BY check_date, log_time DESC
            LIMIT 1 
        """
        print('source_sql_query:\n', source_sql_query)
        source_df, _ = get_data_from_parquet(spark_session=self.spark_session,
                                             hdfs_location=self.metrics_hdfs_path,
                                             sql_query=source_sql_query,
                                             table_name=self.metrics_table)
        collected_source = source_df.collect() if source_df else []
        if collected_source:
            source_min_value = collected_source[0]['metric_value']
            source_check_date = collected_source[0]['check_date']
            print("source_min_value: ", source_min_value)
            print("check_date: ", collected_source[0]['check_date'])
        else:
            print("There are no metric values in the metrics table for this test. Test skipped\n")
            return
        target_sql_query = f"""
            SELECT CAST(metric_value AS AS DECIMAL(38, 2)) AS metric_value, check_date
            FROM {self.metrics_table}     
            WHERE table_full_name = '{self.table_full_name}'
            AND metric_name = 'count_min_by_column_in_target_table'
            ORDER BY check_date, log_time DESC
            LIMIT 1       
        """
        print('target_sql_query:\n', target_sql_query)
        target_df, _ = get_data_from_parquet(spark_session=self.spark_session,
                                             hdfs_location=self.metrics_hdfs_path,
                                             sql_query=target_sql_query,
                                             table_name=self.metrics_table)
        collected_target = target_df.collect() if target_df else []
        if collected_target:
            target_min_value = collected_target[0]['metric_value']
            target_check_date = collected_target[0]['check_date']
            print("target_min_value: ", target_min_value)
            print("check_date: ", collected_source[0]['check_date'])
        else:
            print("There are no metric values in the metrics table for this test. Test skipped\n")
            return

        if source_check_date != target_check_date:
            print("check_date in the source is not the same as in the target table. Test skipped\n")
            return

        if source_min_value == 0:
            if target_min_value == 0:
                self.status = True
            else:
                self.status = False
        else:
            difference = abs(source_min_value) - abs(target_min_value)
            self.status = \
                abs(round(difference / source_min_value * 100, 2)) <= self.variation_percent

    def check_for_max_by_column_to_be_equal_to_source(self):
        """Expect the min value by column in HDFS is equal to min value by the same column in the source"""
        source_sql_query = f"""
            SELECT CAST(metric_value AS DECIMAL(38, 2)) AS metric_value, check_date  
            FROM {self.metrics_table}
            WHERE table_full_name = '{self.source_table_full_name}'
            AND metric_name == 'count_min_by_column_in_source_table'
            ORDER BY check_date, log_time DESC
            LIMIT 1 
        """
        print('source_sql_query:\n', source_sql_query)
        source_df, _ = get_data_from_parquet(spark_session=self.spark_session,
                                             hdfs_location=self.metrics_hdfs_path,
                                             sql_query=source_sql_query,
                                             table_name=self.metrics_table)
        collected_source = source_df.collect() if source_df else []
        if collected_source:
            source_max_value = collected_source[0]['metric_value']
            source_check_date = collected_source[0]['check_date']
            print("source_max_value: ", source_max_value)
            print("check_date: ", collected_source[0]['check_date'])
        else:
            print("There are no metric values in the metrics table for this test. Test skipped\n")
            return
        target_sql_query = f"""
            SELECT CAST(metric_value AS AS DECIMAL(38, 2)) AS metric_value, check_date
            FROM {self.metrics_table}     
            WHERE table_full_name = '{self.table_full_name}'
            AND metric_name = 'count_min_by_column_in_target_table'
            ORDER BY check_date, log_time DESC
            LIMIT 1       
        """
        print('target_sql_query:\n', target_sql_query)
        target_df, _ = get_data_from_parquet(spark_session=self.spark_session,
                                             hdfs_location=self.metrics_hdfs_path,
                                             sql_query=target_sql_query,
                                             table_name=self.metrics_table)
        collected_target = target_df.collect() if target_df else []
        if collected_target:
            target_max_value = collected_target[0]['metric_value']
            target_check_date = collected_target[0]['check_date']
            print("target_max_value: ", target_max_value)
            print("check_date: ", collected_source[0]['check_date'])
        else:
            print("There are no metric values in the metrics table for this test. Test skipped\n")
            return

        if source_check_date != target_check_date:
            print("check_date in the source is not the same as in the target table. Test skipped\n")
            return

        if source_max_value == 0:
            if target_max_value == 0:
                self.status = True
            else:
                self.status = False
        else:
            difference = abs(source_max_value) - abs(target_max_value)
            self.status = \
                abs(round(difference / source_max_value * 100, 2)) <= self.variation_percent

    def check_for_anomalies_in_count_rows(self):
        """Expect the number of rows is roughly similar every time"""
        count_sql_query = f"""
            SELECT CAST(metric_value AS INTEGER) AS metric_value
            FROM {self.metrics_table}     
            WHERE table_full_name = '{self.table_full_name}'
            AND metric_name = 'count_rows_in_target_table'
            ORDER BY check_date, log_time DESC
            LIMIT 1       
        """
        print('count_sql_query:\n', count_sql_query)
        target_df, _ = get_data_from_parquet(spark_session=self.spark_session,
                                             hdfs_location=self.metrics_hdfs_path,
                                             sql_query=count_sql_query,
                                             table_name=self.metrics_table)
        collected_target = target_df.collect() if target_df else []
        if collected_target:
            last_count_rows = collected_target[0]['metric_value']
            self.count_rows = last_count_rows
        else:
            print("There are no metric values in the metrics table for this test. Test skipped\n")
            return
        avg_count_sql_query = f"""
            SELECT AVG(CAST(metric_value AS INTEGER)) AS avg_metric_value 
            FROM (SELECT metric_value
                  FROM {self.metrics_table}     
                  WHERE table_full_name = '{self.table_full_name}'
                  AND metric_name = 'count_rows_in_target_table'
                  ORDER BY check_date DESC
                  LIMIT {self.averaging_interval}) AS t       
        """
        print('avg_count_sql_query:\n', avg_count_sql_query)
        avg_target_df, _ = get_data_from_parquet(spark_session=self.spark_session,
                                                 hdfs_location=self.metrics_hdfs_path,
                                                 sql_query=avg_count_sql_query,
                                                 table_name=self.metrics_table)
        collected_avg_target = avg_target_df.collect() if avg_target_df else []
        if collected_avg_target:
            avg_count_rows = collected_avg_target[0]['avg_metric_value']
        else:
            print("There are no metric values in the metrics table for this test. Test skipped\n")
            return
        print("avg_count_rows: ", avg_count_rows)
        print("last_count_rows: ", last_count_rows)
        print("variation_percent: ", self.variation_percent)
        if avg_count_rows == 0:
            self.status = True
            self.count_rows_with_errors = 0
        else:
            difference = avg_count_rows - last_count_rows
            self.status = \
                abs(round(difference / float(avg_count_rows) * 100, 2)) <= self.variation_percent
            self.count_rows_with_errors = 0 if self.status else difference

    def check_for_anomalies_in_null_values(self):
        """Expect the number of null values is roughly similar every time"""
        count_sql_query = f"""
            SELECT CAST(metric_value AS INTEGER) AS metric_value
            FROM {self.metrics_table}     
            WHERE table_full_name = '{self.table_full_name}'
            AND metric_name = 'count_null_values_by_column_in_target_table'
            ORDER BY check_date, log_time DESC
            LIMIT 1       
        """
        print('count_sql_query:\n', count_sql_query)
        target_df, _ = get_data_from_parquet(spark_session=self.spark_session,
                                             hdfs_location=self.metrics_hdfs_path,
                                             sql_query=count_sql_query,
                                             table_name=self.metrics_table)
        collected_target = target_df.collect() if target_df else []
        if collected_target:
            last_count_nulls = collected_target[0]['metric_value']
        else:
            print("There are no metric values in the metrics table for this test. Test skipped\n")
            return
        avg_count_sql_query = f"""
            SELECT AVG(CAST(metric_value AS INTEGER)) AS avg_metric_value 
            FROM (SELECT metric_value
                  FROM {self.metrics_table}     
                  WHERE table_full_name = '{self.table_full_name}'
                  AND metric_name = 'count_null_values_by_column_in_target_table'
                  ORDER BY check_date DESC
                  LIMIT {self.averaging_interval}) AS t       
        """
        print('avg_count_sql_query:\n', avg_count_sql_query)
        avg_target_df, _ = get_data_from_parquet(spark_session=self.spark_session,
                                                 hdfs_location=self.metrics_hdfs_path,
                                                 sql_query=avg_count_sql_query,
                                                 table_name=self.metrics_table)
        collected_avg_target = avg_target_df.collect() if avg_target_df else []
        if collected_avg_target:
            avg_count_nulls = collected_avg_target[0]['avg_metric_value']
        else:
            print("There are no metric values in the metrics table for this test. Test skipped\n")
            return
        print("avg_count_nulls: ", avg_count_nulls)
        print("last_count_nulls: ", last_count_nulls)
        print("variation_percent: ", self.variation_percent)
        if avg_count_nulls == 0:
            self.status = True
            self.count_rows_with_errors = 0
        else:
            difference = avg_count_nulls - last_count_nulls
            self.status = \
                abs(round(difference / float(avg_count_nulls) * 100, 2)) <= self.variation_percent
            self.count_rows_with_errors = 0 if self.status else difference

    def check_for_anomalies_in_sum_values(self):
        """Expect the sum values is roughly similar every time"""
        sum_sql_query = f"""
            SELECT CAST(metric_value AS DECIMAL(38, 2)) AS metric_value 
            FROM {self.metrics_table}     
            WHERE table_full_name = '{self.table_full_name}'
            AND metric_name = 'count_sum_by_column_in_target_table'
            ORDER BY check_date, log_time DESC
            LIMIT 1       
        """
        print('sum_sql_query:\n', sum_sql_query)
        target_df, _ = get_data_from_parquet(spark_session=self.spark_session,
                                             hdfs_location=self.metrics_hdfs_path,
                                             sql_query=sum_sql_query,
                                             table_name=self.metrics_table)
        collected_target = target_df.collect() if target_df else []
        if collected_target:
            last_sum = collected_target[0]['metric_value']
        else:
            print("There are no metric values in the metrics table for this test. Test skipped\n")
            return
        avg_sum_sql_query = f"""
            SELECT AVG(CAST(metric_value AS DECIMAL(38, 2))) AS avg_metric_value 
            FROM (SELECT metric_value
                  FROM {self.metrics_table}     
                  WHERE table_full_name = '{self.table_full_name}'
                  AND metric_name = 'count_sum_by_column_in_target_table'
                  ORDER BY check_date DESC
                  LIMIT {self.averaging_interval}) AS t       
        """
        print('avg_sum_sql_query:\n', avg_sum_sql_query)
        avg_target_df, _ = get_data_from_parquet(spark_session=self.spark_session,
                                                 hdfs_location=self.metrics_hdfs_path,
                                                 sql_query=avg_sum_sql_query,
                                                 table_name=self.metrics_table)
        collected_avg_target = avg_target_df.collect() if avg_target_df else []
        if collected_avg_target:
            avg_sum = collected_avg_target[0]['avg_metric_value']
        else:
            print("There are no metric values in the metrics table for this test. Test skipped\n")
            return
        print("AVG SUM: ", avg_sum)
        print("LAST SUM: ", last_sum)
        print("variation_percent: ", self.variation_percent)
        if avg_sum == 0:
            self.status = True
        else:
            difference = avg_sum - last_sum
            self.status = \
                abs(round(difference / avg_sum * 100, 2)) <= self.variation_percent


def run_data_quality_test(test_obj: DQTest):
    """Run test for target table"""
    print(f'Starting test: {test_obj.test_name}, assertion_id: {test_obj.assertion_id}')
    print(f'table: {test_obj.table_name}, column: {test_obj.column_name}\n')
    # run test function by its string name
    return getattr(test_obj, test_obj.test_name)()

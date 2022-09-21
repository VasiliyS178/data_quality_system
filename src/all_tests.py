"""
Data Quality Tests Repository
"""
from datetime import datetime as dt
import json
from utils import get_data_via_jdbc, get_data_from_parquet


class DQTest:
    def __init__(self, **kwargs):
        self.assertion_id = None
        self.category = None
        self.check_all_rows = None
        self.check_date = None
        self.column_name = None
        self.count_rows_with_errors = None
        self.date_formatter = None
        self.error = None
        self.hdfs_location = None
        self.is_critical = None
        self.min_value = None
        self.max_value = None
        self.regexp = None
        self.schema_json = None
        self.schema_json_expected = None
        self.scope = None
        self.sql_query = None
        self.spark_session = None
        self.source_connection = None
        self.source_hdfs_location = None
        self.source_schema_name = None
        self.source_table_name = None
        self.source_where_condition = None
        self.status = None
        self.std_aggregation = None
        self.std_operator = None
        self.table_name = None
        self.test_columns = None
        self.test_name = None
        self.where_condition = None
        for key, value in kwargs.items():
            setattr(self, key, value)
        self.datekey = dt.now().strftime('%Y%m%d')
        self.filter_datekey = None if self.check_all_rows else self.check_date.replace('-', '')

    def check_for_column_values_do_not_have_duplicates(self):
        """Check for duplicate entries"""
        self.status = None
        sql_query = f"""
            SELECT COUNT(*)
            FROM {self.table_name} {'' if not self.where_condition else 'WHERE ' + self.where_condition} 
            GROUP BY {self.column_name} 
            HAVING COUNT({self.column_name}) > 1
        """
        print(sql_query)
        result_df = get_data_from_parquet(spark_session=self.spark_session,
                                          hdfs_location=self.hdfs_location,
                                          sql_query=sql_query,
                                          table_name=self.table_name,
                                          datekey=self.filter_datekey)
        self.sql_query = sql_query
        try:
            self.count_rows_with_errors = result_df.collect()[0][0]
            self.status = False if self.count_rows_with_errors else True
        except AttributeError:
            self.count_rows_with_errors = None
            self.status = None
        except IndexError:
            self.count_rows_with_errors = 0
            self.status = True

    def check_for_column_values_to_be_not_null(self):
        """Expect the values in the column do not contain null values"""
        self.status = None

        sql_query = f"""
            SELECT COUNT(*)      
            FROM {self.table_name} 
            WHERE {'' if not self.where_condition else self.where_condition + ' AND'} 
            {self.column_name} IS null
        """
        print(sql_query)
        result_df = get_data_from_parquet(spark_session=self.spark_session,
                                          hdfs_location=self.hdfs_location,
                                          sql_query=sql_query,
                                          table_name=self.table_name,
                                          datekey=self.filter_datekey)
        self.sql_query = sql_query
        self.count_rows_with_errors = result_df.collect()[0][0] if result_df else None
        self.status = not bool(result_df.collect()[0][0]) if result_df else None

    def check_for_count_rows_to_be_equal_to_source(self):
        """Expect the count rows in HDFS is equal to count rows in the source"""
        self.status = None
        # get data from source table
        source_sql_query = f"""
            SELECT COUNT(*) 
            FROM {self.source_schema_name}.{self.source_table_name}
            WHERE {self.where_condition} = '{self.check_date}'
        """
        print('source_sql_query:\n', source_sql_query)
        source_df = get_data_via_jdbc(spark_session=self.spark_session,
                                      connection=self.source_connection,
                                      sql_query=source_sql_query)
        if source_df:
            source_df.show()
            source_count_rows = source_df.collect()
        else:
            source_count_rows = []
        # get data from target table
        target_sql_query = f"""
            SELECT COUNT(*) 
            FROM {self.table_name}            
        """
        print('target_sql_query:\n', target_sql_query)
        target_df = get_data_from_parquet(spark_session=self.spark_session,
                                          hdfs_location=self.hdfs_location,
                                          sql_query=target_sql_query,
                                          table_name=self.table_name,
                                          datekey=self.filter_datekey)
        target_count_rows = self.spark_session.sql(target_sql_query).collect() if target_df else []
        self.sql_query = f'source_sql_query: {source_sql_query}\ntarget_sql_query: {target_sql_query}'
        if source_count_rows and target_count_rows:
            self.count_rows_with_errors = int(source_count_rows) - int(target_count_rows)
            self.status = source_count_rows[0][0] == target_count_rows[0][0]

    def check_for_relevance(self):
        """Check for relevance"""
        self.status = None
        sql_query = f"""
            SELECT COUNT(*)        
            FROM {self.table_name} 
            WHERE {'' if not self.where_condition else self.where_condition}            
        """
        print(sql_query)
        result_df = get_data_from_parquet(spark_session=self.spark_session,
                                          hdfs_location=self.hdfs_location,
                                          sql_query=sql_query,
                                          table_name=self.table_name,
                                          datekey=self.check_date.replace('-', ''))
        self.sql_query = sql_query
        self.count_rows_with_errors = None
        self.status = bool(result_df.collect()[0][0]) if result_df else False

    def check_for_column_values_to_be_gtoet(self):
        """Check for value in the column to be greater than or equal to specific value"""
        self.status = None
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

        result_df = get_data_from_parquet(spark_session=self.spark_session,
                                          hdfs_location=self.hdfs_location,
                                          sql_query=sql_query,
                                          table_name=self.table_name,
                                          datekey=self.filter_datekey)
        self.sql_query = sql_query
        self.count_rows_with_errors = result_df.collect()[0][0] if result_df else None
        self.status = not bool(result_df.collect()[0][0]) if result_df else None

    def check_for_column_values_to_be_gt(self):
        """Check for value in the column to be strictly greater than to specific value"""
        self.status = None
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

        result_df = get_data_from_parquet(spark_session=self.spark_session,
                                          hdfs_location=self.hdfs_location,
                                          sql_query=sql_query,
                                          table_name=self.table_name,
                                          datekey=self.filter_datekey)
        self.sql_query = sql_query
        self.count_rows_with_errors = result_df.collect()[0][0] if result_df else None
        self.status = not bool(result_df.collect()[0][0]) if result_df else None

    def check_for_column_values_to_be_between(self):
        """Check for value in the column to be greater than min and less than max value"""
        self.status = None
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
        result_df = get_data_from_parquet(spark_session=self.spark_session,
                                          hdfs_location=self.hdfs_location,
                                          sql_query=sql_query,
                                          table_name=self.table_name,
                                          datekey=self.filter_datekey)
        self.sql_query = sql_query
        self.count_rows_with_errors = result_df.collect()[0][0] if result_df else None
        self.status = not bool(result_df.collect()[0][0]) if result_df else None

    def check_for_column_distinct_values_to_be_in_reference_table(self):
        """Expect the set of distinct column values to be contained by a given set"""
        self.status = None
        reference_hdfs_location = self.source_hdfs_location
        reference_table_name = self.source_table_name
        # get data from dimension or dictionary table
        reference_sql_query = f"""
            SELECT DISTINCT {self.column_name}
            FROM {reference_table_name}
            {'' if not self.where_condition else 'WHERE ' + self.where_condition} 
        """
        print(reference_sql_query)
        reference_df = get_data_from_parquet(spark_session=self.spark_session,
                                             hdfs_location=reference_hdfs_location,
                                             sql_query=reference_sql_query,
                                             table_name=reference_table_name,
                                             datekey=self.filter_datekey)
        valid_row_values = reference_df.collect() if reference_df else []
        valid_values = [row[self.column_name] for row in valid_row_values]
        # get data from target table
        target_sql_query = f"""
            SELECT DISTINCT {self.column_name}
            FROM {self.table_name}
            {'' if not self.where_condition else 'WHERE ' + self.where_condition}  
        """
        print(target_sql_query)
        target_df = get_data_from_parquet(spark_session=self.spark_session,
                                          hdfs_location=self.hdfs_location,
                                          sql_query=target_sql_query,
                                          table_name=self.table_name,
                                          datekey=self.filter_datekey)
        actual_row_values = target_df.collect() if target_df else []
        actual_values = [row[self.column_name] for row in actual_row_values]
        check_results = [value in valid_values for value in actual_values]
        self.sql_query = f'reference_sql_query: {reference_sql_query}\ntarget_sql_query: {target_sql_query}'
        self.count_rows_with_errors = 0
        self.status = False if False in check_results else True

    def check_for_custom_query(self):
        """Check for table with custom SQL-query"""
        self.status = None
        self.sql_query = self.sql_query.replace('{check_date}', self.check_date)
        self.sql_query = self.sql_query.replace('{table_name}', self.table_name)
        self.sql_query = self.sql_query.replace('{column_name}', self.column_name)
        print('\n', self.sql_query, '\n')
        result_df = get_data_from_parquet(spark_session=self.spark_session,
                                          hdfs_location=self.hdfs_location,
                                          sql_query=self.sql_query,
                                          table_name=self.table_name,
                                          datekey=self.filter_datekey)
        try:
            self.count_rows_with_errors = result_df.collect()[0][0]
            self.status = False if self.count_rows_with_errors else True
        except IndexError:
            self.count_rows_with_errors = 0
            self.status = True
        except AttributeError:
            self.count_rows_with_errors = None
            self.status = None

    def check_for_schema_data_types(self):
        """Check for data types in the target table"""
        self.status = None
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
        self.status = None
        sql_query = f"""
            SELECT COUNT(*)        
            FROM {self.table_name} 
            WHERE {self.column_name} NOT RLIKE '{self.regexp}'
            {'' if not self.where_condition else 'AND ' + self.where_condition}       
        """
        print(sql_query)
        result_df = get_data_from_parquet(spark_session=self.spark_session,
                                          hdfs_location=self.hdfs_location,
                                          sql_query=sql_query,
                                          table_name=self.table_name,
                                          datekey=self.filter_datekey)
        self.sql_query = sql_query
        self.count_rows_with_errors = None
        self.status = bool(result_df.collect()[0][0]) if result_df else None


def run_data_quality_test(test_obj: DQTest):
    """Run test for target table"""
    print(f'Starting test: {test_obj.test_name}, assertion_id: {test_obj.assertion_id}\n')
    # run test function by its string name
    return getattr(test_obj, test_obj.test_name)()


def get_actual_tests_info():
    """
    Get DQ tests info to write in DQ metastore
    :return dict: key = test_name, value - description
    """
    tests_info = {}
    for obj_name in globals().keys():
        if obj_name.startswith('check_for_'):
            tests_info[globals()[obj_name].__name__] = globals()[obj_name].__doc__.replace('\n', '').strip()
    return tests_info

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.empty import EmptyOperator


RUN_PROFILING_SCRIPT = 'run_profiling.py'
RUN_TESTS_SCRIPT = 'run_tests.py'
VENV_PATH = Variable.get('venvs_path')
LIBS_PATH = Variable.get('libs_path')
SCRIPTS_PATH = Variable.get('scripts_path')
all_table_full_names = ["environment.aria.system_name.table_name_1", "environment.aria.system_name.table_name_2"]

default_args = {
    'owner': 'dag_owner_name',
    'start_date': datetime(2022, 9, 21, 0, 0),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ["my_email@domen"]
}

dag = DAG(
    dag_id="dq_reference_dag",
    description="Dag to run DQ tests",
    catchup=False,
    default_args=default_args,
    schedule_interval="@daily",
    max_active_tasks=3,
    tags=['DQ']
)

profiling_tasks = []
for table in all_table_full_names:
    profiling_task = BashOperator(
        task_id=f"run_dq_profiling_for_{table.replace('.', '_')}",
        bash_command=f"/opt/dev/spark/spark-3.2.1/bin/spark-submit \
        --driver-memory 2g \
        --executor-memory 2g \
        --executor-cores 2 \
        --conf spark.dynamicAllocation.maxExecutors=4 \
        --conf spark.dynamicAllocation.initialExecutors=2 \
        --conf spark.sql.shuffle.partitions=8 \
        --conf spark.sql.sources.partitionOverwriteMode=dynamic \
        --conf spark.pyspark.virtualenv.enabled=true \
        --conf spark.pyspark.virtualenv.type=native \
        --conf spark.pyspark.python={VENV_PATH}venv_name/bin/python3.9 \
        --conf spark.pyspark.virtualenv.bin.path={VENV_PATH}venv_name/bin/ \
        {SCRIPTS_PATH}/{RUN_PROFILING_SCRIPT} {table}" + " {{ logical_date }}",
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE
    )
    profiling_tasks.append(profiling_task)

test_tasks = []
for table in all_table_full_names:
    test_task = BashOperator(
        task_id=f"run_dq_tests_for_{table.replace('.', '_')}",
        bash_command=f"/opt/dev/spark/spark-3.2.1/bin/spark-submit \
        --driver-memory 2g \
        --executor-memory 2g \
        --executor-cores 2 \
        --conf spark.dynamicAllocation.maxExecutors=4 \
        --conf spark.dynamicAllocation.initialExecutors=2 \
        --conf spark.sql.shuffle.partitions=8 \
        --conf spark.sql.sources.partitionOverwriteMode=dynamic \
        --conf spark.pyspark.virtualenv.enabled=true \
        --conf spark.pyspark.virtualenv.type=native \
        --jars {LIBS_PATH}/postgresql-42.3.6.jar \
        --conf spark.pyspark.python={VENV_PATH}datahub_venv/bin/python3.9 \
        --conf spark.pyspark.virtualenv.bin.path={VENV_PATH}datahub_venv/bin/ \
        {SCRIPTS_PATH}/{RUN_TESTS_SCRIPT} {table}" + " {{ logical_date }}",
        dag=dag,
        trigger_rule=TriggerRule.ALL_DONE
    )
    test_tasks.append(test_task)

between_task = EmptyOperator(task_id="between_task")

profiling_tasks >> between_task >> test_tasks

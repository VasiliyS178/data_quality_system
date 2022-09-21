from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime
from airflow.models import Variable


DQ_RUN_TESTS_SCRIPT = Variable.get('run_tests.py')
VENV_PATH = Variable.get('venvs_path')
LIBS_PATH = Variable.get('libs_path')
TABLE_FULL_NAME = "environment.aria.system_name.table_name"

default_args = {
    'owner': 'owner_name',
    'start_date': datetime(2022, 9, 21, 0, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
    'catchup': False,
    'email': ["my_email@domen.ru"]
}

dag = DAG(
    dag_id="dq_reference_dag",
    description="Dag to run DQ tests",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    tags=['DQ']
)

run_dq_tests = BashOperator(
    task_id="run_dq_tests",
    bash_command=f"/opt/dev/spark/spark-3.2.1/bin/spark-submit \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 2 \
    --conf spark.dynamicAllocation.maxExecutors=4 \
    --conf spark.dynamicAllocation.initialExecutors=2 \
    --conf spark.sql.shuffle.partitions=8 \
    --conf spark.sql.sources.partitionOverwriteMode=dynamic \
    --conf spark.pyspark.virtualenv.enabled=true \
    --conf spark.pyspark.virtualenv.type=native \
    --conf spark.pyspark.python={VENV_PATH}/venv/bin/python3.9 \
    --conf spark.pyspark.virtualenv.bin.path={VENV_PATH}/venv/bin/ \
    --jars {LIBS_PATH}/postgresql-42.3.6.jar \
    {DQ_RUN_TESTS_SCRIPT} {TABLE_FULL_NAME}" + " {{ ds }}",
    dag=dag)

run_dq_tests

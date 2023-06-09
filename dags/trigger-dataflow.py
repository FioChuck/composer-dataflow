from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.operators.python import PythonOperator


args = {
    'owner': 'packt-developer'
}

with DAG(
    dag_id='trigger-dataflow',
    default_args=args,
    schedule_interval='@daily',
    start_date=days_ago(1),
    max_active_runs=1,
    is_paused_upon_creation=False
) as dag:

    dataflow_launch = BeamRunPythonPipelineOperator(
        task_id="beam-bq-aggregation",
        runner=BeamRunnerType.DataflowRunner,
        py_file="gs://cf-cloud-composer-dags/dags/dataflow2.py",
        py_options=[],
        pipeline_options={},
        py_requirements=['apache-beam[gcp]==2.46.0'],
        py_interpreter='python3',
        py_system_site_packages=False,
        dataflow_config={'location': 'us-central1',
                         'wait_until_finished': False, 'job_name': "job-ac"}
    )

    # https://airflow.apache.org/docs/apache-airflow-providers-google/5.0.0/operators/cloud/dataflow.html#howto-operator-dataflowjobstatussensor
    # https://github.com/apache/airflow/blob/providers-apache-beam/4.3.0/tests/system/providers/apache/beam/example_python_dataflow.py

    # https://airflow.apache.org/docs/apache-airflow/1.10.1/concepts.html?highlight=xcom

    def pull_func(**context):
        out = context['ti'].xcom_pull(
            task_ids='beam-bq-aggregation')['dataflow_job_id']
        print('here')
        print(type(out))
        print(out)

    pull_task = PythonOperator(
        task_id='pull_task',
        python_callable=pull_func,
        provide_context=True,
        dag=dag)

    wait_for_python_job_async_done = DataflowJobStatusSensor(
        task_id="wait-for-python-job-async-done",
        job_id="{{task_instance.xcom_pull(task_ids='beam-bq-aggregation')['dataflow_job_id']}}",
        expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
        project_id='cf-data-analytics',
        location='us-central1',
    )

    dataflow_launch >> pull_task >> wait_for_python_job_async_done

if __name__ == "__main__":
    dag.cli()

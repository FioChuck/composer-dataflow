from airflow.providers.google.cloud.hooks.dataflow import DataflowJobStatus
from airflow.providers.google.cloud.sensors.dataflow import DataflowJobStatusSensor
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType
from airflow import DAG, XComarg
from airflow.utils.dates import days_ago
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.operators.python import PythonOperator


args = {
    'owner': 'packt-developer'
}

# def test():


with DAG(
    dag_id='trigger-dataflow',
    default_args=args,
    schedule_interval='@once',  # set schedule - at every tenth minute
    start_date=days_ago(0),
    is_paused_upon_creation=True

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

    def pull_function(ti, **kwargs) -> None:

        ls = ti.xcom_pull(task_ids='beam-bq-aggregation')
        print(ls)

    pull_task = PythonOperator(
        task_id='pull_task',
        python_callable=pull_function,
        provide_context=True,
        dag=DAG)

    # wait_for_python_job_async_done = DataflowJobStatusSensor(
    #     task_id="wait-for-python-job-async-done",
    #     job_id="{{task_instance.xcom_pull('beam-bq-aggregation')['dataflow_job_config']['job_id']}}",
    #     expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
    #     project_id='cf-data-analytics',
    #     location='us-central1',
    # )


######################################
    # start_python_job_dataflow_runner_async = BeamRunPythonPipelineOperator(
    #     task_id="start_python_job_dataflow_runner_async",
    #     runner="DataflowRunner",
    #     py_file=GCS_PYTHON_DATAFLOW_ASYNC,
    #     pipeline_options={
    #         "tempLocation": GCS_TMP,
    #         "stagingLocation": GCS_STAGING,
    #         "output": GCS_OUTPUT,
    #     },
    #     py_options=[],
    #     py_requirements=["apache-beam[gcp]==2.26.0"],
    #     py_interpreter="python3",
    #     py_system_site_packages=False,
    #     dataflow_config=DataflowConfiguration(
    #         job_name="{{task.task_id}}",
    #         project_id=GCP_PROJECT_ID,
    #         location="us-central1",
    #         wait_until_finished=False,
    #     ),
    # )

    # wait_for_python_job_dataflow_runner_async_done = DataflowJobStatusSensor(
    #     task_id="wait-for-python-job-async-done",
    #     job_id="{{task_instance.xcom_pull('start_python_job_dataflow_runner_async')['dataflow_job_id']}}",
    #     expected_statuses={DataflowJobStatus.JOB_STATE_DONE},
    #     project_id=GCP_PROJECT_ID,
    #     location="us-central1",
    # )
######################################

    dataflow_launch >> pull_task

if __name__ == "__main__":
    dag.cli()

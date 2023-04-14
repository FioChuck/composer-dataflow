from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.apache.beam.operators.beam import (
    BeamRunPythonPipelineOperator, BeamRunnerType)


args = {
    'owner': 'packt-developer',
}

with DAG(
    dag_id='trigger-dataflow',
    default_args=args,
    schedule_interval='@once',  # set schedule - at every tenth minute
    start_date=days_ago(1),
    is_paused_upon_creation=True

) as dag:

    dataflow_launch = BeamRunPythonPipelineOperator(
        task_id="start-python-job",
        runner=BeamRunnerType.DataflowRunner,
        py_file="gs://cf-cloud-composer-dags/dags/dataflow2.py",
        py_options=[],
        pipeline_options={},
        py_requirements=['apache-beam[gcp]==2.46.0'],
        py_interpreter='python3',
        py_system_site_packages=False,
        dataflow_config={'location': 'us-central1'},
    )

#     (
#     task_id="start_python_job_async",
#     runner=BeamRunnerType.DataflowRunner,
#     py_file=GCS_PYTHON_SCRIPT,
#     py_options=[],
#     pipeline_options={
#         "output": GCS_OUTPUT,
#     },
#     py_requirements=["apache-beam[gcp]==2.36.0"],
#     py_interpreter="python3",
#     py_system_site_packages=False,
#     dataflow_config={
#         "job_name": "start_python_job_async",
#         "location": LOCATION,
#         "wait_until_finished": False,
#     },
# )

    dataflow_launch

if __name__ == "__main__":
    dag.cli()

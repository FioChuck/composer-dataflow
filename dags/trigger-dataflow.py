from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.apache.beam.operators.beam import (
    BeamRunPythonPipelineOperator)

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
        py_file="gs://cf-dataflow-jobfiles/dataflow.py",
        py_options=[],
        pipeline_options={},
        py_requirements=['apache-beam[gcp]==2.46.0'],
        py_interpreter='python3',
        py_system_site_packages=False,
        dataflow_config={'location': 'us-central1'},
    )

    dataflow_launch

if __name__ == "__main__":
    dag.cli()

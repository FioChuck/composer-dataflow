import re
import apache_beam as beam
from apache_beam.options import pipeline_options
from apache_beam.options.pipeline_options import GoogleCloudOptions
# from apache_beam.runners import DataflowRunner
from apache_beam.runners.interactive.interactive_runner import InteractiveRunner
import apache_beam.runners.interactive.interactive_beam as ib
import google.auth
import pandas as pd


class ReadWordsFromText(beam.PTransform):

    def __init__(self, file_pattern):
        self._file_pattern = file_pattern

    def expand(self, pcoll):
        return (pcoll.pipeline
                | beam.io.ReadFromText(self._file_pattern)
                | beam.FlatMap(lambda line: re.findall(r'[\w\']+', line.strip(), re.UNICODE)))


p = beam.Pipeline(InteractiveRunner())

words = p | 'read' >> ReadWordsFromText(
    'gs://apache-beam-samples/shakespeare/kinglear.txt')

counts = (words
          | 'count' >> beam.combiners.Count.PerElement())

lower_counts = (words
                | "lower" >> beam.Map(lambda word: word.lower())
                | "lower_count" >> beam.combiners.Count.PerElement())


ib.show(counts)

# Setting up the Apache Beam pipeline options.
options = pipeline_options.PipelineOptions(flags={})

# Sets the project to the default project in your current Google Cloud environment.
_, options.view_as(GoogleCloudOptions).project = google.auth.default()

# Sets the Google Cloud Region in which Cloud Dataflow runs.
options.view_as(GoogleCloudOptions).region = 'us-central1'

# IMPORTANT! Adjust the following to choose a Cloud Storage location.
dataflow_gcs_location = 'gs://cf-dataflow-temp/temp/'

# Dataflow Staging Location. This location is used to stage the Dataflow Pipeline and SDK binary.
options.view_as(
    GoogleCloudOptions).staging_location = '%s/staging' % dataflow_gcs_location

# Dataflow Temp Location. This location is used to store temporary files or intermediate results before finally outputting to the sink.
options.view_as(
    GoogleCloudOptions).temp_location = '%s/temp' % dataflow_gcs_location

# The directory to store the output files of the job.
output_gcs_location = '%s/output' % dataflow_gcs_location

# Specifying the Cloud Storage location to write `counts` to,
# based on the `output_gcs_location` variable set earlier.
(counts | 'Write counts to Cloud Storage'
 >> beam.io.WriteToText(output_gcs_location + '/wordcount-output.txt'))

# Specifying the Cloud Storage location to write `lower_counts` to,
# based on the `output_gcs_location` variable set earlier.
(lower_counts | 'Write lower counts to Cloud Storage'
 >> beam.io.WriteToText(output_gcs_location + '/wordcount-lower-output.txt'))

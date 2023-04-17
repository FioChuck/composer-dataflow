# pytype: skip-file

# beam-playground:
#   name: WordCount
#   description: An example that counts words in Shakespeare's works.
#   multifile: false
#   pipeline_options: --output output.txt
#   context_line: 44
#   categories:
#     - Combiners
#     - Options
#     - Quickstart
#   complexity: MEDIUM
#   tags:
#     - options
#     - count
#     - combine
#     - strings


# #########################################
# import argparse

# import apache_beam as beam
# from apache_beam.options.pipeline_options import PipelineOptions

# parser = argparse.ArgumentParser()
# # parser.add_argument('--my-arg', help='description')
# args, beam_args = parser.parse_known_args()

# # Create and set your PipelineOptions.
# # For Cloud execution, specify DataflowRunner and set the Cloud Platform
# # project, job name, temporary files location, and region.
# # For more information about regions, check:
# # https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
# beam_options = PipelineOptions(
#     beam_args,
#     runner='DataflowRunner',
#     project='my-project-id',
#     job_name='unique-job-name',
#     temp_location='gs://my-bucket/temp',
#     region='us-central1')
# # Note: Repeatable options like dataflow_service_options or experiments must
# # be specified as a list of string(s).
# # e.g. dataflow_service_options=['enable_prime']

# # Create the Pipeline with the specified options.
# with beam.Pipeline(options=beam_options) as pipeline:
#   pass  # build your pipeline here.

# #########################################
import argparse
import logging
import re

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


# class WordExtractingDoFn(beam.DoFn):
#     """Parse each line of input text into words."""

#     def process(self, element):
#         """Returns an iterator over the words of this element.

#         The element is a line of text.  If the line is blank, note that, too.

#         Args:
#           element: the element being processed

#         Returns:
#           The processed element.
#         """
#         return re.findall(r'[\w\']+', element, re.UNICODE)


def run(argv=None, save_main_session=True):
    # """Main entry point; defines and runs the wordcount pipeline."""
    # parser = argparse.ArgumentParser()
    # parser.add_argument(
    #     '--input',
    #     dest='input',
    #     default='gs://dataflow-samples/shakespeare/kinglear.txt',
    #     help='Input file to process.')
    # parser.add_argument(
    #     '--output',
    #     dest='output',
    #     default='gs://cf-dataflow-temp/test.txt',
    #     # required=True,
    #     help='Output file to write results to.')
    # known_args, pipeline_args = parser.parse_known_args(argv)

    # # We use the save_main_session option because one or more DoFn's in this
    # # workflow rely on global context (e.g., a module imported at module level).
    # pipeline_options = PipelineOptions(pipeline_args)
    # pipeline_options.view_as(
    #     SetupOptions).save_main_session = save_main_session

    # # The pipeline will be run on exiting the with block.
    # with beam.Pipeline(options=pipeline_options) as p:

    #########################################
    # https://cloud.google.com/dataflow/docs/guides/setting-pipeline-options#launching_on
    import argparse

    import apache_beam as beam
    from apache_beam.options.pipeline_options import PipelineOptions

    parser = argparse.ArgumentParser()
    # parser.add_argument('--my-arg', help='description')
    args, beam_args = parser.parse_known_args()

    # Create and set your PipelineOptions.
    # For Cloud execution, specify DataflowRunner and set the Cloud Platform
    # project, job name, temporary files location, and region.
    # For more information about regions, check:
    # https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
    beam_options = PipelineOptions(
        beam_args,
        runner='DataflowRunner',
        project='cf-data-analytics',
        job_name='unique-job-name',
        temp_location='gs://cf-dataflow-temp',
        region='us-central1')
    # Note: Repeatable options like dataflow_service_options or experiments must
    # be specified as a list of string(s).
    # e.g. dataflow_service_options=['enable_prime']

    # Create the Pipeline with the specified options.
    with beam.Pipeline(options=beam_options) as p:

        #########################################
        my_column = (p | 'QueryTableStdSQL' >> beam.io.ReadFromBigQuery(
            query='SELECT * EXCEPT(trade_condition) FROM market_data.googl_latest_trade_v', use_standard_sql=True))

        # stats_schema = ','.join(['AIRPORT:string,AVG_ARR_DELAY:float,AVG_DEP_DELAY:float',
        #                          'NUM_FLIGHTS:int64,START_TIME:timestamp,END_TIME:timestamp'])
        # (my_column
        #  | 'bqout' >> beam.io.WriteToBigQuery(
        #             'dsongcp.streaming_delays', schema=stats_schema,
        #             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        #         )
        #  )

    # query_results = p | 'Read' >> beam.io.Read(beam.io.BigQuerySource(
    #     query='select count(*) from cf-data-analytics.market_data.googl'))

    # p | 'Write' >> WriteToText(query_results)

    # Read the text file[pattern] into a PCollection.
    # lines = p | 'Read' >> ReadFromText(known_args.input)

    # counts = (
    #     lines
    #     | 'Split' >> (beam.ParDo(WordExtractingDoFn()).with_output_types(str))
    #     | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
    #     | 'GroupAndSum' >> beam.CombinePerKey(sum))

#    # Format the counts into a PCollection of strings.
#    def format_result(word, count):
#         return '%s: %d' % (word, count)

#     output = counts | 'Format' >> beam.MapTuple(format_result)

    # Write the output using a "Write" transform that has side effects.
    # pylint: disable=expression-not-assigned


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

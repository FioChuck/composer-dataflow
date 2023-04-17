
# https://cloud.google.com/dataflow/docs/concepts/regional-endpoints
# https://cloud.google.com/dataflow/docs/guides/setting-pipeline-options#launching_on
import argparse
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.combiners import Sample


def run(argv=None, save_main_session=True):

    parser = argparse.ArgumentParser()
    args, beam_args = parser.parse_known_args()

    beam_options = PipelineOptions(
        beam_args,
        runner='DataflowRunner',
        project='cf-data-analytics',
        job_name='job-abc',
        temp_location='gs://cf-dataflow-temp',
        region='us-central1')

    with beam.Pipeline(options=beam_options) as p:

        #########################################
        p_col1 = (p | 'read_bq' >> beam.io.ReadFromBigQuery(
            query='SELECT * EXCEPT(trade_condition) FROM market_data.googl_latest_trade_v', use_standard_sql=True))  # googl market data pcollection

        (p_col1
         | 'sample' >> Sample.FixedSizeGlobally(10)
         | 'print' >> beam.Map(print))

        stats_schema = ','.join(
            ['symbol:string,datetime:DATETIME,tm:TIME,dt:DATE,exchange_code:STRING,trade_price:FLOAT,trade_size:INTEGER,trade_id:INTEGER,tape:STRING'])

        (p_col1
         | 'write_bq' >> beam.io.WriteToBigQuery(
             'market_data.stream_out', schema=stats_schema,
             create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
         )
         )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()

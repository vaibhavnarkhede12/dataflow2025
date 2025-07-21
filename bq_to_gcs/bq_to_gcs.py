import json
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.io.gcp.gcsio import GcsIO
import apache_beam as beam
from datetime import datetime
from apache_beam.metrics import Metrics


class AddTimeStampAndToJsonDoFn(beam.DoFn):

    def __init__(self):
        self.processed_rows = Metrics.counter(self.__class__, 'processed_rows')
        self.successful_rows = Metrics.counter(self.__class__, 'successful_rows')
        self.failed_rows = Metrics.counter(self.__class__, 'failed_rows')

    def process(self, row):
        self.processed_rows.inc()
        try:
            # Add a timestamp to the row
            row['timestamp'] = datetime.utcnow().isoformat()
            # Convert the row to JSON
            row_json = json.dumps(row)
            self.successful_rows.inc()
            yield row_json
        except Exception as e:
            # Handle any errors during processing
            row['error'] = str(e)
            self.failed_rows.inc()
            yield json.dumps(row)


def write_to_gcs(json_data, output_path):
    # Write JSON data to a GCS file
    gcs = GcsIO()
    with gcs.open(output_path, 'w') as f:
        for line in json_data:
            f.write(line.encode('utf-8'))
            f.write(b'\n')  # Add a newline after each JSON object


def run():
    options = PipelineOptions(
        project="hsbc-113932821-ihubeucl1-dev",
        region="europe-west2",
        temp_location="gs://hsbc-113932821-ihubeucl1-dev-zs-poc/temp",
        runner="DataflowRunner",
        job_name="bq-to-gcs-job",
        save_main_session=True
    )

    output_gcs_path = "gs://hsbc-113932821-ihubeucl1-dev-zs-poc/output/bq-to-gcs-output.json"

    with beam.Pipeline(options=options) as p:
        # Read from BigQuery
        records = (
            p | "Read from BigQuery" >> ReadFromBigQuery(
                query="""
                    SELECT * FROM `hsbc-113932821-ihubeucl1-dev.ZS_TESTDB_dev.vaibhav_data_test`
                """,
                use_standard_sql=True
            )
        )

        # Process the records: Add timestamp and convert to JSON
        json_records = (
            records
            | "AddTimestampAndConvertToJson" >> beam.ParDo(AddTimeStampAndToJsonDoFn())
        )

        # Write the JSON records to GCS
        json_records | "WriteToGCS" >> beam.io.WriteToText(
            file_path_prefix=output_gcs_path,
            file_name_suffix=".json",
            shard_name_template=""  # Disable sharding to write a single file
        )


if __name__ == "__main__":
    run()
import json
from datetime import datetime
from apache_beam.options.pipelineOptions import PipelineOptions
import apache_beam as beam 
from apache_beam.io.gcp.bigquery import ReadFromBigQuery
from apache_beam.metrics import Metrics


class AddTimeStampAndConvertToJson(beam.DoFn):
    def __init__(self):
       self.processed_rows = Metrics.counter(self.__class__,'processed_rows')
       self.successful_rows = Metrics.counter(self.__class__,'successful_rows')
       self.failed_rows = Metrics.counter(self.__class__,'failed_rows')
    
    def process(self,row):
        self.processed_rows.inc()
        try:
            row['timestamp'] = datetime.utcnow().isoformat()+'z'
            row_json = json.dumps(row)
            self.successful_rows.inc()
            yield row_json
        except:
            row['error'] = str(e)
            self.failed_rows.inc()
            yield json.dumps(row)

def run():
    options=PipelineOptions(
        project="myproject",
        region = "europe-west2",
        temp_location = "gs://gcs-bucket-path/temp",
        runner = "DataflowRunner",
        job_name = "bq_to_gcs",
        save_main_session =  True        
    )
    
    with beam.Pipeline(options=options) as p:
        records = p | "read_from_bigquery" >> ReadFromBigQuery(
            query="""
                select * from thisdataset.thistable
            """
            use_standard_sql = True
        )
        
        
        json_records = records | "AddTimeStampAndConvertToJson" >> beam.ParDo(AddTimeStampAndConvertToJson())
        
        json_records | "writeToGCS" >> beam.io.WriteToText(
            file_path_prefix=output_gcs_path,
            file_name_suffix=".json",
            shard_name_template=""
        )
        


if __name__=="__main__":
    run()

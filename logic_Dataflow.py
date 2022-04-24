import apache_beam as beam
import argparse
import os

from dotenv import load_dotenv
from apache_beam.io.textio import WriteToText
from apache_beam.io.gcp.internal.clients import bigquery as bq
from apache_beam.options.pipeline_options import PipelineOptions

class UnnestCoGrouped(beam.DoFn):
    # This DoFn class unnests the CoGroupBykey output and emits  
    def process(self, tuple):
        # insert Unnest Logic here
        unnested_dict = {
            "First_Name": tuple[1]['file1'][0][0],
            "Last_Name": tuple[1]['file1'][0][1],
            "E-mail": tuple[0],
            "Job_Title": tuple[1]['file2'][0]
        }
        yield unnested_dict 

def run():

    # load environment variables
    load_dotenv()
    parser = argparse.ArgumentParser()
    argv=None
    parser.add_argument(
        "--output",
        dest='output',
        help="Output BigQuery table for results specified as: "
        "PROJECT:DATASET.TABLE or DATASET.TABLE.",
        default='{}.dataflow_table'.format(os.getenv('DATASET_NAME'))
    )
    parser.add_argument(
        "--input1",
        dest='input1',
        help="Location of the file 1 to be processed and loaded",
        default='gs://{}.file1.csv'.format(os.getenv('BUCKET_NAME'))
    )
    parser.add_argument(
        "--input2",
        dest='input2',
        help="Location of the file 2 to be processed and loaded",
        default='gs://{}.file2.csv'.format(os.getenv('BUCKET_NAME'))
    )
    known_args, pipeline_args = parser.parse_known_args(argv) # known_args to be used in GCS ReadFromText
    beam_options = PipelineOptions(
        pipeline_args,
        runner='DataflowRunner',
        project='{}'.format(os.getenv('PROJECT_ID')),
        job_name='dataflow-custom-join',
        temp_location='gs://{}/temp'.format(os.getenv('BUCKET_NAME')),
        region='us-central1'
    )

    with beam.Pipeline(argv=beam_options) as p:

        pcoll_file1 = (p
                        #| 'Create PCol 1' >> beam.io.ReadFromText('Path/To/Local/File/file1.csv', skip_header_lines=1)
                        | 'Read 1 from GCS' >> beam.io.ReadFromText(known_args.input1, skip_header_lines=1)
                        | 'Split CSV 1 into List' >> beam.Map(lambda x: x.split(','))
                        | 'Convert to Key Value pair' >> beam.Map(lambda x: [x[2],[x[0],x[1]]])
        )
        pcoll_file2 = (p
                        #| 'Create PCol 1' >> beam.io.ReadFromText('Path/To/Local/File/file2.csv', skip_header_lines=1)
                        | 'Read 2 from GCS' >> beam.io.ReadFromText(known_args.input2, skip_header_lines=1)
                        | 'Split CSV 2 into List' >> beam.Map(lambda x: x.split(','))
        )
        
        pcoll_merge = ( 
            {'file1': pcoll_file1, 'file2': pcoll_file2}
                | 'CoGroupByKey Modified File' >> beam.CoGroupByKey()
                | 'Unnest the key' >> beam.ParDo(UnnestCoGrouped())
                | 'Writing merged file' >> WriteToText(file_path_prefix='output')
        )
        
        # BigQuery Load part 
        SCHEMA = {
            'fields': [
                {'name': 'First_Name', 'type': 'STRING'},
                {'name': 'Last_Name', 'type': 'STRING'},
                {'name': 'E-mail', 'type': 'STRING'},
                {'name': 'Job_Title', 'type': 'STRING'}
            ]
        }
        table_spec = bq.TableReference(
            projectId='{}'.format(os.getenv('DATASET_NAME')),
            datasetId='{}'.format(os.getenv('BUCKET_NAME')),
            tableId='dataflow_join'
        )

        pcoll_merge | 'Write to BigQuery' >> beam.io.gcp.bq.WriteToBigQuery(
            table_spec,
            SCHEMA,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

if __name__=='__main__':
    run()


# python3 logic_Dataflow.py \
# --runner <DataflowRunner/DirectRunner/any other runner> \
# --project <project_id> \
# --region us-central1 \
# --input1 gs://[BUCKET_NAME]/file1.csv \
# --input2 gs://[BUCKET_NAME]/file2.csv \
# --output <project_id>:<dataset_id>.dataflow_join
# --network <custom_network> \
# --subnetwork https://www.googleapis.com/compute/v1/projects/<project_id>/regions/<region_name>/subnetworks/<subnet_name>
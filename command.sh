python3 logic_Dataflow.py \
--runner <DataflowRunner/DirectRunner/any other runner> \
--project <project_id> \
--region us-central1 \
--input1 gs://[BUCKET_NAME]/file1.csv \
--input2 gs://[BUCKET_NAME]/file2.csv \
--output <project_id>:<dataset_id>.dataflow_join
--network <custom_network> \
--subnetwork https://www.googleapis.com/compute/v1/projects/<project_id>/regions/<region_name>/subnetworks/<subnet_name>
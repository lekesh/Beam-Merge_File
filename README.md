<h1>Merge two files using Dataflow runner</h1>

- logic_Dataflow.py contains the python script that can either read csv files from a GCS Bucket or from local system and upload it to BigQuery sink.
- It uses Apache Beam's CoGroupByKey() method and ParDo function to combine two csv files with a common key
- It then writes the resulting csv to BigQuery sink.  



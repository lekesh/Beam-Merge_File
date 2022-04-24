<h1>Merge two files using Dataflow runner</h1>
<body>
  logic_Dataflow.py contains the python script that can either read csv files from a GCS Bucket or from local system and use Apache Beam's CoGroupByKey() method and ParDo method combine two csv files with a common key and write the resulting csv to BigQuery sink.
</body>

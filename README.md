<h1>Merge two files using Apache Beam Python SDK</h1>

<h2>
  Beam is particularly useful for embarrassingly parallel data processing tasks, in which the problem can be decomposed into many smaller bundles of data that can be processed independently and in parallel.
</h2>
<h2>
  When very large data is used for transformation Beam can show big difference in data processing time compared to conventional method.
</h2>
  
_logic_Dataflow.py_ contains the python script that can either read csv files from a GCS Bucket or from local system and upload it to BigQuery sink.

- It uses Apache Beam's _CoGroupByKey()_ method and _ParDo_ function to combine two csv files with a common key
- It then writes the resulting _.csv_ to BigQuery sink using _WriteToBigQuery()_ method

<h3>When using _DirectRunner_</h3>

1. Create a virtual environment using python's virtualenv and install all the required packages from the _requirements.txt_
2. Create a _.env_ file and include all the environment variables and install python-dotenv package in the virtual environment
3. Edit and run the _command.sh_ file in bash or zsh CLI

<h3>When using _DataflowRunner_</h3>

1. Upload the csv files and python file to a GCS Bucket
2. Replace all the environment variables with explicit values 
3. Create a Dataflow job with custom template and select the python script from the GCS Bucket

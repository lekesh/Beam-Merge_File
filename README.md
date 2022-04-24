<h1>Merge two files using Dataflow runner</h1>

_logic_Dataflow.py_ contains the python script that can either read csv files from a GCS Bucket or from local system and upload it to BigQuery sink.

- It uses Apache Beam's _CoGroupByKey()_ method and _ParDo_ function to combine two csv files with a common key
- It then writes the resulting _.csv_ to BigQuery sink using _WriteToBigQuery()_ method

<h2>When using _DirectRunner_</h2>

1. Create a virtual environment using python's virtualenv and install all the required packages from the _requirements.txt_
2. Create a _.env_ file and include all the environment variables and install python-dotenv package in the virtual environment
3. Edit and run the _command.sh_ file in bash or zsh CLI

<h2>When using _DataflowRunner_</h2>

1. Upload the csv files and python file to a GCS Bucket
2. Replace all the environment variables with explicit values 
3. Create a Dataflow job with custom template and select the python script from the GCS Bucket

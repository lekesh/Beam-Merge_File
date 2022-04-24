<h1>Merge two files using Dataflow runner</h1>

_logic_Dataflow.py_ contains the python script that can either read csv files from a GCS Bucket or from local system and upload it to BigQuery sink.

- It uses Apache Beam's _CoGroupByKey()_ method and _ParDo_ function to combine two csv files with a common key
- It then writes the resulting _.csv_ to BigQuery sink using _WriteToBigQuery()_ method

When using _DirectRunner_

Step - 1:
Create a virtual environment using python's virtualenv and install all the required packages from the _requirements.txt_

Step - 2:
Create a _.env_ file and include all the environment variables and install python-dotenv package in the virtual environment

Step - 3:
Edit and run the _command.sh_ file in bash or zsh CLI

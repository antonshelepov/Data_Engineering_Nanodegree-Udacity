## Data Pipelines with Airflow

## Project Summary

Sparkify, a music streaming company, developed a successful business. Due to large amount of data, which resides in S3, and particularly strong interest in monitoring it frequently, the company tasked me with building an automated solution. Automation and monitoring of the data warehouse ETL pipeline can be achieved with Apache Airflow.

My main goal in this project was to utilize Airflow's built-in functionalities (connection, hooks) as much as possible. The pipeline I set up is run dynamically, tasks are reusable and clearly defined. This allows for good monitoring, and precise quality checks.

## Datasets

There are two sources of data to be managed:

- ```s3://udacity-dend/song_data/``` - metadata on songs and artists 
- ```s3://udacity-dend/log_data/``` - logdata on events from Sparkify app

## File Structure and How to

| File                                               | Purpose                                                      |
| -------------------------------------------------- | ------------------------------------------------------------ |
| ```/airflow/create_tables.sql```                   | Creates staging and data warehouse tables in redshift        |
| ```/airflow/plugins/operators/stage_redshift.py``` | Custom operator **StageToRedshiftOperator**. Loads data from S3 to staging tables in Redshift |
| ```/airflow/plugins/operators/load_fact.py```      | Custom operator **LoadFactOperator**. Loads data from staging tables into the main fact table |
| ```/airflow/plugins/operators/load_dimension.py``` | Custom operator **LoadDimensionOperator**. Loads data into dimension tables |
| ```/airflow/plugins/operators/data_quality.py```   | Custom operator **DataQualityOperator**. Performs quality checks at the end of a pipeline |
| ```/airflow/dags/sparkify_dag.py```                | DAG definition file                                          |

In order to use this repo one has to have valid AWS credentials, as well as functioning Amazon Redshift Cluster.

Steps to follow:

1. Airflow installation on a local machine, or in a Docker Container
2. DAG script parameters can be adjusted, if necessary (by default Airflow will run the script once an hour)
3. ```aws_credentials``` and ```redshift``` connections must be stored in Airflow UI
4. Run ```sparkify_dag.py``` from Airflow UI and wait until it is finished

## References

[Apache Airflow on Docker](https://medium.com/@itunpredictable/apache-airflow-on-docker-for-complete-beginners-cf76cf7b2c9agg) <br>


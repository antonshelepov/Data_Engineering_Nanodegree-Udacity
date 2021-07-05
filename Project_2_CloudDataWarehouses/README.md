## Data Warehouse - ETL Process - Sparkify

### Project Summary

Sparkify is a music streaming startup. Although new to the market, their customer base was growing rapidly over the last year. So did the amount of data the company has to manage. The data on user activity on the app is stored in as JSON logs in S3, JSON metadata in the app itself stores data on the songs.

In order to operate more efficiently I was tasked with building an ETL pipeline to extract data from S3, stage it in Redshift, and after a transformation process store it into a set of dimensional tables. This way the analytics team will have a much better platform for data analysis. 

### File Structure and How to

| File                  | Description                                                  |
| --------------------- | ------------------------------------------------------------ |
| aws_cluster_main.py   | Consists of functions in order to create AWS IAM Role, Redshift Cluster, as well as well as availability check function. Cluster delete function is also here. |
| aws_cluster_check.py  | Script that checks, if the created cluster is live.          |
| aws_cluster_delete.py | Script that deletes the cluster. MUST BE RUN EACH TIME AFTER WORK IS DONE |
| create_tables.py      | Creates defined tables                                       |
| elt.py                | Loads data into Data Warehouse                               |
| sql_queries.py        | Contains all the necessary SQL statements in order to ```DROP```, ```CREATE``` and ```INSERT``` tables and data respectively |
| analytics.py          | Queries to check, if the ETL worked correctly                |
| dwh.cfg               | Config file with all the needed configuration variables      |

#### Configuring AWS Access

#### Building Infrastructure

1. Run ```aws_cluster_main.py``` in order to create a Redshift cluster with the needed IAM Role access permissions. 
2. It takes 3-5 Minutes on average for cluster to be created. Run ```aws_cluster_check.py``` a couple of times to make sure that the cluster war created. Cluster creation can alternatively be verified from AWS UI.
3. After work is done, run ```aws_cluster_delete.py``` to delete the Redshift cluster. Otherwise, you will be charged.

### ETL Process

There are two scripts to be mentioned (both can be activated with python command):

* ```create_tables.py``` - Drops all tables and recreates them
* ```etl.py``` - Purpose of this script is to:
  * Load log files from S3 Bucket to the staging tables
  * Do necessary data transformation and insert it to analytical tables afterwards 

### Database Schema

Data goes through a predefined transformation process and is inserted into a Data Warehouse afterwards. Below you can see the database structure, which is based on a *star schema*

![schema](/images/schema.png)

### Analytics

Run ```analytics.py``` in order to get row count from each table.

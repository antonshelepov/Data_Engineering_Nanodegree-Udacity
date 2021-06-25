## Data Warehouse - ETL Process - Sparkify

## Project Summary

Sparkify is a music streaming startup. Although new to the market, their customer base was growing rapidly over the last year. So did the amount of data the company has to manage. The data on user activity on the app is stored in as JSON logs in S3, JSON metadata in the app itself stores data on the songs.

In order to operate more efficiently I was tasked with building an ETL pipeline to extract data from S3, stage it in Redshift, and after a transformation process store it into a set of dimensional tables. This way the analytics team will have a much better platform for data analysis. 

## File Structure and How to

### Configuring AWS Access

### Building Infrastructure

## ETL Process

There are two scripts to be mentioned (both can be activated with python command):

* ```create_tables.py``` - Drops all tables and recreates them
* ```etl.py``` - Purpose of this script is to:
  * Load log files from S3 Bucket to the staging tables
  * Do necessary data transformation and insert it to analytical tables afterwards 

## Dataset, Database Schema

## 

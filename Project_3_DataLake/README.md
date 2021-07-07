## Data Lake - Sparkify
### Project Summary

Sparkify was initially a music streaming startup. Over the last periods it has grown significantly. The amount of data collected (mainly logs and app data) got disproportionately big in comparison to the standard methods of evaluating it. Data resides in S3 and consists of logs on user activity on the app (format: JSON), and metadata on the songs (format: JSON).

As a data engineer, I was tasked with figuring out a way to handle the data more efficiently, thereafter increasing value of it. The process of data extraction is pretty straightforward and can be summarized in the following steps: extract data from S3, transform it using Spark, and load it back to S3 as a set of dimensional tables. This way the analytics team is able to access data in order to gather insights.

### How to

If using locally no ```df.cfg``` modification needed. Also be warned: comment **config** parameter from SparkSession out. Otherwise, fill in your AWS Credentials.

Run ```etl.py``` . The ETL will process data and put it into five corresponding tables (directories).

### Files in Repository

As mentioned earlier, the ETL produces a set of directories, data will then be stored in parquet format on S3. Each directory can then be translated into a table with specific data. It is worth mentioning that files from table *songs* are partitioned by year and then artist, whereas files belonging to *time* table are partitioned by year and month. *Songplays* files are partitioned by year and month.   

| Table (Directory)         | Description                                                  | Dimensions                                                   |
| ------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
| *songplays*  (Fact Table) | records from log data filtered by *page == NextSong*         | songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent |
| *users*                   | users in the app                                             | user_id, first_name, last_name, gender, level                |
| *songs*                   | songs in database                                            | song_id, title, artist_id, year, duration                    |
| *artists*                 | artists in database                                          | artist_id, name, location, latitude, longitude               |
| *time*                    | timestamps of songplays; parsed into time and date components | start_time, hour, day, week, month, year, weekday            |

### Discussion

#### Schema design and ETL pipeline justification

A set of dimensional tables, with a Fact Table storing quantitative information for analysis, is a time-tested concept for storing data. Pipeline was developed based on the needs of the stakeholders.

#### Takeaways

The more data must be processed, the more time it takes. Each minor mistake in the pipeline can lead to postponed schedule. It is therefore important to develop a habit of testing a data transformation pipeline thoroughly, strictly speaking, on a small dataset. This way a lot of frustration can be saved.

I also found [**docs**](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrameReader.json.html?highlight=json) on how to read JSON with spark helpful, especially the parameters *mode='PERMISSIVE'* and *columnNameOfCorruptRecord='corrupt_record'*. 

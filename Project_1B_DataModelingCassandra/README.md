# Data Modeling with Cassandra

## Project Summary
Sparkify, a startup with specialization in music streaming, was eager to analyse the collection of songs and user activity. They were particularly interested in which songs their users listening to. This information was not easily accesible to the analyst team due to the location and format: CSV files in a directory on the app.
In order to get useful information from the data provided Sparkify needed a data engineer and so they hired me. My assignment was to create a database which would allow the analysis described above.

## Database
### Datasets
The data is stored in 30 .csv files in a directory **event_data**. File name contains the date it was generated.
*Sample record*:
> ```event_data/2018-11-08-events.csv```

Each .csv has following columns names:
> ```
> "artist" 
> "firstName"
> "gender"
> "itemInSession"
> "lastName"
> "length"
> "level"
> "location"
> "sessionId"
> "song"
> "userId"
> ```

Overall **6821** rows of data have to be extracted and transformed.

![data](image_event_datafile_new.jpg)
*data how it is stored in .csv*

### Database consideration
When choosing database, following aspects were considered:
* it needed to be highly available
* fast reads and writes
* large amounts of data (long-term)
* linear scalability

All the mentioned aspects can be easily achieved with Apache Cassandra

## Project File Structure
```event_datafile_new.csv``` - database <br>
```Project_1B_main.ipynd``` - Jupiter notebook with transformation methods

## How to
Details on ```How to``` use the notebook can be found in it above each code block.

## References
[Apache Cassandra Documentation](https://cassandra.apache.org/doc/latest/) <br>
[Pandas Documentation](https://pandas.pydata.org/pandas-docs/stable/) <br>
[NumPy Documentation](https://numpy.org/doc/) <br>
[Psycopg Documentation](https://www.psycopg.org/docs/)

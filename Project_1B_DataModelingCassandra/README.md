# Data Modeling with Cassandra

## Project Summary
Sparkify, a startup with specialization in music streaming, is eager to analyse the collection of songs and user activity. They were particularly interested in which songs their users listening to. This information was not easily accesible to the analyst team due to the location and format: CSV files in a directory on the app.
In order to get useful information from the data provided Sparkify needed a data engineer and so they hired me. My assignment was to create a database which would allow the analysis described above.

## Database
### Datasets
The data is stored in 30 .csv files in a directory **event_data**. File name contains the date it was generated.
*Sample record*:
> ```event_data/2018-11-08-events.csv```

Each .csv has following columns names:
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

Overall **6821** rows of data have to be extracted and transformed.
### Database consideration


## Project File Structure

## How to

## References
[PostgreSQL Documentation](https://www.postgresql.org/docs/) <br>
[Pandas Documentation](https://pandas.pydata.org/pandas-docs/stable/) <br>
[Psycopg Documentation](https://www.psycopg.org/docs/)

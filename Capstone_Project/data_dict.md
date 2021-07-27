### Capstone Project

#### Data dictionaries

##### Table immigrants

| name_column    | type               | description                                                  |
| -------------- | ------------------ | ------------------------------------------------------------ |
| i94id          | SERIAL             | Unique Primary Key                                           |
| CoC            | VARCHAR            | Country of Citizenship code                                  |
| CoR            | VARCHAR            | Country of Residence code                                    |
| PoE            | VARCHAR            | Three character code of destination USA city (Port of Entry) |
| state_landing  | VARCHAR            | State code                                                   |
| age            | INTEGER            | Age of Respondent in Years                                   |
| visa_issued_in | VARCHAR            | Department of State where Visa was issued                    |
| occup          | VARCHAR            | Occupation that will be performed in U.S.                    |
| year_birth     | INTEGER            | Four digit year of birth                                     |
| gender         | VARCHAR            | Non-immigrant sex                                            |
| airline_used   | VARCHAR            | Airline used to arrive in U.S.                               |
| num_flight     | VARCHAR            | Flight number of Airline used to arrive in U.S.              |
| visatype       | VARCHAR            | Class of admission legally admitting the non-immigrant to temporarily stay in U.S |
| dt_arrival     | TIMESTAMP          | arrival date in the USA                                      |
| dt_departure   | TIMESTAMP          | departure date from the USA                                  |
| month          | VARCHAR            | Additional column to partition by                            |
| CONSTRAINT     | PRIMARY KEY(i94id) |                                                              |

##### Table airports

| name_column     | type                              | description                                                  |
| --------------- | --------------------------------- | ------------------------------------------------------------ |
| city_name       | VARCHAR                           | Name of a city                                               |
| airport_name    | VARCHAR                           | Name of an airport                                           |
| iata_code       | VARCHAR                           | A three-letter geocode designating many airports and metropolitan areas around the world, defined by the International Air Transport Association |
| airport_size    | VARCHAR                           | Size of an airport (Categorical)                             |
| count_passenger | INTEGER                           | Amount of passengers an airport can proceed annually         |
| I94_port_code   | VARCHAR                           | Three character code of destination USA city (Port of Entry) |
| state_name      | VARCHAR                           | State code                                                   |
| elevation_ft    | INTEGER                           | Distance above sea level                                     |
| iso_country     | VARCHAR                           | Country code based on ISO 3166                               |
| iso_region      | VARCHAR                           | Defines codes for identifying the principal subdivisions of all countries coded in ISO 3166-1 |
| municipality    | VARCHAR                           | Name of a single administrative division having corporate status |
| gps_code        | VARCHAR                           | GPS code of an airport                                       |
| lat             | DECIMAL(16,5)                     | A geographic coordinate that specifies the north–south position of a point on the Earth's surface |
| lng             | DECIMAL(16,5)                     | A geographic coordinate that specifies the east–west position of a point on the Earth's surface |
| CONSTRAINT      | PRIMARY KEY(city_name,state_name) |                                                              |

##### Table ccodes

| name_column             | type                          | description                                                  |
| ----------------------- | ----------------------------- | ------------------------------------------------------------ |
| I94_country_code        | INTEGER                       | Country code as defined for I-94 Form                        |
| county_name             | VARCHAR                       | Name of a country                                            |
| country_alpha_2         | VARCHAR                       | Alpha-2 codes are two-letter country codes defined in ISO 3166-1 |
| country_alpha_3         | VARCHAR                       | Alpha-3 codes are three-letter country codes defined in ISO 3166-1 |
| country_code            | VARCHAR                       | Three-digit [country codes](https://en.wikipedia.org/wiki/Country_code) defined in [ISO 3166-1](https://en.wikipedia.org/wiki/ISO_3166-1) |
| country_iso_3166_2      | VARCHAR                       | ISO 3166-2 is part of the ISO 3166 standard published by the International Organization for Standardization, and defines codes for identifying the principal subdivisions of all countries coded in ISO 3166-1 |
| country_region          | VARCHAR                       | Region a country located in                                  |
| country_sub_region      | VARCHAR                       | Sub region a country located in                              |
| country_region_code     | VARCHAR                       | Region code a country located in                             |
| country_sub_region_code | VARCHAR                       | Sub region code a country located in                         |
| CONSTRAINT              | PRIMARY KEY(I94_country_code) |                                                              |

##### Table demographics

| name_column        | type                              | description                                                  |
| ------------------ | --------------------------------- | ------------------------------------------------------------ |
| city_name          | VARCHAR                           | Name of a city                                               |
| state_name         | VARCHAR                           | Name of a state                                              |
| state_code         | VARCHAR                           | State code (Categorical)                                     |
| age_median         | NUMERIC                           | Median of age                                                |
| popul_male         | INTEGER                           | Number of male population                                    |
| popul_female       | INTEGER                           | Number of female population                                  |
| household_size_ave | NUMERIC                           | Average size of a household                                  |
| popul_total        | INTEGER                           | Total population                                             |
| share_female       | NUMERIC                           | Share of females on total                                    |
| popul_density      | NUMERIC                           | Density of a city population                                 |
| lat                | DECIMAL(16,5)                     | A geographic coordinate that specifies the north–south position of a point on the Earth's surface |
| lng                | DECIMAL(16,5)                     | A geographic coordinate that specifies the east–west position of a point on the Earth's surface |
| CONSTRAINT         | PRIMARY KEY(city_name,state_name) |                                                              |

##### Table visatype

| name_column | type                  | description                |
| ----------- | --------------------- | -------------------------- |
| visatype    | VARCHAR               | Type of Visa (Categorical) |
| description | TEXT                  | Visa description           |
| CONSTRAINT  | PRIMARY KEY(visatype) |                            |
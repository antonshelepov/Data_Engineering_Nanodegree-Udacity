### Data Engineering Capstone Project

#### Project Summary

An international beverage production company called "A sweet thing" is interested in increasing their sales.  Present in more than 50 countries it sees the greatest potential in North America, the last continent the company couldn't take a step in, until now.

The rapidly increasing popularity of the company's main product, bio energy juice, and solid sales backend the idea of expanding into one of the most profitable market spaces. The U.S.A. has been strategically (due to it's geographical position) chosen by the marketing department as a first country to expand to.

As the country of choice is big, has several time zones and culturally rich, the C-level of the "A sweet thing" decided to gather data in order to minimize initial costs for the project. Marketing department has gathered fundamental insights on customers in the countries the product is being sold. 

Next step is to analyze US immigration data in order to understand, which locations (Airports, cities, states) to approach first. The plan is to offer the product to the masses over the people familiar with it already. So, firstly, the beverage in planned to be sold to the immigrants, arriving and leaving in the U.S.A temporarily. Afterwards, as the popularity of the brand grows, the product can be introduced to Americans.

As a Data Engineer of "A sweet Thing" I was tasked with finding an appropriate data set, find (in cooperation with stakeholders) valuable information and 

Extended information about data can be found in two provided jupyter notebooks.

#### How to
* run ```process_input.py``` in order to process data from ./input folder
* run ```create_tables.py``` so that Data Warehouse tables can be created
* run ```etl.py``` in order to upload processed data from ./output to Data Warehouse

#### Data
| name_file                       | source                                                       | name_format | name_df         | description                                                  |
| ------------------------------- | ------------------------------------------------------------ | ----------- | --------------- | ------------------------------------------------------------ |
| immigration_data_sample.csv     | Udacity                                                      | csv         | df_immi         | This dataset (sample) comes from the US National Tourism and Trade Office and holds data about |
| visatype.csv                    |                                                              | csv         | df_visatype     | Holds data about different visa types a person can apply for based on his immigration purpose |
| country_codes.csv               | [Github / lukes](https://github.com/lukes/ISO-3166-Countries-with-Regional-Codes) | csv         | df_ccodes       | ISO 3166-1 country lists merged with their UN Geoscheme regional codes |
| us-cities-demographics.csv      | Udacity                                                      | csv         | df_demographics |                                                              |
| airport-codes_csv.csv           | Udacity                                                      | csv         | df_airports     |                                                              |
| international_airports_US.csv   | [Wikipedia](https://en.wikipedia.org/wiki/List_of_international_airports_by_country#United_States) | csv         | df_inter_US     |                                                              |
| us-states.csv                   | Internet                                                     | csv         | df_states_US    |                                                              |
| I94_SAS_Labels_Descriptions.SAS | Udacity                                                      | SAS         |                 | Provided labels for CoC, CoR, and PoE                        |
| uszips.csv                      | Internet                                                     | csv         | df_uszips       | Valuable source of information for zips, county_fips etc.    |


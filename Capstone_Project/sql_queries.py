immi_table_drop = "DROP TABLE IF EXISTS immigrants;"
airports_table_drop = "DROP TABLE IF EXISTS airports;"
ccodes_table_drop = "DROP TABLE IF EXISTS ccodes;"
demographics_table_drop = "DROP TABLE IF EXISTS demographics;"
visatype_table_drop = "DROP TABLE IF EXISTS visatype;"


# create tables
fact_immi_table_create = ("""CREATE TABLE IF NOT EXISTS immigrants (i94id SERIAL, \
                                                                CoC VARCHAR, \
                                                                CoR VARCHAR, \
                                                                PoE VARCHAR, \
                                                                state_landing VARCHAR, \
                                                                age INTEGER, \
                                                                visa_issued_in VARCHAR, \
                                                                occup VARCHAR, \
                                                                year_birth INTEGER, \
                                                                gender VARCHAR, \
                                                                airline_used VARCHAR, \
                                                                num_flight VARCHAR, \
                                                                visatype VARCHAR, \
                                                                dt_arrival TIMESTAMP, \
                                                                dt_departure TIMESTAMP, \
                                                                month VARCHAR, \
                                                                PRIMARY KEY(i94id));
 """)
 
dim_airports_table_create = ("""CREATE TABLE IF NOT EXISTS airports (city_name VARCHAR, \
                                                                 airport_name VARCHAR, \
                                                                 iata_code VARCHAR, \
                                                                 airport_size VARCHAR, \
                                                                 count_passenger INTEGER, \
                                                                 I94_port_code VARCHAR, \
                                                                 state_name VARCHAR, \
                                                                 elevation_ft INTEGER, \
                                                                 iso_country VARCHAR, \
                                                                 iso_region VARCHAR, \
                                                                 municipality VARCHAR, \
                                                                 gps_code VARCHAR, \
                                                                 lat DECIMAL(16,5), \
                                                                 lng DECIMAL(16,5), \
                                                                 PRIMARY KEY(city_name,state_name));
 """)
 
dim_ccodes_table_create = ("""CREATE TABLE IF NOT EXISTS ccodes (I94_country_code INTEGER, \
                                                             county_name VARCHAR, \
                                                             country_alpha_2 VARCHAR, \
                                                             country_alpha_3 VARCHAR, \
                                                             country_code INTEGER, \
                                                             country_iso_3166_2 VARCHAR, \
                                                             country_region VARCHAR, \
                                                             country_sub_region VARCHAR, \
                                                             country_region_code INTEGER, \
                                                             country_sub_region_code INTEGER, \
                                                             PRIMARY KEY(I94_country_code));
 """)
 
dim_demographics_table_create = ("""CREATE TABLE IF NOT EXISTS demographics (city_name VARCHAR, \
                                                                         state_name VARCHAR, \
                                                                         state_code VARCHAR, \
                                                                         age_median NUMERIC, \
                                                                         popul_male INTEGER, \
                                                                         popul_female INTEGER, \
                                                                         household_size_ave NUMERIC, \
                                                                         popul_total INTEGER, \
                                                                         share_female NUMERIC, \
                                                                         popul_density NUMERIC, \
                                                                         lat DECIMAL(16,5), \
                                                                         lng DECIMAL(16,5), \
                                                                         PRIMARY KEY(city_name,state_name));
 """)
 
dim_visatype_table_create = ("""CREATE TABLE IF NOT EXISTS visatype (visatype VARCHAR, \
                                                                 description TEXT, \
                                                                 PRIMARY KEY(visatype));
 """)
 
 
# insert records
immi_table_insert = ("""INSERT INTO immigrants (CoC,CoR,PoE,state_landing,age,visa_issued_in,occup,year_birth,gender,airline_used,num_flight,visatype,dt_arrival,dt_departure,month)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (i94id)
                            DO NOTHING;
""")

airports_table_insert = ("""INSERT INTO airports (city_name,airport_name,iata_code,airport_size,count_passenger,I94_port_code,state_name,elevation_ft,iso_country,iso_region,municipality,gps_code,lat,lng)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (city_name,state_name)
                            DO NOTHING;
""")

ccodes_table_insert = ("""INSERT INTO ccodes (I94_country_code,county_name,country_alpha_2,country_alpha_3,country_code,country_iso_3166_2,country_region,country_sub_region,country_region_code,country_sub_region_code)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (I94_country_code)
                            DO NOTHING;
""")

demographics_table_insert = ("""INSERT INTO demographics (city_name,state_name,state_code,age_median,popul_male,popul_female,household_size_ave,popul_total,share_female,popul_density,lat,lng)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (city_name,state_name)
                            DO NOTHING;
""")

visatype_table_insert = ("""INSERT INTO visatype (visatype,description)
                        VALUES (%s,%s);
""")


# analytics queries
tables_list = ['immigrants', 'airports', 'ccodes', 'demographics', 'visatype']

# query lists
create_table_queries = [fact_immi_table_create, 
                        dim_airports_table_create, 
                        dim_ccodes_table_create, 
                        dim_demographics_table_create, 
                        dim_visatype_table_create]
drop_table_queries = [immi_table_drop, 
                      airports_table_drop, 
                      ccodes_table_drop, 
                      demographics_table_drop, 
                      visatype_table_drop]

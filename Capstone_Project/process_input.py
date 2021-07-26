import pandas as pd
import numpy as np
import os
import re
import json
import glob

# define spark modules
from pyspark.sql import SparkSession

from pyspark.sql.types import IntegerType, DateType

from pyspark.sql.functions import udf

from pyspark.sql.functions import month


spark = SparkSession.builder.\
config("spark.jars.repositories", "https://repos.spark-packages.org/").\
config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
enableHiveSupport().getOrCreate()

def get_I94port(ports):
    """This method
    
    Params:
    
    Returns:
    """
    ports_strip = [p.strip() for p in ports]
    ports_known = [port for port in ports_strip if 'No PORT Code' not in port and 'Collapsed' not in port]
    ports_cleaned = [port.replace('\t','') for port in ports_known]
    ports_results = []
    for port in ports_cleaned:
        match = re.search("""\'([A-Z0-9]{3})\'\S*=\S*\'([A-Z\(\)\.\s\/-]*),\s?([A-Z]{2})(\s\(BPS\)|\s#ARPT|\s*)\'""", port)
        if match:
            code, city, state = match.group(1), match.group(2), match.group(3).strip()
            ports_results.append((code, city, state))
    df = pd.DataFrame(ports_results, columns=['I94_port_code', 'I94_port_city', 'I94_port_state'])
    return df
    
def get_I94cit(cit_codes):
    """This method
    
    Params:
    
    Returns:
    """
    cit_codes_strip = [c.strip() for c in cit_codes]
    cit_codes_valid = [c for c in cit_codes_strip if 'No Country Code' not in c 
                                                 and 'INVALID:' not in c 
                                                 and '(should not show)' not in c]
    cit_codes_cleaned = [c.replace('\t','') for c in cit_codes_valid]
    cit_results = []
    for c in cit_codes_cleaned:
        match = re.search("""(\d{3})\s?=\s*\'(.*)\'""", c)
        if match:
            cit_results.append((match.group(1), match.group(2)))
    #cit_codes_json = json.dumps(dict(cit_results))
    df = pd.DataFrame(cit_results, columns=['I94_country_code', 'I94_country'])
    return df
    
def get_I94addr(addr_states):
    """This method
    
    Params:
    
    Returns:
    """
    addr_states_strip = [a.strip() for a in addr_states]
    addr_states_cleaned = [a.replace('\t','') for a in addr_states_strip]
    addr_results = []
    for a in addr_states_cleaned:
        match = re.match("""\'([A-Z]{2})\'=\'([A-Z\.\s]*)\'""", a)
        if match:
            addr_results.append((match.group(1), match.group(2)))
    df = pd.DataFrame(addr_results, columns=['I94_state_code', 'I94_state'])
    return df
    
def get_count(self):
    """This method
    
    Params:
    
    Returns:
    """
    if 'No' in self or 'TBA' in self or 'Unknown' in self or 'Service' in self:
        return 0
    else:
        try:
            split = self.split('[')
        except ValueError:
            pass
        else:
            count = int(split[0].replace(',',''))
            return count

def timed(fn):
    """This method
    
    Params:
    
    Returns:
    """
    from time import perf_counter
    from functools import wraps
    
    @wraps(fn)
    def inner(*args,**kwargs):
        time_start = perf_counter()
        result = fn(*args,**kwargs)
        time_stop = perf_counter()
        elapsed = time_stop - time_start
        print('{0} took {1:.6f}s to run.'.format(fn.__name__,elapsed))
        return result
    return inner

def remove_whitespace(df):
    """This method 
    
    Params:
        df: dataframe 
    
    Returns: dataframe
    """
    for col in df.columns:
        if df[col].dtype == 'object':                      
            mask = df[col].notnull()
            df.loc[mask,col] = df.loc[mask,col].map(str.strip)            
    return df

# passed
@timed
def process_visatype(df_visatype):
    """This method
    
    Params:
      
    Returns: 
    """
    print(f'Processing: df_visatype')
    df_visatype["visatype"] = df_visatype["visatype"].map(str.strip)
    return df_visatype

# passed
@timed
def process_ccodes(I94_cit_codes, df_ccodes):
    """This method
    
    Params:
    
    Returns:
    """
    print(f'Processing: df_ccodes')
    # apply lower() of both dataframes in order to map them by country name
    I94_cit_codes["I94_country_low"] = I94_cit_codes["I94_country"].apply(lambda x: x.lower())
    df_ccodes["name_low"] = df_ccodes.name.apply(lambda x: x.lower())
    # 
    df_I94_merged = I94_cit_codes.merge(df_ccodes, 
                                        how='left', 
                                        left_on='I94_country_low', 
                                        right_on='name_low')
    # remove all NaN values
    df_ccodes_final = df_I94_merged[df_I94_merged.name.notna()].copy()
    cols_2_drop = ['I94_country', 'I94_country_low', 'intermediate_region', 'intermediate_region_code', 'name_low']
    df_ccodes_final.drop(cols_2_drop, inplace = True, axis = 1)
    df_ccodes_final.rename(columns={"name":"county_name",
                              "alpha_2":"country_alpha_2",
                              "alpha_3":"country_alpha_3",
                              "iso_3166_2":"country_iso_3166_2",
                              "region":"country_region",
                              "sub_region":"country_sub_region",
                              "region_code":"country_region_code",
                              "sub_region_code":"country_sub_region_code"}, inplace=True)
    # set type int
    cols_float_2_int = ['country_sub_region_code']
    for col in cols_float_2_int:
        df_ccodes_final[col] = df_ccodes_final[col].replace(np.nan, 0)
        df_ccodes_final[col] = df_ccodes_final[col].astype(int)
    return df_ccodes_final

# passed
@timed
def process_airports(I94_ports, df_airports, df_international):
    """This method
    
    Params:
    
    Returns:
    """
    print(f'Processing: df_airports')
    # get passengers count were defined
    df_international["passengers_count"] = df_international["2018_passengers"].apply(get_count)
    # lower() column location in I94_ports and df_international in order to match it with another dataframe
    df_international["Location_lower"] = df_international.Location.apply(lambda x: x.lower())
    I94_ports["I94_port_city_lower"] = I94_ports["I94_port_city"].apply(lambda x: x.lower())
    # merge dataframes
    df_inter_merged = df_international.merge(I94_ports, 
                                              how='left', 
                                              left_on='Location_lower', 
                                              right_on='I94_port_city_lower')
    # remove all notna()
    df_inter_merged_final = df_inter_merged[df_inter_merged.I94_port_city.notna()]
    # filter by column Passenger_Role for only 'Small', 'Medium', or 'Large'
    df_inter_merged_final = df_inter_merged_final[(df_inter_merged_final.Passenger_Role != "Non-Hub/Reliever") & 
                                                  (df_inter_merged_final.Passenger_Role != "Non-Hub") &
                                                  (df_inter_merged_final.Passenger_Role != "Reliever")]
    # filter only 'US' airports
    df_airports_us = df_airports[df_airports.iso_country == "US"]
    # filter only airports with a IATA code
    df_airports_iata = df_airports_us[~df_airports_us.iata_code.isnull()]
    # filter medium / large sized airports
    airport_type = ['medium_airport', 'large_airport']
    df_airports_type = df_airports_iata[df_airports_iata.type.isin(airport_type)]
    # use column iso_region in order to generate state
    df_airports_temp = df_airports_type.copy()
    df_airports_temp["state"] = df_airports_temp.iso_region.apply(lambda x: re.match(r'US-(.*)',str(x)).group(1) if x else x)
    # use column coordinates in order to generate latitude and longitude
    df_airports_temp['latitude'] = df_airports_temp.coordinates.str.split(", ",expand=True)[0]
    df_airports_temp['longitude'] = df_airports_temp.coordinates.str.split(", ",expand=True)[1]
    # 
    df_inter_final = df_inter_merged_final.merge(df_airports_temp, 
                                                 how='left', 
                                                 left_on='IATA_Code', 
                                                 right_on='iata_code')
    # drop columns which are not essential for further analysis
    cols_2_drop = ['Unnamed: 0', '2018_passengers', 'Location_lower', 'state', 'I94_port_city', 'I94_port_city_lower', 'ident', 'type', 'name', 'continent', 'iata_code', 'local_code', 'coordinates']
    df_inter_final.drop(cols_2_drop, inplace = True, axis = 1)  
    # rename columns
    df_inter_final.rename(columns={"Location":"city_name",
                                   "Airport":"airport_name",
                                   "IATA_Code":"iata_code",
                                   "I94_port_state":"state_name",
                                   "Passenger_Role":"airport_size",
                                   "passengers_count":"count_passenger",
                                   "latitude":"lat",
                                   "longitude":"lng"}, inplace=True)
    # set type from float to integer
    cols_float_2_int = ['elevation_ft']
    for col in cols_float_2_int:
        df_inter_final[col] = df_inter_final[col].replace(np.nan, 0)
        df_inter_final[col] = df_inter_final[col].astype(int)
    return df_inter_final

# passed
@timed
def process_demographics(df_demographics, df_uszips):
    """This method
    
    Params:
    
    Returns:
    """
    print(f'Processing: df_demographics')
    # convert multiple columns from floats to integer
    cols_float_2_int = ['Male Population', 'Female Population', 'Total Population']
    for col in cols_float_2_int:
        df_demographics[col] = df_demographics[col].replace(np.nan, 0)
        df_demographics[col] = df_demographics[col].astype(int)
    # filter rows where male + femal population = total; otherwise incorrect data
    df_demographics = df_demographics[(df_demographics['Male Population'] + 
                                       df_demographics['Female Population'] == 
                                       df_demographics['Total Population'])].copy()
    
    # drop unnecessary columns
    cols_2_drop = ['Number of Veterans', 'Foreign-born', 'Race', 'Count']
    df_demographics.drop(cols_2_drop, inplace = True, axis = 1)
    # aggregate data by city, state, state code
    df_demog_agg = df_demographics.groupby(['City', 'State', 'State Code']).agg({'Median Age':'mean', 
                                                                                 'Male Population':'sum', 
                                                                                 'Female Population':'sum',
                                                                                 'Average Household Size':'mean'}).reset_index().copy()
    # make multiple important calculations
    df_demog_agg['popul_total'] = df_demog_agg['Male Population'] + df_demog_agg['Female Population']
    df_demog_agg['share_female'] = df_demog_agg['Female Population'] / df_demog_agg['popul_total']
    # define a matcher column for uszips dataframe
    df_demog_agg['matcher'] = df_demog_agg['City'] + '/' + df_demog_agg['State']
    # aggregate data from uszips by city, state_id, state_name
    temp_density_coor = df_uszips.groupby(['city', 'state_id', 'state_name']).agg({'density':'mean',
                                                                                   'lat':'mean',
                                                                                   'lng':'mean'}).reset_index().copy()
    # define another matcher column for mapping
    temp_density_coor['matcher'] = temp_density_coor['city'] + '/' + temp_density_coor['state_name']
    # map dataframes
    df_merged = df_demog_agg.merge(temp_density_coor, how='left', on='matcher')
    # drop columns
    cols_2_drop = ['matcher', 'city', 'state_id', 'state_name']
    df_merged.drop(cols_2_drop, inplace = True, axis = 1)
    # round columns share_female, density by 2 places
    df_merged['share_female'] = df_merged['share_female'].round(decimals=2)
    df_merged['density'] = df_merged['density'].round(decimals=2)
    # rename columns
    df_merged.rename(columns={'City':'city_name', 
                              'State':'state_name', 
                              'State Code':'state_code',
                              'Median Age':'age_median',
                              'Male Population':'popul_male',
                              'Female Population':'popul_female',
                              'Average Household Size':'household_size_ave',
                              'density':'popul_density'}, inplace=True)
    return df_merged
    
def remove_extra(df):
    """This method loooks for pre-defined columns in a dataframe and removes them
    
    Params:
        df: dataframe
        
    Returns: cleaned dataframe
    """
    cols_2_remove = ['validres', 'delete_days', 'delete_mexl', 'delete_dup', 'delete_visa', 'delete_recdup']
    df = df.drop(*cols_2_remove)
    return df

udf_to_datetime_sas = udf(lambda x: to_datetime(x), DateType())
def to_datetime(x):
    """This method takes in a SAS coded date and converts it to a normal one
    
    Params:
        x: SAS encoded date
    
    Returns: normal encoded date
    """
    try:
        start = dt.datetime(1960, 1, 1).date()
        return start + dt.timedelta(days=int(x))
    except:
        return None

@timed
def process_immi_data(spark, df_raw, df_visatype, df_country_code, df_airports):
    """This method cleanes immigration datasets
    
    Params:
        spark: spark session
        df_raw: dataset to manipulate
        df_visatype: additional dataset for mapping purposes
        df_country_code: additional dataset for mapping purposes
        df_airports: additional dataset for mapping purposes
    
    Returns: df, cleaned immigration dataset
    """
    print(f'Processing: immigration_data')
    # matflag is null
    df_raw = df_raw.filter(df_raw.matflag.isNotNull())
    # visatype GMT  
    visatype_list = df_visatype.visatype.tolist()
    df_raw = df_raw.filter( df_raw.visatype.isin(visatype_list) )
    # i94mode other than 1 2 3
    # 1: Air, 2: Sea, 3: Land
    travel_mode = [1]
    df_raw = df_raw.filter( df_raw.i94mode.isin(travel_mode) )
    # gender is null
    df_raw = df_raw.filter(df_raw.gender.isNotNull())
    # Remove rows having invalid CoC & CoR
    country_code_list = df_country_code.I94_country_code.astype('int').tolist()
    df_raw = df_raw.filter( df_raw.i94cit.isin(country_code_list) )
    df_raw = df_raw.filter( df_raw.i94res.isin(country_code_list) )
    # filter only US international Airports
    airports_us_list = df_airports.I94_port_code.tolist()
    df_raw = df_raw.filter( df_raw.i94port.isin(airports_us_list) )
    # Conversion of SAS encoded dates(arrdate & depdate)
    df_raw = df_raw.withColumn("dt_arrival", udf_to_datetime_sas(df_raw.arrdate))
    df_raw = df_raw.withColumn("dt_departure", udf_to_datetime_sas(df_raw.depdate))
    # Departure date can't before Arrival date 
    # ~(arrival date > departure date ) or (departure date can be null)
    df_raw = df_raw.filter(~(df_raw.dt_arrival > df_raw.dt_departure) | (df_raw.dt_departure.isNull()))
    # Adding month which is used when saving file in parquet format partioning by month & landing state
    df_raw = df_raw.withColumn("month", month("dt_arrival"))
    # dropping columns
    drop_cols = ['cicid', 'i94yr', 'i94mon', 'i94mode', 'i94visa', 'arrdate', 'depdate', 'count', 'dtadfile', 'entdepa', 'entdepd', 'entdepu', 'matflag', 'dtaddto', 'insnum', 'admnum']
    df_raw = df_raw.drop(*drop_cols)
    # change column type to integer
    cols_2_integer = ['i94cit', 'i94res', 'i94bir', 'biryear']
    for col in cols_2_integer:
        df_raw = df_raw.na.fill(0, subset=[col])
        df_raw = df_raw.withColumn(col, df_raw[col].cast(IntegerType()))
    # Columns Rename
    df_raw = (df_raw
                .withColumnRenamed("i94cit",  "CoC")
                .withColumnRenamed("i94res", "CoR")
                .withColumnRenamed("i94port", "PoE")
                .withColumnRenamed("i94addr", "state_landing")
                .withColumnRenamed("i94bir", "age")
                .withColumnRenamed("biryear", "year_birth")
                .withColumnRenamed("airline", "airline_used")
                .withColumnRenamed("fltno", "num_flight"))
    return df_raw

def main():
    
    ## define path and file names ##
    fname_labels = 'input/I94_SAS_Labels_Descriptions.SAS'
    fname_visatype = 'input/visatype.csv'
    fname_ccodes = 'input/country_codes.csv'
    fname_airports = 'input/airport-codes_csv.csv'
    fname_international = 'input/international_airports_US.csv'
    fname_demographics = 'input/us-cities-demographics.csv'
    fname_uszips = 'input/uszips.csv'
    dir_immi_data = '../../data/18-83510-I94-Data-2016/*'
    
    ## read I94_SAS_Labels_Descriptions file in order to extract labels
    with open(fname_labels) as f:
        data_labels = f.readlines()
    
    ## define lines for each label set ##
    ports = data_labels[302:962]
    cit_codes = data_labels[10:298]
    addr_states = data_labels[982:1036]
    
    ## read files as raw ##
    raw_visatype = pd.read_csv(fname_visatype,delimiter='|')
    raw_ccodes = pd.read_csv(fname_ccodes, converters={"country_code":str,
                                                       "region_code":str})
    raw_airports = pd.read_csv(fname_airports)
    raw_international = pd.read_csv(fname_international)
    raw_demographics = pd.read_csv(fname_demographics,delimiter=";")
    raw_uszips = pd.read_csv(fname_uszips)
    
    ## create copies of raw dataframes ##
    df_visatype = raw_visatype.copy()
    df_ccodes = raw_ccodes.copy()
    df_airports = raw_airports.copy()
    df_international = raw_international.copy()
    df_demographics = raw_demographics.copy()
    df_uszips = raw_uszips.copy()
    
    ## remove whitespace from dataframes ##
    df_visatype = remove_whitespace(df_visatype).copy()
    df_ccodes = remove_whitespace(df_ccodes).copy()
    df_airports = remove_whitespace(df_airports).copy()
    df_international = remove_whitespace(df_international).copy()
    df_demographics = remove_whitespace(df_demographics).copy()
    df_uszips = remove_whitespace(df_uszips).copy()
    
    ## define intermidiary dataframes / lists ##
    ## extract Point of Entry, Country of Citizenship, and US State codes ## 
    I94_ports = get_I94port(ports)
    I94_cit_codes = get_I94cit(cit_codes)
    I94_addr_states = get_I94addr(addr_states)
    
    ## process dataframes ##
    df_visatype = process_visatype(df_visatype)
    assert df_visatype.shape[0] != 0,"df_visatype is empty"
    df_ccodes = process_ccodes(I94_cit_codes, df_ccodes)
    assert df_ccodes.shape[0] != 0,"df_ccodes is empty"
    df_airports = process_airports(I94_ports, df_airports, df_international)
    assert df_airports.shape[0] != 0,"df_airports is empty"
    df_demographics = process_demographics(df_demographics, df_uszips)
    assert df_demographics.shape[0] != 0,"df_demographics is empty"
    
    # define list of immigration files to process
    immi_files = glob.glob(dir_immi_data)
    
    # process immigration data
    flag = True
    for count,immi_file in enumerate(immi_files):
        print(f'--- Processing: {count+1} | {len(immi_files)} ---> {immi_file}')
        raw_file = spark.read.format('com.github.saurfang.sas.spark').load(immi_file)
        assert raw_file.count() != 0,"raw_file is empty" 
        if flag:
            df_processed = process_immi_data(spark, remove_extra(raw_file), df_visatype, df_ccodes, df_airports)
            flag = False
        else:
            df_processed = df_processed.union(process_immi_data(spark, remove_extra(raw_file), df_visatype, df_ccodes, df_airports))
            
    assert df_processed.count() != 0,"df_processed is empty"   
        
    ## write processed dataframes ##
    print(f'started writing processed dataframes')
    directory = './output/'
    os.makedirs(os.path.dirname(directory),exist_ok=True)
    
    df_visatype.to_csv('output/df_visatype.csv', encoding='utf-8',index=False)
    df_ccodes.to_csv('output/df_ccodes.csv', encoding='utf-8',index=False)
    df_airports.to_csv('output/df_airports.csv', encoding='utf-8',index=False)
    df_demographics.to_csv('output/df_demographics.csv', encoding='utf-8',index=False)
    
    # write immigration data to .gz
    df_processed.write\
    .format("com.databricks.spark.csv")\
    .option("header","true")\
    .option("codec", "org.apache.hadoop.io.compress.GzipCodec")\
    .save('./output_gzip/df_immi_processed.gzip')
    
    print(f'finished writing processed dataframes')
    
if __name__ == '__main__':
    main()

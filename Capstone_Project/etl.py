import glob
import psycopg2
import pandas as pd
from sql_queries import *
import os

def process_csv(cur, conn, df_airports, df_ccodes, df_demographics, df_visatype):
    """
    - inserts data into postgres
    """
    for i, row in df_airports.iterrows():
        cur.execute(airports_table_insert, row)
    
    for i, row in df_ccodes.iterrows():
        cur.execute(ccodes_table_insert, row)
        
    for i, row in df_demographics.iterrows():
        cur.execute(demographics_table_insert, row)
        
    for i, row in df_visatype.iterrows():
        cur.execute(visatype_table_insert, row)
    
def process_immi(cur, conn, filepath):
    """
    - 
    """
    files_immi = glob.glob(filepath)
    for file in files_immi:
        print(f'Processing file: {file}')
        temp = pd.read_csv(file, compression='gzip')
        for i, row in temp.iterrows():
            cur.execute(immi_table_insert, row)
            
def quality(cur, conn, tables):
    """This method 
    
    Params:
    
    Returns:
    """
    for table in tables:
        stmt = f'SELECT count(*) FROM {table}'
        cur.execute(stmt)
        
    
def main():
    """
    - establishes connection to postgres
    
    - sends files to ETL
    """
    conn = psycopg2.connect("host= dbname= user= password= port=")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # read data
    raw_airports = pd.read_csv('./output/df_airports.csv')
    raw_ccodes = pd.read_csv('./output/df_ccodes.csv')
    raw_demographics = pd.read_csv('./output/df_demographics.csv')
    raw_visatype = pd.read_csv('./output/df_visatype.csv')
    
    process_csv(cur, conn, raw_airports, raw_ccodes, raw_demographics, raw_visatype)
    process_immi(cur, conn, filepath='./output/df_immi/*')
    quality(cur, conn, tables_list)

    conn.close()

if __name__ == "__main__":
    main()

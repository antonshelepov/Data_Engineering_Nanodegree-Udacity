import configparser
import psycopg2
from sql_queries import copy_table_order, copy_table_queries, insert_table_order, insert_table_queries


def load_staging_tables(cur, conn):
    """
    """
    ITEM = 0
    for query in copy_table_queries:
        print("Copying data into {}...".format(copy_table_order[ITEM]))
        cur.execute(query)
        conn.commit()
        ITEM = ITEM + 1
        print("  [DONE]  ")


def insert_tables(cur, conn):
    """
    """
    ITEM = 0
    for query in insert_table_queries:
        print("Inserting data into {}...".format(insert_table_order[ITEM]))
        cur.execute(query)
        conn.commit()
        ITEM = ITEM + 1
        print("  [DONE]  ")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
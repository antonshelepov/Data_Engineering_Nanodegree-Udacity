import configparser
import psycopg2
from sql_queries import analytics_queries


def run_analytical_queries(cur):
    """This function runs all analytics queries and returns number of row.total
    
    Params: 
        cur:
        
    Returns:
    """
    for query in analytical_queries:
        row = cur.execute(query)
        print(f"For query {query} number of rows returned: {row.total}")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    run_analytical_queries(cur)

    conn.close()


if __name__ == "__main__":
    main()

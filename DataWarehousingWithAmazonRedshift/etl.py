import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Takes in cursor, redshift connection and runs queries available in copy_table_queries list from sql_queries.py"""

    # Loop through and execute each query in copy_table_queries list from sql_queries.py  (loads data into staging tables)
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Takes in cursor, redshift connection and runs queries available in insert_table_queries list from sql_queries.py"""

    # Loop through and execute each query in insert_table_queries list from sql_queries.py  (loads data into analytical tables)
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Reads configuration settings from dwh.cfg file and execute load_staging_tables, insert_tables functions"""
    
    # Parse configuration file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # Create redshift connection and cursor
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Load staging tables
    load_staging_tables(cur, conn)
    
    # Load analytical tables
    insert_tables(cur, conn)
    
    # Close connection
    conn.close()


if __name__ == "__main__":
    main()

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """Takes in cursor, redshift connection and runs queries available in drop_table_queries list from sql_queries.py"""
    
    # Loop through and execute each query in drop_table_queries list from sql_queries.py
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """Takes in cursor, redshift connection and runs queries available in create_table_queries list from sql_queries.py"""
    
    # Loop through and execute each query in create_table_queries list from sql_queries.py
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Reads configuration settings from dwh.cfg file and execute drop_tables, create_tables functions"""
    
    # Parse configuration file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    # Create redshift connection and cursor
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Drop tables
    drop_tables(cur, conn)
    
    # Create tables
    create_tables(cur, conn)
    
    # Close connetion to redshift
    conn.close()


if __name__ == "__main__":
    main()

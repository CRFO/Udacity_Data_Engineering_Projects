import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    
    """
    This function loads the staging tables from JSON files. 
    
    INPUTS: 
    * cur = the cursor variable
    * conn = the connection variable
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    
    """
    This function inserts data into songplays, users, songs, artists and time from the two staging tables:
    - staging_events
    - staging_songs
    
    INPUTS: 
    * cur = the cursor variable
    * conn = the connection variable
    """
        
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    """
    This function parsers all config information from dwh.cjg file storing them into the cursor and connection variables. Then executes load_staging_tables and insert_tables functions. Both functions are explained above.
    """
        
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
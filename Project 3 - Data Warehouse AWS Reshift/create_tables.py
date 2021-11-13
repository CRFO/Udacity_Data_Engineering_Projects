import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    This function drops all tables. 
    
    INPUTS: 
    * cur = the cursor variable
    * conn = the connection variable
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    This function creates all tables. 
    
    INPUTS: 
    * cur = the cursor variable
    * conn = the connection variable
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    This function parsers all config information from dwh.cjg file storing them into the cursor and connection variables. Then executes drop_tables and create_tables functions. Both functions are explained above.
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
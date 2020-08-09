import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """This Function loads songs data and log data that reside in s3 buckets"""
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """This Function  insert into songs , artists, users, time and songplay tables using songs and log stage tables"""
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """This main Function connects to the database and calls other functions to load data into tables"""
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
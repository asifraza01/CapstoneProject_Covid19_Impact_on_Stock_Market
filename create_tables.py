import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
def drop_tables(cur, conn):
    """
    Drop all existing tables.
    Keyword arguments:
    * cur --    cursory to connected DB. Allows to execute SQL commands.
    * conn --   connect to Postgres database.
    Output:
    * Older tables are dropped from AWS Redshift.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
def create_tables(cur, conn):
    """
    Create new tables.
    Keyword arguments:
    * cur --    cursory to connected DB. Allows to execute SQL commands.
    * conn --   connect to Postgres database.
    Output:
    * Create new tables in AWS Redshift.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
def main():
    """
    Connect to AWS Redshift, create new DB,
    drop and create new tables. Close DB connection.
    Keyword arguments (from dwh.cfg):
    * host --       AWS Redshift cluster address.
    * dbname --     DB name.
    * user --       Username for the DB reshift.
    * password --   Password for the DB redshift.
    * port --       DB port to connect to reshift.
    * cur --        cursory to connected DB. Allows to execute SQL commands.
    * conn --       (psycopg2) connection to Postgres database (sparkifydb).
    Output:
    * new db and tables are created, old tables are droppped,
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
import configparser
import psycopg2
from sql_queries import queries_to_be_executed


def display_res(cur, conn):
    for query in queries_to_be_executed:
        print('\n'.join(('', 'Running:', query)))
        cur.execute(query)
        results = cur.fetchone()
        for row in results:
            print(row)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print('Connected to AWS Redshift')
    cur = conn.cursor()

    display_res(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()

import os
import connection
import sqlparse
import pandas as pd

if __name__ == '__main__':

    # Connection to the database
    conf = connection.config('marketplace_prod')
    conn, engine = connection.get_conn(conf, 'DataSource')
    cursor = conn.cursor()

    # Connection dwh
    conf_dwh = connection.config('dwh')
    conn_dwh, engine_dwh = connection.get_conn(conf_dwh, 'DataWarehouse')
    cursor_dwh = conn_dwh.cursor()

    # get query string
    path_query = os.getcwd() + '/query/'
    query = sqlparse.format(
        open(path_query + '/query.sql', 'r').read(), strip_comments=True
    ).strip()
    dwh_design = sqlparse.format(
        open(path_query + '/dwh_design.sql', 'r').read(), strip_comments=True
    ).strip()

    # execute query
    try:
        # get data
        print('Executing query...')
        df = pd.read_sql_query(query, engine)
        print('Query executed successfully!')
        # print(df.head())

        # cretae schema dwh
        cursor_dwh.execute(dwh_design)
        conn_dwh.commit()
        print('Schema DWH created successfully!')

        # ingest data to dwh
        df.to_sql('dim_orders',
                  engine_dwh,
                  schema='gustian_dwh',
                  if_exists='append',
                  index=False)
        print('Data ingested to DWH successfully!')

    except Exception as e:
        print('Error executing query:')
        print(str(e))

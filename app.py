import pandas as pd
import sqlalchemy as sa

#Input database object names
src_srvr = input('Enter source server name: ')
tgt_srvr = input('Enter target server name: ')

src_db = input('Enter source database name: ')
tgt_db = input('Enter target database name: ')

src_schema = input('Enter source schema name: ')
tgt_schema = input('Enter target schema name: ')

src_tbl = input('Enter source table name: ')
tgt_tbl = input('Enter target table name: ')



#Build connection strings
src_conn_str = (
    r"Driver={ODBC Driver 17 for SQL Server};"
    f"Server={src_srvr};"
    f"Database={src_db};"
    r"Trusted_Connection=yes;"
    )

tgt_conn_str = (
    r"Driver={ODBC Driver 17 for SQL Server};"
    f"Server={tgt_srvr};"
    f"Database={tgt_db};"
    r"Trusted_Connection=yes;"
    )


#Build connection url SQL Alchemy style
src_conn_url = sa.engine.URL.create("mssql+pyodbc", query={"odbc_connect": src_conn_str})
tgt_conn_url = sa.engine.URL.create("mssql+pyodbc", query={"odbc_connect": tgt_conn_str})

#Build engines
src_engine = sa.create_engine(src_conn_url)
tgt_engine = sa.create_engine(tgt_conn_url)

#Build sql queries
src_qry = f"SELECT * FROM {src_schema}.{src_tbl}"
tgt_qry = f"SELECT * FROM {tgt_schema}.{tgt_tbl}"

src_pk_qry = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = '{src_schema}' AND TABLE_NAME = '{src_tbl}'"
tgt_pk_qry = f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA = '{tgt_schema}' AND TABLE_NAME = '{tgt_tbl}'"

#Retrieve data
with src_engine.connect() as conn:
    src_df = pd.read_sql(src_qry, conn)
    src_pk = pd.read_sql(src_pk_qry, conn).values[0][0]

with tgt_engine.connect() as conn:
    tgt_df = pd.read_sql(tgt_qry, conn)    
    tgt_pk = pd.read_sql(tgt_pk_qry, conn).values[0][0]

#Compare!
if src_pk != tgt_pk:
    print("Couldn't match primary keys!")
else:
    diff = src_df.compare(tgt_df, keep_shape=True, keep_equal=True, result_names=('src', 'tgt'))
    print(diff)

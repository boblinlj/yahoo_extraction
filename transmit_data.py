from sqlalchemy import event
from sqlalchemy import create_engine
from configs import database_configs_prod
from configs import database_configs_nas
import pandas as pd


cnn_pod = create_engine(
                        f"""mysql+mysqlconnector://{database_configs_prod.MYSQL_USER}"""
                        f""":{database_configs_prod.MYSQL_PASSWORD}"""
                        f"""@{database_configs_prod.MYSQL_HOST}"""
                        f""":{database_configs_prod.MYSQL_PORT}"""
                        f"""/{database_configs_prod.MYSQL_DATABASE}""",
                        pool_size=20,
                        max_overflow=0
                        )

cnn_nas = create_engine(
                        f"""mysql+mysqlconnector://{database_configs_nas.MYSQL_USER}"""
                        f""":{database_configs_nas.MYSQL_PASSWORD}"""
                        f"""@{database_configs_nas.MYSQL_HOST}"""
                        f""":{database_configs_nas.MYSQL_PORT}"""
                        f"""/{database_configs_nas.MYSQL_DATABASE}""",
                        pool_size=20,
                        max_overflow=0
                        )
@event.listens_for(cnn_nas, "before_cursor_execute")
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    if executemany:
        cursor.fast_executemany = True


table = 'nasdaq_universe'

sql = """ select * from {}"""

print(f"Reading data from {table}...")

df = pd.read_sql(con=cnn_pod, sql=sql.format(table), index_col='data_id')

print(f"Data Received, {df.shape[0]}...")

df.to_sql(name=table,
          con=cnn_nas,
          index=False,
          if_exists="append",
          schema="financial",
          index_label='data_id',
          chunksize=1000)
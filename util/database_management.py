from configs import database_configs_nas as dbcfg
from configs import database_configs_prod as prod_dbcfg
from sqlalchemy import create_engine
import pandas as pd


class DatabaseManagementError(Exception):
    pass


class DatabaseManagement:

    def __init__(self, data_df=None, table=None, key=None, where=None, date=None, sql=None, insert_index=False, use_prod=False):
        self.data_df = data_df
        self.table = table
        self.key = key
        self.where = where
        self.date = date
        self.sql = sql
        self.insert_index = insert_index
        self.use_prod = use_prod

        if self.use_prod:
            self.database_ip = prod_dbcfg.MYSQL_HOST
            self.database_user = prod_dbcfg.MYSQL_USER
            self.database_pw = prod_dbcfg.MYSQL_PASSWORD
            self.database_port = prod_dbcfg.MYSQL_PORT
            self.database_nm = prod_dbcfg.MYSQL_DATABASE
        else:
            self.database_ip = dbcfg.MYSQL_HOST
            self.database_user = dbcfg.MYSQL_USER
            self.database_pw = dbcfg.MYSQL_PASSWORD
            self.database_port = dbcfg.MYSQL_PORT
            self.database_nm = dbcfg.MYSQL_DATABASE

        try:
            self.cnn = create_engine(
                                    f"""mysql+mysqlconnector://{self.database_user}"""
                                    f""":{self.database_pw}"""
                                    f"""@{self.database_ip}"""
                                    f""":{self.database_port}"""
                                    f"""/{self.database_nm}""",
                                    pool_size=20,
                                    max_overflow=0)
        except Exception as e:
            raise DatabaseManagementError(f'database cannot be created, {e}')     

    def insert_db(self) -> None:
        """
        Insert Pandas DataFrame to a table, requires the following parameters:
            data_df: Pandas DataFrame
            table(str): Target table to insert the DataFrame
        :return: None
        """
        try:
            if self.data_df is None or self.data_df.empty:
                raise DatabaseManagementError(f"dataframe is empty, therefore cannot be inserted")
            elif self.table is None:
                raise DatabaseManagementError(f"table to be inserted is empty, therefore cannot be inserted")
            else:
                self.data_df.to_sql(name=self.table,
                                    con=self.cnn,
                                    if_exists='append',
                                    index=self.insert_index,
                                    method='multi',
                                    chunksize=1)
        except Exception as e:
            raise DatabaseManagementError(f"data insert to {self.table} failed as {e}")

    def read_sql_to_df(self) -> pd.DataFrame:
        """
        Read SQL into a Pandas DataFrame, requires the following parameters:
            sql(str): sql statement

        :return: Pandas DataFrame
        """
        if self.sql is  None:
            raise DatabaseManagementError(f'data extraction from database failed due to sql is none')
        else:
            try:
                df = pd.read_sql(con=self.cnn, sql=self.sql)
                return df
            except Exception as e:
                raise DatabaseManagementError(f"data extractions from database failed for sql={self.sql} as {e}")

    def _construct_sql(self) -> str:
        """
        Construct SQL statement with DISTINCT, requires the following parameters:
            key(str): the elements of select statement
            table(str): the table
            where(str): the filters

        This is an internal function that are used in
        - check_population
        - get_record


        :return: SQL statement in a string
        """
        return f"""
                    SELECT DISTINCT {self.key}
                    FROM {self.table}
                    WHERE {self.where}
                """

    def check_population(self) -> list:
        """
        Create a list of distinct key from a SQL, requires the following parameters:
            key(str): the key of select statement
            table(str): the table
            where(str): the filters

        :return: List of unique key elements
        """
        if all(var is None for var in [self.key, self.table, self.where]):
            raise DatabaseManagementError(
                f'Cannot check population, due to critical variable missing (key, table, where)')
        if len(self.key.split(',')) > 1:
            raise DatabaseManagementError(
                f'Cannot check population, only one key can be passed in')
        else:
            return pd.read_sql(con=self.cnn, sql=self._construct_sql())[self.key].to_list()

    def get_record(self) -> pd.DataFrame:
        """
        Extract table based on the SQL construction function, requires the following parameters:
            key(str): the elements of select statement
            table(str): the table
            where(str): the filters

        :return: Pandas DataFrame
        """
        if all(var is None for var in [self.key, self.table, self.where]):
            raise DatabaseManagementError(
                f'cannot run sql, due to critical variable missing (key, table, where)')
        else:
            return pd.read_sql(con=self.cnn, sql=self._construct_sql())

    def table_update_summary(self) -> pd.DataFrame:
        """
        Check the table summary grouping by updated_dt, requires the following parameters:
            table(str): the table

        :return: Pandas DataFrame
        """
        sql = f"""
               SELECT "{self.table}" as table_name, updated_dt, count(1) as total_updated
               FROM  {self.table}
               GROUP BY 1,2
               ORDER BY 2 DESC
               LIMIT 1
            """
        if self.table is None:
            raise DatabaseManagementError(
                f"Cannot run sql, due to the table name is missing"
            )
        else:
            return pd.read_sql(con=self.cnn, sql=sql)

    def upload_csv_to_table(self) -> None:
        pass


if __name__ == '__main__':
    obj = DatabaseManagement(table='yahoo_quarterly_fundamental')
    print(obj.table_update_summary())

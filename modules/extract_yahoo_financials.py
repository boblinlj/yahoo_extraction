import time
import numpy as np
import pandas as pd
import datetime
from util.helper_functions import create_log
from util.parallel_process import parallel_process
from util.request_website import YahooAPIParser, WebParseError
from util.database_management import DatabaseManagement, DatabaseManagementError
from util.get_stock_population import SetPopulation
from configs.job_configs import WORKER
from configs import database_configs_prod as prod_dbcfg
from sqlalchemy import create_engine

pd.set_option('display.max_columns', None)

class ExtractionError(Exception):
    pass


class ReadYahooFinancialData:

    def __init__(self, dict_data, stock, logger):
        self.dict_data = dict_data
        self.stock = stock
        self.logger = logger

    def parse(self):
        results = self.dict_data['timeseries']['result']
        try:
            annual_dataframes = []
            quarter_dataframes = []
            ttm_dataframes = []

            for item in results[:]:
                data_type = item['meta']['type'][0]
                try:
                    df = pd.DataFrame.from_records(item[data_type])
                    df["reportedValue"] = df["reportedValue"].apply(
                        lambda x: x.get("raw") if isinstance(x, dict) else x
                    )
                    df["asOfDate"] = pd.to_datetime(df["asOfDate"], format="%Y-%m-%d")
                    df.drop(columns=['dataId', 'currencyCode'], axis=0, inplace=True)
                    df['type'] = data_type

                    if 'annual' in data_type:
                        annual_dataframes.append(df)
                    elif 'quarterly' in data_type:
                        quarter_dataframes.append(df)
                    else:
                        ttm_dataframes.append(df)
                except KeyError:
                    pass

            annual_concat = pd.concat(annual_dataframes, sort=False)
            del annual_dataframes
            quarter_concat = pd.concat(quarter_dataframes, sort=False)
            del quarter_dataframes
            ttm_concat = pd.concat(ttm_dataframes, sort=False)
            del ttm_dataframes

            ttm_concat = ttm_concat.pivot_table(
                index=['asOfDate', 'periodType'],
                columns="type",
                values="reportedValue",
            )

            annual_concat = annual_concat.pivot_table(
                index=['asOfDate', 'periodType'],
                columns="type",
                values="reportedValue",
            )
            quarter_concat = quarter_concat.pivot_table(
                index=['asOfDate', 'periodType'],
                columns="type",
                values="reportedValue",
            )

            annual_df = pd.DataFrame(annual_concat.to_records())

            quarter_df = pd.DataFrame(quarter_concat.to_records())

            ttm_df = pd.DataFrame(ttm_concat.to_records())

            return annual_df, quarter_df, ttm_df
        except Exception as e:
            raise ExtractionError(f"Unable to parse Json API from Yahoo, error: {e}")


class YahooFinancial:
    workers = WORKER
    BASE_URL = 'https://query1.finance.yahoo.com'
    df_for_elements = DatabaseManagement(sql="""SELECT type, freq, data
                                            FROM `yahoo_financial_statement_data_control`""").read_sql_to_df()
    all_elements = df_for_elements.data.values.tolist()
    table_lookup = {'yahoo_quarterly_fundamental': 'quarter',
                    'yahoo_annual_fundamental': 'annual',
                    'yahoo_trailing_fundamental': 'ttm'}
    failed_extract = []

    def __init__(self, updated_dt, targeted_pop, batch=False, loggerFileName=None, use_tqdm=True, test_size=None):
        self.updated_dt = updated_dt
        self.targeted_population = targeted_pop
        self.loggerFileName = loggerFileName
        self.batch = batch
        self.logger = create_log(loggerName='YahooFinancialStatements', loggerFileName=self.loggerFileName)
        self.use_tqdm = use_tqdm
        self.test_size = test_size
        # object variables for reporting purposes
        self.no_of_stock = 0
        self.time_decay = 0
        self.no_of_web_calls = 0
        self.no_of_db_entries = 0

    def _existing_dt(self) -> None:
        annual_data = DatabaseManagement(table='yahoo_annual_fundamental',
                                         key="ticker, asOfDate, 'annual' as type",
                                         where="1=1",
                                         use_prod=True).get_record()

        quarter_data = DatabaseManagement(table='yahoo_quarterly_fundamental',
                                          key="ticker, asOfDate, 'quarter' as type",
                                          where="1=1",
                                          use_prod=True).get_record()

        ttm_data = DatabaseManagement(table='yahoo_trailing_fundamental',
                                      key="ticker, asOfDate, 'ttm' as type",
                                      where="1=1",
                                      use_prod=True).get_record()

        self.ext_list_data = pd.concat([annual_data, quarter_data, ttm_data], axis=0)
        self.ext_list_data['asOfDate'] = pd.to_datetime(self.ext_list_data['asOfDate'])

    def _url_builder_fundamentals(self) -> str:
        tdk = str(int(time.mktime(datetime.datetime.now().timetuple())))
        yahoo_fundamental_url = '/ws/fundamentals-timeseries/v1/finance/timeseries/{stock}?symbol={stock}&type='
        yahoo_fundamental_url_tail = '&merge=false&period1=493590046&period2=' + tdk
        elements = '%2C'.join(self.all_elements)

        return self.BASE_URL + yahoo_fundamental_url + elements + yahoo_fundamental_url_tail

    def _extract_api(self, stock) -> dict:
        url = self._url_builder_fundamentals().format(stock=stock)

        data = None

        try:
            apiparse = YahooAPIParser(url=url)
            data = apiparse.parse()
            self.no_of_web_calls = self.no_of_web_calls + apiparse.no_requests
        except WebParseError:
            self.logger.debug(f"Error: {stock} Unable to open {url}")

        return data

    def _extract_each_stock(self, stock) -> None:
        self.logger.info(f"Processing {stock} for fundamental data")
        start = time.time()
        js = self._extract_api(stock)
        if js is None:
            self.failed_extract.append(stock)
            return None

        # extract data from YAHOO JSON
        try:
            df_12m, df_3m, df_ttm = ReadYahooFinancialData(js, stock, self.logger).parse()

            self._insert_to_db(df_12m, stock, 'yahoo_annual_fundamental')
            self._insert_to_db(df_3m, stock, 'yahoo_quarterly_fundamental')
            self._insert_to_db(df_ttm, stock, 'yahoo_trailing_fundamental')

            end = time.time()
            self.logger.info(f'Finish data parse, took {round(end - start, 2)} seconds, stock={stock}')
        except ExtractionError:
            self.logger.debug(f"Failed to parse JSON for stock {stock}")
            self.failed_extract.append(stock)

    def _insert_to_db(self, df, stock, table) -> None:
        df_to_insert = df.copy()
        df_to_insert.drop(columns=['periodType'], inplace=True)
        df_to_insert['updated_dt'] = self.updated_dt
        df_to_insert['updated_dt'] = df_to_insert['updated_dt'].astype('str')
        df_to_insert['asOfDate'] = df_to_insert['asOfDate'].astype('str')
        df_to_insert['ticker'] = stock
        df_to_insert = df_to_insert.replace(np.NaN, None)

        database_ip = prod_dbcfg.MYSQL_HOST
        database_user = prod_dbcfg.MYSQL_USER
        database_pw = prod_dbcfg.MYSQL_PASSWORD
        database_port = prod_dbcfg.MYSQL_PORT
        database_nm = prod_dbcfg.MYSQL_DATABASE

        cnn = create_engine(
            f"""mysql+mysqlconnector://{database_user}"""
            f""":{database_pw}"""
            f"""@{database_ip}"""
            f""":{database_port}"""
            f"""/{database_nm}""",
            pool_size=20,
            max_overflow=0)
        connection = cnn.raw_connection()
        cursor = connection.cursor()

        try:
            # df_to_insert.to_csv(f'appl_{table}.csv')
            cols = "`, `".join([str(i) for i in df_to_insert.columns.tolist()])
            for i, row in df_to_insert.iterrows():
                sql = "INSERT INTO " + table + "(`" + cols + "`) VALUES(" + " %s,"*(len(row) - 1) + " %s)"
                # print(row)
                try:
                    cursor.execute(sql, tuple(row))
                except Exception as e:
                    print(table , e)
            # DatabaseManagement(data_df=df_to_insert, table=table, insert_index=True, use_prod=True).insert_db()
            self.logger.info(f"{stock} data entered in {table}")
            self.no_of_db_entries += 1
        except DatabaseManagementError as e:
            self.logger.debug(f"Failed to insert data for stock={stock} as {e}")

    def run(self) -> None:
        start = time.time()

        stocks = SetPopulation(user_pop=self.targeted_population, table='yahoo_fundamental').setPop()
        if self.test_size is not None:
            if self.test_size >= 0:
                stocks = stocks[: self.test_size]
            else:
                stocks = stocks[self.test_size:]

        self.no_of_stock = len(stocks)

        for i in range(3):
            self.logger.info(f"{'-' * 20}Start Extraction{'-' * 20}")
            if self.batch:
                parallel_process(stocks, self._extract_each_stock, n_jobs=self.workers, use_tqdm=self.use_tqdm)
            else:
                parallel_process(stocks, self._extract_each_stock, n_jobs=1)

            stocks = self.failed_extract

            if i < 2:
                self.failed_extract = []
            self.logger.info(f"{'-' * 20}Extract Ends{'-' * 20}")

        end = time.time()

        self.time_decay = end - start

    @property
    def get_failed_extracts(self):
        return len(self.failed_extract)


if __name__ == '__main__':
    spider = YahooFinancial(datetime.datetime.today().date() - datetime.timedelta(days=0),
                            targeted_pop='PREVIOUS_POP',
                            batch=True,
                            use_tqdm=False,
                            loggerFileName=None)
    spider._extract_each_stock('AAPL')
    # spider.run()

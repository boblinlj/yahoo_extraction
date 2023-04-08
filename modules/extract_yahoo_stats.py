from configs import yahoo_configs as ycfg
from datetime import date
import json
import pandas as pd
import numpy as np
import time
import random
from util.get_stock_population import SetPopulation
from util.helper_functions import create_log
from util.helper_functions import unix_to_regular_time
from util.helper_functions import dedup_list
from util.helper_functions import returnNotMatches
from util.parallel_process import *
from util.request_website import YahooAPIParser
from util.database_management import DatabaseManagement, DatabaseManagementError

YAHOO_MODULE = ['defaultKeyStatistics', 'financialData', 'summaryDetail', 'price']


def ReadYahooStatsData(data) -> pd.DataFrame:
    dataframes = []
    for table in YAHOO_MODULE:
        # defaultKeyStatistics
        json_data = json.dumps(data['quoteSummary']['result'][0][table])
        df = pd.read_json(json_data)
        df = df.iloc[:1]
        # print(df.to_csv(f"{table}.csv"))
        df.drop(eval(f'ycfg.drop_{table}'), axis=1, inplace=True)
        dataframes.append(df)

    # combine separate datasets
    final_df = pd.concat(dataframes, axis=1)
    final_df.rename(columns={"symbol": "ticker"}, inplace=True)
    for column_name in ycfg.YAHOO_STATS_COLUMNS:
        if column_name not in final_df.columns:
            final_df[column_name] = np.nan

    final_df = final_df[ycfg.YAHOO_STATS_COLUMNS]
    final_df.replace(to_replace='Infinity', value=np.nan, inplace=True)
    final_df.to_csv('final.csv')
    try:
        final_df['sharesShortPreviousMonthDate'] = final_df['sharesShortPreviousMonthDate'].apply(
            unix_to_regular_time)
    except ValueError:
        final_df['sharesShortPreviousMonthDate'] = np.nan

    final_df['lastFiscalYearEnd'] = final_df['lastFiscalYearEnd'].apply(unix_to_regular_time)
    final_df['nextFiscalYearEnd'] = final_df['nextFiscalYearEnd'].apply(unix_to_regular_time)
    final_df['mostRecentQuarter'] = final_df['mostRecentQuarter'].apply(unix_to_regular_time)
    final_df['lastDividendDate'] = final_df['lastDividendDate'].apply(unix_to_regular_time)
    final_df['exDividendDate'] = final_df['exDividendDate'].apply(unix_to_regular_time)
    final_df['lastSplitDate'] = final_df['lastSplitDate'].apply(unix_to_regular_time)

    final_df.rename(columns={"symbol": "ticker"}, inplace=True)
    final_df.reset_index(drop=True, inplace=True)

    return final_df


class YahooStats:
    BASE_URL = 'https://query{}.finance.yahoo.com/v10/finance'
    workers = jcfg.WORKER
    failed_extract = []

    def __init__(self, updated_dt: date, targeted_pop: str, batch=False, loggerFileName=None, use_tqdm=True, test_size=None):
        self.loggerFileName = loggerFileName
        self.updated_dt = updated_dt
        self.targeted_pop = targeted_pop
        self.batch = batch
        self.logger = create_log(loggerName='YahooStats', loggerFileName=self.loggerFileName)
        self.use_tqdm = use_tqdm
        self.test_size = test_size
        # object variables for reporting purposes
        self.no_of_stock = 0
        self.time_decay = 0
        self.no_of_web_calls = 0

    def _create_url(self, stock) -> str:
        url = self.BASE_URL.format(random.choice([1, 2]))
        url = url + f'/quoteSummary/{stock}?modules='+'%2C'.join(YAHOO_MODULE)
        url = url

        return url

    def _get_stock_statistics(self, stock) -> pd.DataFrame:
        try:
            # parse the Yahoo API, it returns a JSON and a class variable of number of website calls
            apiparse = YahooAPIParser(url=self._create_url(stock))
            data = apiparse.parse()
            self.no_of_web_calls = self.no_of_web_calls + apiparse.no_requests
            out_df = ReadYahooStatsData(data)
            out_df['updated_dt'] = self.updated_dt
            return out_df

        except Exception as e:
            self.logger.error(f"Fail to extract stock = {stock}, error: {e}")
            return pd.DataFrame()

    def _extract_each_stock(self, stock) -> None:
        data_df = self._get_stock_statistics(stock)

        if data_df.empty:
            self.logger.debug(f'Fail to find {stock} data after {5} trails')
            self.failed_extract.append(stock)
        # enter yahoo fundamental table
        try:
            DatabaseManagement(data_df=data_df, table='yahoo_fundamental', insert_index=False).insert_db()
            self.logger.info(f"yahoo_fundamental: Yahoo statistics data entered successfully for stock = {stock}")
        except (DatabaseManagementError, KeyError) as e:
            self.logger.error(f"yahoo_fundamental: Yahoo statistics data entered failed for stock = {stock}, {e}")
            self.failed_extract.append(stock)

        return None

    def _existing_stock_list(self) -> list:
        return DatabaseManagement(table='yahoo_fundamental',
                                  key='ticker',
                                  where=f"updated_dt = '{self.updated_dt}'").check_population()

    def _extract_population(self, table) -> list:
        stocks = SetPopulation(self.targeted_pop, table=table).setPop()
        stocks = dedup_list(stocks)
        stocks = returnNotMatches(stocks, self._existing_stock_list() + jcfg.BLOCK)[:]
        return stocks

    def run(self) -> None:
        start = time.time()

        stocks = self._extract_population('yahoo_fundamental')

        if self.test_size is not None:
            stocks = stocks[:self.test_size]

        self.no_of_stock = len(stocks)

        for _ in range(3):
            self.logger.info(f"{'-'*20}Start Extraction{'-'*20}")
            if self.batch:
                parallel_process(stocks, self._extract_each_stock, n_jobs=self.workers, use_tqdm=self.use_tqdm)
            else:
                parallel_process(stocks, self._extract_each_stock, n_jobs=1)
            stocks = dedup_list(self.failed_extract)
            self.failed_extract = []
            self.logger.info(f"{'-'*20}Extract Ends{'-'*20}")

        end = time.time()
        self.time_decay = round((end - start) / 60)

    @property
    def get_failed_extracts(self):
        return len(self.failed_extract)


if __name__ == '__main__':
    spider = YahooStats(date(9999, 12, 31),
                        targeted_pop='PREVIOUS_POP',
                        batch=False,
                        loggerFileName=None,
                        use_tqdm=False)
    print(spider._get_stock_statistics('NUWE'))

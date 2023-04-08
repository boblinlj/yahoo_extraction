import pandas as pd
import datetime
import numpy as np
from util.helper_functions import timer_func
from util.helper_functions import create_log
from util.request_website import NasdaqAPIParser, WebParseError
from util.database_management import DatabaseManagement, DatabaseManagementError


class NasdaqUniverse:
    LIMIT = 10_000
    BASE_URL = 'https://api.nasdaq.com/api/screener/stocks?letter=0&limit={limit}&download=true'

    def __init__(self, updated_dt, loggerFileName=None):
        self.updated_dt = updated_dt
        self.loggerFileName = loggerFileName
        self.logger = create_log(loggerName='NasdaqUniverseExtraction', loggerFileName=self.loggerFileName)
        self.no_of_stock = 0
        self.no_of_web_calls = 0

    @timer_func
    def _call_api(self):
        url = self.BASE_URL.format(limit=self.LIMIT)
        try:
            response = NasdaqAPIParser(url=url).parse()
            return response
        except WebParseError as e:
            self.logger.error(e)

    @timer_func
    def _parse_json(self, js):
        try:
            data = js['data']['rows']
            if data is None:
                return pd.DataFrame()
            else:
                df = pd.DataFrame.from_records(data)
                df.drop(columns=['lastsale', 'volume', 'netchange', 'pctchange', 'marketCap', 'url'], inplace=True)
                df.rename(columns={'symbol': 'ticker'}, inplace=True)
                df = df.replace(to_replace='', value=np.nan)
                df['yahoo_ticker'] = df['ticker']
                df['yahoo_ticker'] = df['yahoo_ticker'].str.replace('^', '-P', regex=True)
                df['yahoo_ticker'] = df['yahoo_ticker'].str.replace('/', '-', regex=True)
                df['updated_dt'] = self.updated_dt
                return df
        except KeyError as e:
            self.logger.error(e)

    @timer_func
    def run(self):
        df = self._parse_json(self._call_api())
        try:
            DatabaseManagement(data_df=df, table='nasdaq_universe', insert_index=False).insert_db()
        except DatabaseManagementError as e:
            self.logger.error(e)

    @property
    def get_failed_extracts(self):
        return 0


if __name__ == '__main__':
    updated_dt = datetime.datetime.today().date() - datetime.timedelta(days=0)
    obj = NasdaqUniverse(updated_dt)
    obj.run()








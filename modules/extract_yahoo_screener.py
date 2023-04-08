from util.request_website import YahooWebParser
from util.helper_functions import create_log
from util.parallel_process import parallel_process
from util.database_management import DatabaseManagement
import pandas as pd
from datetime import date
from pandas_datareader import nasdaq_trader


class ExtractScreenerError(Exception):
    pass


class ExtractScreener:
    keep_col = ['symbol', 'shortName', 'longName', 'quoteType', 'currency']
    base_url = "https://finance.yahoo.com/screener/unsaved/{token}?count=100&offset={offset}"

    def __init__(self, yahoo_screener_token, updated_dt, proxy=True, loggerFileName=None):
        self.token = yahoo_screener_token
        self.updated_dt = updated_dt
        self.proxy = proxy
        # output_df is used to receive results
        self.output_df = pd.DataFrame()
        self.loggerFileName = loggerFileName
        self.logger = create_log(loggerName=f'ExtractScreener-{self.token}', loggerFileName=self.loggerFileName)

    def create_url(self, offset):
        return self.base_url.format(token=self.token, offset=offset)

    def parse_results_from_each_page(self, offset):
        url = self.create_url(offset)
        self.logger.info(f'Processing {url}')
        js = YahooWebParser(url=url, proxy=self.proxy).parse()
        print(js)
        try:
            rows = js['context']['dispatcher']['stores']['ScreenerResultsStore']['results']['rows']
            total = js['context']['dispatcher']['stores']['ScreenerResultsStore']['results']['total']
            if len(rows) > 0:
                df = pd.DataFrame.from_records(rows)[self.keep_col]
                df['offset'] = offset
                if df.empty:
                    return total, False
                else:
                    self.output_df = self.output_df.append(df, ignore_index=True)
                return total, df
            else:
                return total, pd.DataFrame(columns=[self.keep_col])
        except Exception:
            raise ExtractScreenerError(f"Failed to extract data from {url}")

    def loop_all_pages(self):
        offset = 0
        while True:
            if self.parse_results_from_each_page(offset)[1]:
                break
            offset += 100

        self.logger.info(f'Extraction successful - {self.output_df.shape[0]} records extracted')

    def loop_all_pages_concurrently(self):
        total, temp_df = self.parse_results_from_each_page(0)
        start = 0
        end = (int(total / 100) + 1) * 100
        offsets = list(range(start, end, 100))[1:]
        parallel_process(offsets, self.parse_results_from_each_page, 5, use_tqdm=True)

    def parse(self):
        self.loop_all_pages_concurrently()
        self.output_df.to_csv(f"{self.token}.csv")

    def insert_to_db(self, token):
        self.output_df = pd.read_csv(f'{token}.csv')
        self.output_df.rename(columns={'symbol': 'ticker'}, inplace=True)
        self.output_df['updated_dt'] = self.updated_dt
        DatabaseManagement(data_df=self.output_df,
                           table='yahoo_universe',
                           insert_index=False).insert_db()


if __name__ == "__main__":
    # token = '57ccea24-687c-4dc0-baa2-097d555499f1'
    # token = '3e3e5bbe-722e-489c-b1d9-d540eeb5d72d'
    # token = '894b3aeb-4ce5-4847-80c8-594dbf774469'
    token = '6f47fc7f-ef42-479d-bedb-5fbd3e489239'
    today = date.today()
    logfile_name = f"{token}_{today}.log"
    print(logfile_name)
    obj = ExtractScreener(token, updated_dt=today, loggerFileName=None)
    obj.parse()

    # from pandas_datareader.nasdaq_trader import get_nasdaq_symbols
    #
    # symbols = get_nasdaq_symbols()
    # symbols.to_csv('nas.csv')

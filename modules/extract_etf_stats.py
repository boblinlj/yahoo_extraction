import time
from configs import job_configs as jcfg
import pandas as pd
from util.helper_functions import create_log,unix_to_regular_time, dedup_list, returnNotMatches
from util.get_stock_population import SetPopulation
from util.parallel_process import parallel_process
from util.request_website import YahooAPIParser
from util.database_management import DatabaseManagement, DatabaseManagementError


class YahooETFExtractionError(Exception):
    pass


class ReadYahooETFStatData:
    parse_items = [
        'trailingReturns',
        'trailingReturnsCat',
        'annualTotalReturns',
        'riskOverviewStatistics',
        'riskOverviewStatisticsCat'
    ]

    def __init__(self, js):
        self.js = js

    def preliminary_check(self) -> bool:
        """
        Check if the JSON dictionary is usable. Run this before extractions
        :return: Boolean
        """
        try:
            var1 = self.js['quoteSummary']['result'][0]['fundPerformance']
            return True
        except KeyError:
            return False

    def annualTotalReturns(self) -> pd.DataFrame:
        """
        Extract annual total return for an ETF
        :return: DataFrame
        """
        try:
            etf_r = self.js['quoteSummary']['result'][0]['fundPerformance']['annualTotalReturns']['returns']
            bk_r = self.js['quoteSummary']['result'][0]['fundPerformance']['annualTotalReturns']['returnsCat']

            etf_r_df = pd.DataFrame.from_records(etf_r, index='year')
            etf_r_df['etfAnnualReturn'] = etf_r_df['annualValue'].apply(lambda x: x.get('raw'))
            etf_r_df.drop(columns=['annualValue'], inplace=True)

            bk_r_df = pd.DataFrame.from_records(bk_r, index='year')
            bk_r_df['benchmarkAnnualReturn'] = bk_r_df['annualValue'].apply(lambda x: x.get('raw'))
            bk_r_df.drop(columns=['annualValue'], inplace=True)

            final_df = etf_r_df.join(bk_r_df, how='left')
            final_df.reset_index(inplace=True)

            return final_df

        except KeyError as e:
            raise YahooETFExtractionError(f"Key({e}) does not exist")

    def trailingReturns(self) -> pd.DataFrame:
        """
        Extract trailing returns for an ETF
        :return: DataFrame
        """
        try:
            etf_ttm_r = self.js['quoteSummary']['result'][0]['fundPerformance']['trailingReturns']
            etf_ttm_r_df = pd.DataFrame.from_records(etf_ttm_r).loc['raw']
            etf_ttm_r_df['asOfDate'] = unix_to_regular_time(etf_ttm_r_df['asOfDate'])
            return etf_ttm_r_df.to_frame().transpose()

        except KeyError as e:
            raise YahooETFExtractionError(f"Key({e}) does not exist")

    def riskOverviewStatistics(self) -> pd.DataFrame:
        """
        Extract risk overview
        :return: DataFrame
        """
        try:
            etf_risk = self.js['quoteSummary']['result'][0]['fundPerformance']['riskOverviewStatistics']
            etf_risk_df = pd.DataFrame.from_records(etf_risk.get('riskStatistics'))
            for col in etf_risk_df.columns:
                if col != 'year':
                    etf_risk_df[col] = etf_risk_df[col].apply(lambda x: x.get('raw'))

            df_5y = etf_risk_df.loc[etf_risk_df['year'] == '5y'][:]
            df_5y.columns = [x + '_5y' for x in df_5y.columns]
            df_5y.reset_index(inplace=True)
            df_5y.drop(columns=['year_5y', 'index'], inplace=True)

            df_3y = etf_risk_df.loc[etf_risk_df['year'] == '3y'][:]
            df_3y.columns = [x + '_3y' for x in df_3y.columns]
            df_3y.reset_index(inplace=True)
            df_3y.drop(columns=['year_3y', 'index'], inplace=True)

            df_10y = etf_risk_df.loc[etf_risk_df['year'] == '10y'][:]
            df_10y.columns = [x + '_10y' for x in df_10y.columns]
            df_10y.reset_index(inplace=True)
            df_10y.drop(columns=['year_10y', 'index'], inplace=True)

            output_df = pd.concat([df_3y, df_5y, df_10y], axis=1, ignore_index=False)

            return output_df

        except KeyError as e:
            raise YahooETFExtractionError(f"Key({e}) does not exist")

    def topHoldings(self) -> pd.DataFrame:
        """
        Extract top holding of an ETF
        :return: DataFrame
        """
        try:
            etf_holdings = self.js['quoteSummary']['result'][0]['topHoldings']

            sector = {}
            for i in etf_holdings.get('sectorWeightings'):
                for key, item in i.items():
                    sector['sector_weight_' + key] = item.get('raw')

            bond_rating = {}
            for i in etf_holdings.get('bondRatings'):
                for key, item in i.items():
                    bond_rating['bond_rating_' + key] = item.get('raw')

            holding_stats = {
                'cashPosition': etf_holdings['cashPosition'].get('raw'),
                'stockPosition': etf_holdings['stockPosition'].get('raw'),
                'bondPosition': etf_holdings['bondPosition'].get('raw'),
                'otherPosition': etf_holdings['otherPosition'].get('raw'),
                'preferredPosition': etf_holdings['preferredPosition'].get('raw'),
                'convertiblePosition': etf_holdings['convertiblePosition'].get('raw'),
                'Equity_priceToEarnings': etf_holdings['equityHoldings']['priceToEarnings'].get('raw'),
                'Equity_priceToBook': etf_holdings['equityHoldings']['priceToBook'].get('raw'),
                'Equity_priceToSales': etf_holdings['equityHoldings']['priceToSales'].get('raw'),
                'Equity_priceToCashflow': etf_holdings['equityHoldings']['priceToCashflow'].get('raw'),
                'Equity_threeYearEarningsGrowth': etf_holdings['equityHoldings']['threeYearEarningsGrowth'].get('raw'),
                'Bond_creditQuality': etf_holdings['bondHoldings']['creditQuality'].get('raw'),
                'Bond_duration': etf_holdings['bondHoldings']['duration'].get('raw'),
                'Bond_maturity': etf_holdings['bondHoldings']['maturity'].get('raw'),
            }

            sector_df = pd.DataFrame(data=sector, index=[0])
            bond_rating_df = pd.DataFrame(data=bond_rating, index=[0])
            holding_stats_df = pd.DataFrame(data=holding_stats, index=[0])

            final_output = pd.concat([sector_df, bond_rating_df, holding_stats_df], axis=1)

            return final_output
        except KeyError as e:
            raise YahooETFExtractionError(f"Key({e}) does not exist")

    def price(self) -> pd.DataFrame:
        """
        Extract price data of an ETF
        :return: DataFrame
        """
        try:
            etf_price = self.js['quoteSummary']['result'][0]['price']
            etf_price_df = pd.DataFrame.from_records(etf_price).loc['raw'][:]
            etf_price_df = etf_price_df.to_frame().transpose()
            drop_columns = ['index', 'currencySymbol', 'marketState', 'maxAge', 'postMarketSource', 'preMarketSource',
                            'quoteSourceName', 'regularMarketSource', 'symbol', 'postMarketTime', 'preMarketTime',
                            'regularMarketTime', 'toCurrency', 'underlyingSymbol', 'priceHint', 'circulatingSupply',
                            'fromCurrency', 'lastMarket', 'exchangeDataDelayedBy', 'strikePrice', 'volume24Hr',
                            'volumeAllCurrencies']
            etf_price_df.drop(columns=[x for x in drop_columns if x in etf_price_df.columns], inplace=True)
            return etf_price_df
        except KeyError as e:
            raise YahooETFExtractionError(f"Key({e}) does not exist")


class YahooETF:
    yahoo_module = ['topHoldings', 'fundPerformance', 'price']
    BASE_URL = 'https://query2.finance.yahoo.com/v10/finance/quoteSummary/{stock}?modules=' + '%2C'.join(yahoo_module)
    workers = jcfg.WORKER

    def __init__(self, updated_dt, targeted_pop, batch=False, loggerFileName=None, use_tqdm=True, test_size=None):

        self.loggerFileName = loggerFileName
        self.updated_dt = updated_dt
        self.targeted_pop = targeted_pop
        self.batch = batch
        self.logger = create_log(loggerName='YahooETFStats', loggerFileName=self.loggerFileName)
        self.use_tqdm = use_tqdm
        self.test_size = test_size
        # list to store failed extractions
        self.failed_extract = []
        # object variables for reporting purposes
        self.no_of_stock = 0
        self.time_decay = 0
        self.no_of_web_calls = 0

        # prepare variables
        self.existing_yahoo_etf_annual_returns = None
        self.existing_yahoo_etf_prices = None
        self.existing_yahoo_etf_holdings = None
        self.existing_yahoo_etf_3y5y10y_risk = None
        self.existing_yahoo_etf_trailing_returns = None

    def _acquire_existing_data(self) -> list:
        """
        Get the list of stocks that have been extracted under the same date
        :return: list of stocks
        """
        sql = f"""
                    with raw_data as (
                        SELECT distinct ticker, 'yahoo_etf_prices' as table_name
                        FROM yahoo_etf_prices
                        WHERE updated_dt = '{self.updated_dt}'
                        union 
                        SELECT distinct ticker, 'yahoo_etf_holdings' as table_name
                        FROM yahoo_etf_holdings
                        WHERE updated_dt = '{self.updated_dt}'
                        union 
                        SELECT distinct ticker, 'yahoo_etf_3y5y10y_risk' as table_name
                        FROM yahoo_etf_3y5y10y_risk
                        WHERE updated_dt = '{self.updated_dt}'
                        union 
                        SELECT distinct ticker, 'yahoo_etf_annual_returns' as table_name
                        FROM yahoo_etf_annual_returns
                        WHERE updated_dt = '{self.updated_dt}'
                        union 
                        SELECT distinct ticker, 'yahoo_etf_trailing_returns' as table_name
                        FROM yahoo_etf_trailing_returns
                        WHERE updated_dt = '{self.updated_dt}'
                    )
                    select ticker, count(1)
                    from raw_data
                    group by 1
                    having count(1) <5
            """
        try:
            existing_stock_list = DatabaseManagement(sql=sql).read_sql_to_df()['ticker'].tolist()
        except DatabaseManagementError:
            self.logger.debug(f"Failed to extract existing data from ETF tables")
            existing_stock_list = []

        return existing_stock_list

    def _process_each_table(self, stock, etf_obj, method, table_name) -> None:
        """
        Extract each character of ETF data by method
        :param stock: stock name, which will be used in logs
        :param etf_obj: ETF extraction object (ReadYahooETFStatData)
        :param method: the name of extraction in ReadYahooETFStatData
        :param table_name: insert table name
        :return: None
        """
        try:
            df = getattr(etf_obj, method)()
            if not df.empty:
                df['ticker'] = stock
                df['updated_dt'] = self.updated_dt
                DatabaseManagement(data_df=df, table=table_name, insert_index=False).insert_db()
        except (YahooETFExtractionError, DatabaseManagementError) as e:
            self.logger.debug(f"{stock}: {method} extraction failed, due to {e}")
            self.failed_extract.append(stock)

    def _get_etf_statistics(self, stock) -> None:
        """
        Extract ETF, this is the maine function for each ETF
        :param stock: ETF
        :return: None
        """
        apiparse = YahooAPIParser(url=self.BASE_URL.format(stock=stock))
        data = apiparse.parse()
        self.no_of_web_calls = self.no_of_web_calls + apiparse.no_requests
        # read data from Yahoo API
        etfdataobj = ReadYahooETFStatData(data)

        self._process_each_table(stock=stock, etf_obj=etfdataobj, method='trailingReturns',
                                 table_name='yahoo_etf_trailing_returns')
        self._process_each_table(stock=stock, etf_obj=etfdataobj, method='riskOverviewStatistics',
                                 table_name='yahoo_etf_3y5y10y_risk')
        self._process_each_table(stock=stock, etf_obj=etfdataobj, method='topHoldings',
                                 table_name='yahoo_etf_holdings')
        self._process_each_table(stock=stock, etf_obj=etfdataobj, method='price',
                                 table_name='yahoo_etf_prices')
        self._process_each_table(stock=stock, etf_obj=etfdataobj, method='annualTotalReturns',
                                 table_name='yahoo_etf_annual_returns')

        self.logger.info(f"{stock}: processed successfully")

    def run(self) -> None:
        """
        This is the entrance of this class that runs the ETF extractions
        :return: None
        """
        start = time.time()

        # get the final stock list to be extracted
        stocks_to_extract = SetPopulation(self.targeted_pop).setPop()
        existing_stock = self._acquire_existing_data()
        stocks = returnNotMatches(stocks_to_extract, existing_stock)

        if self.test_size is not None:
            stocks = stocks[:self.test_size]

        self.no_of_stock = len(stocks)

        for i in range(3):
            self.logger.info(f"{'*'*40}{i}st Run Started{'*'*40}")
            if self.batch:
                parallel_process(stocks, self._get_etf_statistics, n_jobs=self.workers, use_tqdm=self.use_tqdm)
            else:
                parallel_process(stocks, self._get_etf_statistics, n_jobs=1, use_tqdm=self.use_tqdm)

            self.logger.info(f"{'*'*40}{i}rd Run Finished{'*'*40}")
            stocks = dedup_list(self.failed_extract)
            if i < 3:
                self.failed_extract = []

        end = time.time()

        self.time_decay = round((end - start) / 60)

    @property
    def get_failed_extracts(self) -> int:
        """
        Get the statistics of failed extractions
        :return: int
        """
        return len(self.failed_extract)


if __name__ == '__main__':
    obj = YahooETF('9999-12-31',
                   targeted_pop='YAHOO_ETF_ALL',
                   batch=True,
                   loggerFileName=None,
                   use_tqdm=True,
                   test_size=100)
    obj.run()

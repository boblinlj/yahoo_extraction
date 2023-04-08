WORKER = 10
JOB_ROOT = "C:/Users/Bob Lin/SynologyDrive/Python Projects/yahoo_spider/stock_data_extractions"
LOG_FORMATTER = '%(levelname)s:%(name)s:%(message)s'
LOG_LVL = 'logging.DEBUG'
GOOGLE_KEY = 'bobanalytics-baba01215163.json'
GCP_BUSKET_NAME = 'stock_data_busket2'
BLOCK = []
UA_LIST = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Mozilla/5.0 (Windows NT 6.1; rv2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11",
    "Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
    "Mozilla/5.0 (Linux; Android 8.0.0;) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.116 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 12_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/79.0.3945.73 Mobile/15E148 Safari/605.1",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36"
]

JOB_DICT = {'YahooStats': ['yahoo_fundamental'],
            'YahooETF': ['yahoo_etf_prices',
                         'yahoo_etf_3y5y10y_risk',
                         'yahoo_etf_annual_returns',
                         'yahoo_etf_holdings',
                         'yahoo_etf_trailing_returns'],
            'YahooFinancial': ['yahoo_annual_fundamental',
                               'yahoo_quarterly_fundamental',
                               'yahoo_trailing_fundamental'],
            'YahooAnalysis': ['yahoo_consensus'],
            'NasdaqUniverse': ['nasdaq_universe']}

FREQUENCY_LIST = ['daily', 'weekly']
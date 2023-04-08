YAHOO_STATS_COLUMNS = ['ticker',
                       'sharesOutstanding',
                       'enterpriseToRevenue',
                       'enterpriseToEbitda',
                       'forwardPE',
                       'trailingPE',
                       'priceToBook',
                       'enterpriseValue',
                       'priceToSalesTrailing12Months',
                       'pegRatio',
                       'marketCap',
                       'shortName',
                       'totalRevenue',
                       'revenueGrowth',
                       'revenueQuarterlyGrowth',
                       'ebitda',
                       'totalAssets',
                       'totalCash',
                       'totalDebt',
                       'operatingCashflow',
                       'freeCashflow',
                       'revenuePerShare',
                       'bookValue',
                       'forwardEps',
                       'trailingEps',
                       'netIncomeToCommon',
                       'profitMargins',
                       'ebitdaMargins',
                       'grossMargins',
                       'operatingMargins',
                       'grossProfits',
                       'currentRatio',
                       'returnOnAssets',
                       'totalCashPerShare',
                       'quickRatio',
                       'payoutRatio',
                       'debtToEquity',
                       'returnOnEquity',
                       'beta',
                       'beta3Year',
                       'floatShares',
                       'sharesShort',
                       '52WeekChange',
                       'sharesPercentSharesOut',
                       'heldPercentInsiders',
                       'heldPercentInstitutions',
                       'shortRatio',
                       'shortPercentOfFloat',
                       'sharesShortPreviousMonthDate',
                       'sharesShortPriorMonth',
                       'lastFiscalYearEnd',
                       'nextFiscalYearEnd',
                       'mostRecentQuarter',
                       'fiveYearAverageReturn',
                       'twoHundredDayAverage',
                       'volume24Hr',
                       'averageDailyVolume10Day',
                       'fiftyDayAverage',
                       'averageVolume10days',
                       'SandP52WeekChange',
                       'dateShortInterest',
                       'regularMarketVolume',
                       'averageVolume',
                       'averageDailyVolume3Month',
                       'volume',
                       'fiftyTwoWeekHigh',
                       'fiveYearAvgDividendYield',
                       'fiftyTwoWeekLow',
                       'currentPrice',
                       'previousClose',
                       'regularMarketOpen',
                       'regularMarketPreviousClose',
                       'open',
                       'dayLow',
                       'dayHigh',
                       'regularMarketDayHigh',
                       'postMarketChange',
                       'postMarketPrice',
                       'preMarketChange',
                       'regularMarketPrice',
                       'preMarketChangePercent',
                       'postMarketChangePercent',
                       'regularMarketChange',
                       'regularMarketChangePercent',
                       'preMarketPrice',
                       'targetLowPrice',
                       'targetMeanPrice',
                       'targetMedianPrice',
                       'targetHighPrice',
                       'dividendYield',
                       'lastDividendValue',
                       'lastSplitDate',
                       'lastDividendDate',
                       'lastSplitFactor',
                       'earningsGrowth',
                       'numberOfAnalystOpinions',
                       'trailingAnnualDividendYield',
                       'trailingAnnualDividendRate',
                       'dividendRate',
                       'exDividendDate',
                       # 'updated_dt'
                       ]

drop_summaryDetail = ['algorithm', 'maxAge']

drop_price = ['averageDailyVolume10Day',
              'circulatingSupply',
              'currency',
              'fromCurrency',
              'lastMarket',
              'marketCap',
              'maxAge',
              'openInterest',
              'priceHint',
              'regularMarketDayHigh',
              'regularMarketDayLow',
              'regularMarketOpen',
              'regularMarketPreviousClose',
              'regularMarketVolume',
              'strikePrice',
              'toCurrency',
              'volume24Hr',
              'volumeAllCurrencies']

drop_defaultKeyStatistics = ['beta',
                             # 'forwardPE',
                             'maxAge',
                             'priceHint',
                             'priceToSalesTrailing12Months',
                             'totalAssets',
                             'yield',
                             'ytdReturn']

drop_financialData = ['maxAge',
                      'profitMargins']

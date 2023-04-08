from util.extract_ticker_from_nasdaq import get_ticker_cik_mapping

updated_dt = '2022-12-17'
df = get_ticker_cik_mapping()

print(df)
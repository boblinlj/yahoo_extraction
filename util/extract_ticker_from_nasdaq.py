from pandas_datareader.nasdaq_trader import get_nasdaq_symbols
import pandas as pd


def get_nasdaq_ticker(): 
    try:
        symbols = get_nasdaq_symbols()
        return symbols
    except Exception as e:
        print(e)
        
def get_ticker_cik_mapping():
    try :
        df = pd.read_csv("https://www.sec.gov/include/ticker.txt",
                        header=None,
                        names=['ticker','cik'],
                        sep='\t')
        return df
    except Exception as e:
        print(e)
   


if __name__ == '__main__':
    updated_dt = '2022-12-17'
    df = get_ticker_cik_mapping()
    
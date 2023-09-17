import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
import datetime as dt

@st.cache_data
def load_data(filepath):
    if 'prices' in filepath:
        df = pd.read_parquet(filepath)
        df['Date'] = pd.to_datetime(df['Date'])
        df['Date'] = df['Date'].dt.date
        df = df.set_index(['Date'])
    elif filepath == 'data/earnings.csv':
        today = dt.date.today()
        df = pd.read_csv(filepath, parse_dates=['dates'])
        df = df.query('dates < @today')
        df = df.drop(columns=['country', 'category'])
        df['dates'] = df['dates'].dt.date
    else:
        df = pd.read_csv(filepath)
    return df


@st.cache_data
def convert_df(df):
   return df.to_csv(index=False).encode('utf-8')

prices1 = load_data('data/prices1.parquet')
prices2 = load_data('data/prices2.parquet')
prices = pd.concat([prices1, prices2])
# prices = load_data('data/prices.csv')
stocks = load_data('data/stocks.csv')
stocks['category'] = stocks.category.str.split(', ')
earnings = load_data('data/earnings.csv')


ticker_options = stocks.ticker.unique()
category_options = stocks.category.str[0].unique()

# create sidebar menu with options
selected = option_menu(
    menu_title=None,
    menu_icon='cast',
    default_index=0,
    options=['Stock Filter', 'Earnings Data'],
    orientation='horizontal',
    icons=['funnel', 'graph-up'],
    styles= {'container': {
                'font-size': '12px'
    }}
)


if selected == 'Stock Filter':
    st.header('Stock Filter')

    # market cap filter
    mkt_cap_range = st.slider('Market Cap (billions)', value=(0, 3000), step=100)
    temp_df = stocks.query('mkt_cap_billions.between(@mkt_cap_range[0], @mkt_cap_range[1])').sort_values(by='mkt_cap_billions', ascending=False)
    temp_df['volume_90_day'] = temp_df['volume_90_day'] / 1000


    # volume filter
    volume_range = st.slider('90-Day Median Volume (millions)', value=(0, 150), step=5)
    temp_df = temp_df.query('volume_90_day.between(@volume_range[0], @volume_range[1])')


    # category filter
    categories = st.multiselect('Select Category', options=category_options)
    if len(categories) > 0:
        temp_df = temp_df[temp_df['category'].apply(lambda x: any(cat in x for cat in categories))]


    st.dataframe(temp_df.reset_index(drop=True))


if selected == 'Earnings Data':
    st.header('Earnings Data')

    valid_tickers = earnings['ticker'].unique()
    error_tickers = []
    tickers_filter = st.text_input('Enter Ticker(s)', help='Separate tickers by single space')
    tickers = tickers_filter.split()
    tickers = [ticker.upper() for ticker in tickers]

    for ticker in tickers:
        if ticker not in valid_tickers:
            tickers.remove(ticker)
            error_tickers.append(ticker)
    if len(error_tickers) > 0:
        st.write('The following tickers are not available:', ' '.join(error_tickers))
    tickers = set(tickers)
    tickers = list(tickers)
    n_days = st.number_input('Stock performance after n trading days', step=1, min_value=1)


    if len(tickers) > 0:
        earnings_temp = earnings.query('ticker.isin(@tickers)')
        earnings_temp['dow'] = pd.to_datetime(earnings_temp['dates']).dt.day_of_week
        earnings_temp = earnings_temp.query('dow.isin([0, 1, 2, 3, 4])')

        trading_dates = prices.reset_index()['Date']
        cutoff =  trading_dates.iloc[-n_days]
        earnings_temp = earnings_temp.query('dates < @cutoff')
        
        earnings_temp = earnings_temp.dropna()

        prices_temp = ((prices['Close'][tickers].shift(-n_days) - prices['Close'][tickers]) / prices['Close'][tickers])
        prices_temp.index = pd.to_datetime(prices_temp.index)

        for index, row in earnings_temp.iterrows():
            try:
                some_date = row['dates']
                ticker = row['ticker']
                date_filter = earnings_temp['dates'] == some_date
                ticker_filter = earnings_temp['ticker'] == ticker
                earnings_temp.loc[date_filter & ticker_filter, f'{n_days}_day_performance'] = prices_temp[ticker][str(some_date)]
            except:
                print(ticker, some_date)

        # format the data frame
        earnings_temp[f'{n_days}_day_performance'] = (earnings_temp[f'{n_days}_day_performance'] * 100).round(2)
        earnings_temp = earnings_temp.rename(columns={'dates': 'earnings_date'})
        earnings_temp = earnings_temp[['ticker', 'earnings_date', 'eps_estimate', 'eps_actual', f'{n_days}_day_performance']].reset_index(drop=True)
        st.dataframe(earnings_temp)



        csv = convert_df(earnings_temp)
        st.download_button(
        "Press to Download",
        csv,
        "earnings.csv",
        "text/csv",
        key='download-csv'
        )
import polars as pl
import streamlit as st 
import numpy as np
import requests
import os 

st.set_page_config(
    page_title='NYC Taxi Dashboard',
    page_icon='🚕',
    layout='wide',
    initial_sidebar_state='expanded'
)

st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1E3A5F;
        margin-bottom: 0;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #666;
        margin-top: 0;
    }
</style>
""", unsafe_allow_html=True)

# download = [
#     {
#         'url': 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet',
#         'filename': 'yellow_tripdata_2024-01.parquet'
#     },
#     {
#         'url': 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv',
#         'filename': 'taxi_zone_lookup.csv'
#     }
# ]

# for file in download:
#   print(f'Downloading {file['url']}...')

#   response = requests.get(file['url'], stream=True)

#   response.raise_for_status()

#   with open(file['filename'], 'wb') as f:
#     for chunk in response.iter_content(chunk_size=8192):
#       f.write(chunk)

@st.cache_data
def load_taxi():
    """
    Load the taxi data and do some basic prep work.

    The @st.cache_data decorator is honestly a lifesaver here - without it,
    the data would reload EVERY time someone moves a slider or clicks anything.
    That would be painfully slow with 100k rows.
    """
    try:
        # First try the local copy in the dashboard folder
        df = pl.read_parquet('yellow_tripdata_2024-01.parquet')
        return df
    except FileNotFoundError:
        try:
            # Maybe it's in the parent directory?
            df = pl.read_parquet('../yellow_tripdata_2024-01.parquet')
            return df
        except FileNotFoundError:
            # Okay, we're stuck - let the user know what's up
            st.error("Can't find the dataset! Make sure 'yellow_tripdata_2024-01.parquet' is in the dashboard folder.")
            st.info("You can download it from: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet")
            st.stop()
            return None

def load_lookup():
    """
    Load the taxi data and do some basic prep work.

    The @st.cache_data decorator is honestly a lifesaver here - without it,
    the data would reload EVERY time someone moves a slider or clicks anything.
    That would be painfully slow with 100k rows.
    """
    try:
        # First try the local copy in the dashboard folder
        df = pl.read_csv('taxi_zone_lookup.csv')
    except FileNotFoundError:
        try:
            # Maybe it's in the parent directory?
            df = pl.read_csv('../taxi_zone_lookup.csv')
        except FileNotFoundError:
            # Okay, we're stuck - let the user know what's up
            st.error("Can't find the dataset! Make sure 'taxi_zone_lookup.csv' is in the dashboard folder.")
            st.info("You can download it from: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet")
            st.stop()

# taxi_df = load_taxi()
# zones_df = load_lookup()

# # e) removing null from critical columns
# clean_df = taxi_df.filter(pl.col('tpep_pickup_datetime').is_not_null()& pl.col('tpep_dropoff_datetime').is_not_null()&
#                           pl.col('PULocationID').is_not_null()& pl.col('DOLocationID').is_not_null()& pl.col('fare_amount').is_not_null())

# # h.e) Rows removed
# length = len(taxi_df)
# print(f'Number of rows removed due to null values: {len(taxi_df) - len(clean_df):,}')

# # f) removing invalid rows
# clean_df = (clean_df
#             .filter(pl.col('trip_distance') > 0)
#             .filter(pl.col('trip_distance') <= 50)
#             .filter(pl.col('fare_amount') > 0)
#             .filter(pl.col('fare_amount') <= 500)
#             )

# # h.f) Rows removed
# print(f'Number of rows removed due to invalid or outlier values: {length - len(clean_df):,}')
# length = len(clean_df)

# # g) removing invalid times
# clean_df = clean_df.filter(pl.col('tpep_pickup_datetime') < pl.col('tpep_dropoff_datetime'))

# # h.g) Rows removed
# print(f'Number of rows removed due to invalid times: {length - len(clean_df):,}')

# print(f'\nNumber of rows in cleaned dataset: {len(clean_df):,}')
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

download = [
    {
        'url': 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet',
        'filename': 'yellow_tripdata_2024-01.parquet'
    },
    {
        'url': 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv',
        'filename': 'taxi_zone_lookup.csv'
    }
]

for file in download:
  print(f'Downloading {file['url']}...')

  response = requests.get(file['url'], stream=True)

  response.raise_for_status()

  with open(file['filename'], 'wb') as f:
    for chunk in response.iter_content(chunk_size=8192):
      f.write(chunk)

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
        return df
    except FileNotFoundError:
        try:
            # Maybe it's in the parent directory?
            df = pl.read_csv('../taxi_zone_lookup.csv')
            return df
        except FileNotFoundError:
            # Okay, we're stuck - let the user know what's up
            st.error("Can't find the dataset! Make sure 'taxi_zone_lookup.csv' is in the dashboard folder.")
            st.info("You can download it from: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet")
            st.stop()
            return None

taxi_df = load_taxi()
zones_df = load_lookup()

# e) removing null from critical columns
clean_df = taxi_df.filter(pl.col('tpep_pickup_datetime').is_not_null()& pl.col('tpep_dropoff_datetime').is_not_null()&
                          pl.col('PULocationID').is_not_null()& pl.col('DOLocationID').is_not_null()& pl.col('fare_amount').is_not_null())

# f) removing invalid rows
clean_df = (clean_df
            .filter(pl.col('trip_distance') > 0)
            .filter(pl.col('trip_distance') <= 50)
            .filter(pl.col('fare_amount') > 0)
            .filter(pl.col('fare_amount') <= 500)
            )

# g) removing invalid times
clean_df = clean_df.filter(pl.col('tpep_pickup_datetime') < pl.col('tpep_dropoff_datetime'))

# Adding new columns
enriched = clean_df.with_columns([
    # i) trip duration in minutes
    ((pl.col('tpep_dropoff_datetime')-pl.col('tpep_pickup_datetime')).dt.total_seconds() / 60).alias('trip_duration_minutes'),

])

enriched = enriched.with_columns([
    # j) trip speed
    pl.when(pl.col('trip_duration_minutes') > 0)
    .then((pl.col('trip_distance') / pl.col('trip_duration_minutes')))
    .otherwise(0)
    .alias('trip_speed_mph'),

    # k) pickup hour
    pl.col('tpep_pickup_datetime').dt.hour().alias('pickup_hour'),

    # l) pickup day
    pl.col('tpep_pickup_datetime').dt.strftime('%A').alias('pickup_day_of_week')
])

enriched = (enriched
                .join(zones_df, left_on='PULocationID', right_on='LocationID', how='left')
                .rename({'Zone': 'pickup_zone', 'Borough': 'pickup_borough'})
                .join(zones_df, left_on='DOLocationID', right_on='LocationID', how='left')
                .rename({'Zone': 'dropoff_zone', 'Borough': 'dropoff_borough'})
)

enriched.select([
    'PULocationID', 'pickup_zone', 'pickup_borough',
    'DOLocationID', 'dropoff_zone', 'dropoff_borough',
    'trip_distance', 'fare_amount', 'trip_duration_minutes'
])

vis_sam = enriched.sample(n=100000, seed = 42)

st.markdown('<p class="main-header">NYC Taxi Trip Dashboard</p>', unsafe_allow_html=True)
st.markdown('<p class="sub-header">Exploring Yellow Taxi Data from January 2024</p>', unsafe_allow_html=True)

st.divider()

st.subheader('Key Metrics at a Glance')

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric(
        label="Total Trips",
        value=f"{len(vis_sam):,}",
        help="Number of trips in our sample (we took 100k from the filtered dataset)"
    )

with col2:
    avg_fare = vis_sam['fare_amount'].mean()
    st.metric(
        label="Average Fare",
        value=f"${avg_fare:.2f}",
        help="Mean fare"
    )

with col3:
    total_fare = vis_sam['fare_amount'].sum()
    st.metric(
        label="Total Fare",
        value=f"${total_fare:.2f}",
        help="Total fare for all trips in the visual sample"
    )

with col4:
    avg_distance = vis_sam['trip_distance'].mean()
    st.metric(
        label="Avg Distance",
        value=f"{avg_distance:.2f} mi",
        help="Most NYC taxi trips are pretty short, actually"
    )

with col5:
    avg_duration = vis_sam['trip_duration_min'].mean()
    st.metric(
        label="Avg Duration",
        value=f"{avg_duration:.1f} min",
        help="Includes time stuck in traffic, of course"
    )

st.divider()
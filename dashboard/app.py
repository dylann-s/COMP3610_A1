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

if taxi_df is not None:
    os.remove('yellow_tripdata_2024-01.parquet')
    print("Deleted taxi data file")

if zones_df is not None:
    os.remove('taxi_zone_lookup.csv')
    print("Deleted zone lookup file")

# e) removing null from critical columns
taxi_df = taxi_df.filter(pl.col('tpep_pickup_datetime').is_not_null()& pl.col('tpep_dropoff_datetime').is_not_null()&
                          pl.col('PULocationID').is_not_null()& pl.col('DOLocationID').is_not_null()& pl.col('fare_amount').is_not_null())

# f) removing invalid rows
taxi_df = (taxi_df
            .filter(pl.col('trip_distance') > 0)
            .filter(pl.col('trip_distance') <= 50)
            .filter(pl.col('fare_amount') > 0)
            .filter(pl.col('fare_amount') <= 500)
            )

# g) removing invalid times
taxi_df = taxi_df.filter(pl.col('tpep_pickup_datetime') < pl.col('tpep_dropoff_datetime'))

# Adding new columns
taxi_df = taxi_df.with_columns([
    # i) trip duration in minutes
    ((pl.col('tpep_dropoff_datetime')-pl.col('tpep_pickup_datetime')).dt.total_seconds() / 60).alias('trip_duration_minutes'),

])

taxi_df = taxi_df.with_columns([
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

vis_sam = taxi_df.sample(n=50000, seed = 42)

if 'pickup_zone' not in vis_sam.columns and 'dropoff_zone' not in vis_sam.columns:
    vis_sam = (vis_sam
        .join(zones_df, left_on='PULocationID', right_on='LocationID', how='left')
        .rename({'Zone': 'pickup_zone', 'Borough': 'pickup_borough'})
        .join(zones_df, left_on='DOLocationID', right_on='LocationID', how='left')
        .rename({'Zone': 'dropoff_zone', 'Borough': 'dropoff_borough'})
    )
    print("Successfully joined zone data")
    zones_df.drop()
else:
    print("Zone columns already exist, skipping join")

if 'payment_description' not in vis_sam.columns:
  payment_lookup = pl.DataFrame({
    'payment_type': [0, 1, 2, 3, 4],
    'payment_description': ['Credit Card', 'Cash', 'No Charge', 'Dispute', 'Unknown']
  })
  
  vis_sam = vis_sam.join(
    payment_lookup,
    on='payment_type',
    how='left'
  )

  payment_lookup.drop()
else:
  print("Payment description column already exists, skipping join")

# vis_sam.select([
#     'PULocationID', 'pickup_zone', 'pickup_borough',
#     'DOLocationID', 'dropoff_zone', 'dropoff_borough',
#     'trip_distance', 'fare_amount', 'trip_duration_minutes'
# ])

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
    avg_duration = vis_sam['trip_duration_minutes'].mean()
    st.metric(
        label="Avg Duration",
        value=f"{avg_duration:.1f} min",
        help="Includes time stuck in traffic, of course"
    )

st.divider()

# ============== SIDEBAR FILTERS ==============

st.sidebar.header("Filters")

# Date range
st.sidebar.subheader("Date Range")
min_date = vis_sam['tpep_pickup_datetime'].min()
max_date = vis_sam['tpep_pickup_datetime'].max()

date_range = st.sidebar.date_input(
    "Pick your dates:",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = end_date = date_range

hour_range = st.sidebar.slider(
    "🕐 Hour Range",
    min_value=0,
    max_value=23,
    value=(0, 23),
    step=1
)

payment_types = vis_sam.select(pl.col('payment_description').unique()).to_series().to_list()
payment_types = sorted([p for p in payment_types if p is not None])
selected_payments = st.sidebar.multiselect(
    "💳 Payment Types",
    options=payment_types,
    default=payment_types
)

filtered_df = vis_sam.clone()

if len(date_range) == 2:
    start_date, end_date = date_range
    filtered_df = filtered_df.filter(
        (pl.col('tpep_pickup_datetime') >= start_date) &
        (pl.col('tpep_pickup_datetime') <= end_date)
    )

# Apply hour filter
filtered_df = filtered_df.filter(
    (pl.col('pickup_hour') >= hour_range[0]) & 
    (pl.col('pickup_hour') <= hour_range[1])
)

# Apply payment type filter
if selected_payments:
    filtered_df = filtered_df.filter(pl.col('payment_description').is_in(selected_payments))


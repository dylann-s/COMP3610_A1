import polars as pl
import plotly.express as px
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

vis_sam = taxi_df.sample(n=10000, seed = 42)

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
    "Hour Range",
    min_value=0,
    max_value=23,
    value=(0, 23),
    step=1
)

payment_types = vis_sam.select(pl.col('payment_description').unique()).to_series().to_list()
payment_types = sorted([p for p in payment_types if p is not None])
selected_payments = st.sidebar.multiselect(
    "Payment Types",
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

tab1, tab2, tab3, tab4, tab5 = st.tabs(["BarGraph", "LineGraph", "Histogram", "BarGraph2", "Heatmap"])

with tab1:
    st.subheader("Top 10 Pickup Zones")
    st.caption("Top 10 Taxi Pickup Zones")

    zone_counts_vis = (filtered_df
    .group_by('pickup_zone')
    .agg(pl.len().alias('zone_pickups'))
    .sort('zone_pickups', descending=True)
    .head(10)
)

    fig1 = px.bar(zone_counts_vis,
        x = 'pickup_zone',
        y = 'zone_pickups',
        title = 'Top 10 Pickup Zones for NYC Taxies',
        labels = {'pickup_zone': 'Pickup Zone', 'zone_cnunts': 'Number of Trips'},
        text = 'zone_pickups'
    )

    st.plotly_chart(fig1, use_container_width=True)

with tab2:
    st.subheader("Line Graph of Average Fare vs Hour")
    st.caption("Line Graph of average fare")

    avg_fare_vis = (filtered_df
        .group_by('pickup_hour')
        .agg(
            pl.mean('fare_amount').round(2).alias('avg_fare'),
        )
        .sort('pickup_hour')
    )

    fig2 = px.line(
        avg_fare_vis,
        x='pickup_hour',
        y='avg_fare',
        title='Average Fare per Hour',
        labels=({'pickup_hour': 'Hour', 'avg_fare': 'Average Fare'}),
        markers=True
    )

    st.plotly_chart(fig2, use_container_width=True)

with tab3:
    st.subheader("Histogram of Trip Distances")
    st.caption("Histogram showing the amount of trips for each range of distances")

    fig3 = px.histogram(
        filtered_df,
        x = 'trip_distance',
        nbins = 50,
        range_x=[filtered_df['trip_distance'].min(), filtered_df['trip_distance'].max()],
        title = 'Distribution of Trip Distances',
        labels = {'trip_distance': 'Trip Distance (miles)', 'count': 'Number of Trips'}
    )

    st.plotly_chart(fig3, use_container_width=True)

with tab4:
    st.subheader("Bar Chart of Paymeent Type Percentages")
    st.caption("Bar chart showing the percentagage that each payment type occupies")

    pay_type_vis = (filtered_df
        .group_by('payment_description')
        .agg(pl.len().alias('count'))
        .with_columns(
            (pl.col('count') * 100.0 / pl.sum('count')).round(2).alias('percentage')
        )
        .sort('percentage', descending=True)
    )


    fig4 = px.bar(
        pay_type_vis,
        x='payment_description',
        y='percentage',
        title='Percentage of Payment Types',
        labels={'payment_description': 'Payment Type', 'percentage': 'Percentage'},
        text='percentage'
    )

    st.plotly_chart(fig4, use_container_width=True)

with tab5:
    st.subheader("Heatmap for Trip amounts for the day and hours of the weeks")
    st.caption("Histogram showing the amount of trips for each range of distances")

    fig5 = px.density_heatmap(
        filtered_df,
        x = 'pickup_hour',
        y = 'pickup_day_of_week',
        title = 'Trip Density by Hour and Day of Week',
        labels = {'pickup_hour': 'Pickup Hour', 'pickup_day_of_week': 'Pickup Day of Week', 'count': 'Number of Trips'},
        color_continuous_scale = 'agsunset',
        category_orders = {'pickup_day_of_week': ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']}
    )

    st.plotly_chart(fig5, use_container_width=True)
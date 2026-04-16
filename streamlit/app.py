# streamlit/app.py

import streamlit as st
import pandas as pd

# 1. Page Configuration
st.set_page_config(
    page_title="Weather Analytics Dashboard",
    page_icon="⛅",
    layout="wide"
)

st.title("⛅ Weather Analytics Dashboard")
st.markdown("Stakeholder consumption layer powered by Snowflake & governed dbt metrics.")

# 2. Connection & Caching
@st.cache_data(ttl=600)
def load_data():
    conn = st.connection("snowflake", type="snowflake")
    query = """
        SELECT *
        FROM mart_daily_weather_summary
        ORDER BY summary_date DESC
    """
    df = conn.query(query, ttl=0)
    
    # Normalize column names to lowercase to match our dbt models
    df.columns = [col.lower() for col in df.columns]
    
    # Ensure date column is properly typed for Streamlit date inputs
    df['summary_date'] = pd.to_datetime(df['summary_date']).dt.date
    return df

# Fetch data
with st.spinner("Connecting to Snowflake warehouse..."):
    try:
        weather_df = load_data()
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        st.stop()

# 3. Sidebar Filters
st.sidebar.header("Dashboard Filters")

# Location Filter
locations = weather_df['location_name'].unique().tolist()
selected_location = st.sidebar.selectbox("Select Location", options=["All Locations"] + locations)

# Date Filter
min_date = weather_df['summary_date'].min()
max_date = weather_df['summary_date'].max()

# Handle edge case where there is only one day of data
if min_date == max_date:
    st.sidebar.write(f"**Date:** {min_date}")
    start_date, end_date = min_date, max_date
else:
    date_selection = st.sidebar.date_input(
        "Select Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    # date_input can return a single date or a tuple/list of dates
    if isinstance(date_selection, tuple) and len(date_selection) == 2:
        start_date, end_date = date_selection
    else:
        start_date = end_date = date_selection

# Apply Filters 
filtered_df = weather_df[
    (weather_df['summary_date'] >= start_date) &
    (weather_df['summary_date'] <= end_date)
]

if selected_location != "All Locations":
    filtered_df = filtered_df[filtered_df['location_name'] == selected_location]

# 4. KPI Cards
st.subheader("Key Performance Indicators")
kpi1, kpi2, kpi3, kpi4 = st.columns(4)

if not filtered_df.empty:
    avg_temp = filtered_df['avg_temp_c'].mean()
    total_precip = filtered_df['total_precipitation_mm'].sum()
    max_wind = filtered_df['max_wind_speed_kmh'].max()
    avg_humidity = filtered_df['avg_humidity'].mean()
else:
    avg_temp = total_precip = max_wind = avg_humidity = 0.0

kpi1.metric("Avg Temperature", f"{avg_temp:.1f} °C")
kpi2.metric("Total Precipitation", f"{total_precip:.1f} mm")
kpi3.metric("Max Wind Speed", f"{max_wind:.1f} km/h")
kpi4.metric("Avg Humidity", f"{avg_humidity:.1f} %")

st.divider()

# 5. Charts
col1, col2 = st.columns(2)

with col1:
    st.subheader("Temperature Trends")
    # Group by date to average metrics if 'All Locations' is selected
    temp_trend = filtered_df.groupby('summary_date')[['min_temp_c', 'avg_temp_c', 'max_temp_c']].mean()
    st.line_chart(temp_trend)

with col2:
    st.subheader("Daily Precipitation")
    precip_trend = filtered_df.groupby('summary_date')['total_precipitation_mm'].sum()
    st.bar_chart(precip_trend)

st.subheader("Wind Speed vs. Humidity")
# Requires Streamlit 1.26+ for native scatter_chart
st.scatter_chart(
    filtered_df,
    x='avg_humidity',
    y='max_wind_speed_kmh',
    color='location_name' if selected_location == "All Locations" else None
)
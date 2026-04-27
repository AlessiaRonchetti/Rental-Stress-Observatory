import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
import json
import requests

# ============================================================
# CONFIGURAZIONE PAGINA
# ============================================================
st.set_page_config(
    page_title="NYC Rental Stress Observatory",
    page_icon="🏙️",
    layout="wide"
)

# ============================================================
# CONNESSIONE POSTGRESQL
# ============================================================
@st.cache_resource
def get_connection():
    return psycopg2.connect(
        host="postgres",
        dbname="rental_observatory",
        user="bdt_admin",
        password="bdt_password"
    )

@st.cache_data
def load_data():
    conn = get_connection()
    zip_stress      = pd.read_sql("SELECT * FROM zip_airbnb_stress", conn)
    borough_summary = pd.read_sql("SELECT * FROM airbnb_borough_summary", conn)
    pressure        = pd.read_sql("SELECT * FROM airbnb_pressure", conn)
    market_stress   = pd.read_sql("SELECT * FROM market_rental_stress", conn)
    return zip_stress, borough_summary, pressure, market_stress

zip_stress, borough_summary, pressure, market_stress = load_data()

# ============================================================
# SCARICA GEOJSON ZIP NYC
# ============================================================
@st.cache_data
def load_geojson():
    url = "https://data.cityofnewyork.us/resource/pri4-ifjk.geojson?$limit=5000"
    r = requests.get(url, timeout=60)
    return r.json()

geojson = load_geojson()

# ============================================================
# HEADER
# ============================================================
st.title("🏙️ NYC Rental Market Stress Observatory")
st.markdown("Monitoring the impact of short-term rentals on housing affordability in New York City")

# ============================================================
# KPI CARDS
# ============================================================
total_zip    = len(market_stress)
affordable   = len(market_stress[market_stress["rent_burden_pct"] < 30])
stressed     = len(market_stress[(market_stress["rent_burden_pct"] >= 30) & (market_stress["rent_burden_pct"] < 50)])
severe       = len(market_stress[market_stress["rent_burden_pct"] >= 50])
total_listings = int(zip_stress["num_airbnb_listings"].sum())

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("📍 ZIP Codes Analyzed", total_zip)
col2.metric("🟢 Affordable", affordable)
col3.metric("🟡 Stressed", stressed)
col4.metric("🔴 Severely Stressed", severe)
col5.metric("🏠 Total Airbnb Listings", f"{total_listings:,}")

st.divider()

# ============================================================
# HEATMAP — RENTAL STRESS PER ZIP CODE
# ============================================================
st.subheader("🗺️ Rental Stress Heatmap by ZIP Code")

# Slider per filtrare per rent burden
min_burden, max_burden = st.slider(
    "Filter by Rent Burden %",
    min_value=0, max_value=200,
    value=(0, 200)
)

filtered = market_stress[
    (market_stress["rent_burden_pct"] >= min_burden) &
    (market_stress["rent_burden_pct"] <= max_burden)
]

fig_map = px.choropleth_mapbox(
    filtered,
    geojson=geojson,
    locations="zip_code",
    featureidkey="properties.modzcta",   # chiave ZIP nel GeoJSON
    color="rent_burden_pct",
    color_continuous_scale="Reds",
    range_color=(0, 150),
    mapbox_style="carto-positron",
    zoom=9,
    center={"lat": 40.7128, "lon": -74.0060},
    opacity=0.7,
    hover_data={
        "zip_code": True,
        "rent_burden_pct": True,
        "median_income": True,
        "market_rent": True,
        "total_population": True
    },
    labels={
        "rent_burden_pct": "Rent Burden %",
        "median_income": "Median Income $",
        "market_rent": "Market Rent $/mo",
        "total_population": "Population"
    }
)

fig_map.update_layout(height=600, margin={"r":0,"t":0,"l":0,"b":0})
st.plotly_chart(fig_map, use_container_width=True)

st.divider()

# ============================================================
# GRAFICI BOROUGH
# ============================================================
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("🏘️ Airbnb Listings per Borough")
    fig_bar = px.bar(
        borough_summary.sort_values("num_listings", ascending=True),
        x="num_listings",
        y="neighbourhood_group_cleansed",
        orientation="h",
        color="avg_occupancy_pct",
        color_continuous_scale="Oranges",
        labels={
            "num_listings": "Number of Listings",
            "neighbourhood_group_cleansed": "Borough",
            "avg_occupancy_pct": "Avg Occupancy %"
        }
    )
    st.plotly_chart(fig_bar, use_container_width=True)

with col_right:
    st.subheader("📊 Airbnb Pressure Index per Borough")
    fig_pressure = px.bar(
        pressure.sort_values("pressure_score", ascending=True),
        x="pressure_score",
        y="neighbourhood_group_cleansed",
        orientation="h",
        color="pressure_score",
        color_continuous_scale="Reds",
        labels={
            "pressure_score": "Pressure Score",
            "neighbourhood_group_cleansed": "Borough"
        }
    )
    st.plotly_chart(fig_pressure, use_container_width=True)

st.divider()

# ============================================================
# TOP 10 ZIP CODE PIÙ STRESSATI
# ============================================================
st.subheader("🔝 Top 10 Most Stressed ZIP Codes")
top10 = zip_stress \
    .dropna(subset=["rent_burden_pct"]) \
    .sort_values("rent_burden_pct", ascending=False) \
    .head(10)[["zip_code", "neighbourhood_group_cleansed",
               "rent_burden_pct", "stress_category",
               "median_income", "market_rent",
               "num_airbnb_listings", "avg_occupancy_pct"]]

st.dataframe(top10, use_container_width=True)

st.divider()

# ============================================================
# SCATTER PLOT — CORRELAZIONE AIRBNB VS STRESS
# ============================================================
st.subheader("📈 Correlation: Airbnb Listings vs Rent Burden")
fig_scatter = px.scatter(
    zip_stress.dropna(subset=["rent_burden_pct"]),
    x="num_airbnb_listings",
    y="rent_burden_pct",
    color="neighbourhood_group_cleansed",
    size="avg_occupancy_pct",
    hover_data=["zip_code", "median_income", "market_rent"],
    labels={
        "num_airbnb_listings": "Number of Airbnb Listings",
        "rent_burden_pct": "Rent Burden %",
        "neighbourhood_group_cleansed": "Borough"
    }
)
fig_scatter.add_hline(y=30, line_dash="dash", line_color="orange",
                      annotation_text="Stressed threshold (30%)")
fig_scatter.add_hline(y=50, line_dash="dash", line_color="red",
                      annotation_text="Severely stressed threshold (50%)")
st.plotly_chart(fig_scatter, use_container_width=True)

# ============================================================
# FOOTER
# ============================================================
st.markdown("---")
st.markdown("**Data Sources:** Inside Airbnb | Zillow ZORI | US Census ACS 2024")
st.markdown("**Stack:** Apache Spark · MinIO · PostgreSQL · Streamlit · Apache Sedona")

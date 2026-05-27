import streamlit as st
import psycopg2 # connessione a PostgreSQL da Python
import pandas as pd
import plotly.express as px
import requests
 
#Page config 
st.set_page_config(
    page_title="NYC Rental Stress Observatory",
    layout="wide"
)
#PostgreSQL connection
@st.cache_resource #"esegui questa funzione una sola volta e riusa la stessa connessione per sempre"
 
 
def get_connection():
    return psycopg2.connect(
        host="postgres",
        dbname="rental_observatory",
        user="bdt_admin",
        password="bdt_password"
    )
 
@st.cache_data(ttl=300)
def load_data():
    """
    Loads all tables from PostgreSQL.
    ttl=300 → cache expires every 5 minutes (auto-refresh).
    Returns None for all tables if pipeline has not run yet.
    """
    try:
        conn = get_connection()
        zip_stress      = pd.read_sql("SELECT * FROM zip_airbnb_stress_summary", conn)
        borough_summary = pd.read_sql("SELECT * FROM airbnb_borough_summary", conn)
        pressure        = pd.read_sql("SELECT * FROM airbnb_pressure", conn)
        market_stress   = pd.read_sql("SELECT * FROM market_rental_stress", conn)
        return zip_stress, borough_summary, pressure, market_stress
    except Exception as e:
        return None, None, None, None
 
zip_stress, borough_summary, pressure, market_stress = load_data()
 
#GeoJSON NYC ZIP boundaries
@st.cache_data
def load_geojson():
    url = "https://data.cityofnewyork.us/resource/pri4-ifjk.geojson?$limit=5000"
    r = requests.get(url, timeout=60)
    return r.json()
 
#Header
st.title("NYC Rental Market Stress Observatory")
st.markdown("Monitoring the impact of short-term rentals on housing affordability in New York City")
 
#Pipeline not yet run
if market_stress is None:
    st.warning("""
         **Data not available yet.**
 
        The pipeline has not run yet or is still processing.
        The dashboard will refresh automatically every 5 minutes.
    """)
    st.stop() # tutto il codice dopo questa riga non viene eseguito. 
    #Evita che Streamlit cerchi di disegnare grafici con dati None e vada in errore
 
#KPI Cards
total_zip      = len(market_stress)
affordable     = len(market_stress[market_stress["rent_burden_pct"] < 30])
stressed       = len(market_stress[(market_stress["rent_burden_pct"] >= 30) & (market_stress["rent_burden_pct"] < 50)])
severe         = len(market_stress[market_stress["rent_burden_pct"] >= 50])
total_listings = int(zip_stress["num_airbnb_listings"].sum())
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric(" ZIP Codes Analyzed",    total_zip)#mostra un numero con un etichetta
col2.metric("🟢 Affordable",            affordable)
col3.metric("🟡 Stressed",              stressed)
col4.metric("🔴 Severely Stressed",     severe)
col5.metric(" Total Airbnb Listings", f"{total_listings:,}")
 
st.divider()#linea orizzontale
 
#Heatmap: Rental Stress per ZIP code
st.subheader("Rental Stress Heatmap by ZIP Code")
min_burden, max_burden = st.slider("Filter by Rent Burden %",min_value=0,max_value=200,value=(0,200)) 
filtered = market_stress[
    (market_stress["rent_burden_pct"] >= min_burden) &
    (market_stress["rent_burden_pct"] <= max_burden)
].merge(
    zip_stress[["zip_code", "neighbourhood_group_cleansed"]].drop_duplicates(),
    on="zip_code",
    how="left"
)
#Crea uno slider interattivo. 
# Quando l'utente lo muove, Streamlit riesegue tutto il file con i nuovi valori 
# — min_burden e max_burden cambiano → filtered viene ricalcolato 
# → la mappa si aggiorna.
geojson = load_geojson()
 
fig_map = px.choropleth_mapbox(
    filtered,
    geojson=geojson,
    locations="zip_code",
    featureidkey="properties.modzcta",#"per ogni riga del DataFrame, cerca nel GeoJSON il feature dove properties.modzcta corrisponde al valore di zip_code"
    color="rent_burden_pct",
    color_continuous_scale="Reds",
    range_color=(0, 150),
    mapbox_style="carto-positron",
    zoom=9,
    center={"lat": 40.7128, "lon": -74.0060},
    opacity=0.7,
    hover_data={
        "neighbourhood_group_cleansed": True,
        "zip_code": True,
        "rent_burden_pct": True,
        "median_income": True,
        "market_rent": True,
        "total_population": True
    },
    labels={
        "neighbourhood_group_cleansed": "Borough",
        "zip_code": "Zip Code",
        "rent_burden_pct": "Rent Burden %",
        "median_income": "Median Income $",
        "market_rent": "Market Rent $/mo",
        "total_population": "Population"
    }
)
fig_map.update_layout(height=600, margin={"r":0,"t":0,"l":0,"b":0})#no bordi
st.plotly_chart(fig_map, use_container_width=True) #adatta la larghezza del grafico alla larghezza della colonna in cui si trova e plotta il grafico
 
st.divider()
 
#Borough charts
col_left, col_mid, col_right = st.columns(3)
 
with col_left:
    st.subheader("Airbnb Listings per Borough")
    fig_bar = px.bar(
        borough_summary.sort_values("num_listings", ascending=True),
        x="num_listings",
        y="neighbourhood_group_cleansed",
        orientation="h",
        color="avg_occupancy_pct",
        color_continuous_scale="Oranges",
        # avg_host_listings mostrato nel tooltip: segnala la presenza di host commerciali
        hover_data={"avg_host_listings": True},
        labels={
            "num_listings": "Number of Listings",
            "neighbourhood_group_cleansed": "Borough",
            "avg_occupancy_pct": "Avg Occupancy %",
            "avg_host_listings": "Avg Listings per Host"
        }
    )
    st.plotly_chart(fig_bar, use_container_width=True)
 
with col_mid:
    st.subheader("Airbnb Pressure Index per Borough")
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
 
with col_right:
    # Entire Home %: quota di alloggi interi = unità di fatto sottratte al mercato degli affitti a lungo termine
    st.subheader("Entire-Home % per Borough")
    fig_entire = px.bar(
        borough_summary.sort_values("entire_home_pct", ascending=True),
        x="entire_home_pct",
        y="neighbourhood_group_cleansed",
        orientation="h",
        color="entire_home_pct",
        color_continuous_scale="Purples",
        range_color=(0, 100),
        hover_data={"avg_host_listings": True},
        labels={
            "entire_home_pct": "Entire-Home %",
            "neighbourhood_group_cleansed": "Borough",
            "avg_host_listings": "Avg Listings per Host"
        }
    )
    st.plotly_chart(fig_entire, use_container_width=True)
 
st.divider()
 
#Top 10 most stressed ZIP codes
st.subheader("Top 10 Most Stressed Zip Codes")
top10 = (
    zip_stress
    .dropna(subset=["rent_burden_pct"])
    .sort_values("rent_burden_pct", ascending=False)
    .head(10)[["zip_code", "neighbourhood_group_cleansed",
               "rent_burden_pct", "stress_category",
               "median_income", "market_rent",
               "num_airbnb_listings", "avg_occupancy_pct"]]
)
 
st.dataframe(
    top10.rename(columns={
        "zip_code": "ZIP Code",
        "neighbourhood_group_cleansed": "Borough",
        "rent_burden_pct": "Rent Burden %",
        "stress_category": "Stress Category",
        "median_income": "Median Income $",
        "market_rent": "Market Rent $/mo",
        "num_airbnb_listings": "Airbnb Listings",
        "avg_occupancy_pct": "Avg Occupancy %",
    }),
    use_container_width=True
)
 
st.divider()
 
#Scatter, Airbnb listings vs Rent Burden
st.subheader(" Airbnb Concentration vs Rental Stress")
 
scatter_df = zip_stress.dropna(subset=["rent_burden_pct"]).copy() #scatter_df è un oggetto separato in memoria, modificarlo non tocca zip_stress
scatter_df["avg_occupancy_pct"] = scatter_df["avg_occupancy_pct"].fillna(0)
 
fig_scatter = px.scatter(
    scatter_df,
    x="num_airbnb_listings",
    y="rent_burden_pct",
    color="neighbourhood_group_cleansed",
    size="avg_occupancy_pct",
    hover_data=["zip_code", "median_income", "market_rent"],
    labels={
        "num_airbnb_listings": "Number of Airbnb Listings",
        "rent_burden_pct": "Rent Burden %",
        "neighbourhood_group_cleansed": "Borough",
        "avg_occupancy_pct": "Avg Occupancy %",
        "zip_code": "ZIP Code",
        "median_income": "Median Income $",
        "market_rent": "Market Rent $/mo"
    }
)
fig_scatter.add_hline(
    y=30, line_dash="dash", line_color="orange",
    annotation_text="Stressed threshold (30%)"
)
fig_scatter.add_hline(
    y=50, line_dash="dash", line_color="red",
    annotation_text="Severely stressed threshold (50%)"
)
 
st.plotly_chart(fig_scatter, use_container_width=True)
 
st.markdown("**Data Sources:** Inside Airbnb · Zillow ZORI · US Census ACS 2024 · NYC Open Data (GeoJSON)")
st.markdown("**Stack:** Apache Kafka · Apache Spark · Apache Sedona · MinIO · PostgreSQL · Streamlit")
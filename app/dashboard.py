
import streamlit as st
import psycopg2 # connessione a PostgreSQL da Python
import pandas as pd
import plotly.express as px
import requests

#Page config 
st.set_page_config(
    page_title="NYC Rental Stress Observatory",
    page_icon="🏙️",
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
st.title("🏙️ NYC Rental Market Stress Observatory")
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
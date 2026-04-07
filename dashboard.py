import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import time
from config import user, password, dash_url

# Database connection
engine = create_engine(f"postgresql://{user}:{password}@{dash_url}")

# Page config
st.set_page_config(page_title="Truck Fleet Live Tracker", layout="wide")

# --- HEADER ---
st.markdown("<h1 style='text-align: center;'> Real-Time Truck Fleet Monitor (Johannesburg)</h1>", unsafe_allow_html=True)
st.markdown("---")  # horizontal line

# Placeholder for live updates
placeholder = st.empty()

while True:
    try:
        with engine.connect() as conn:
            query = """
                SELECT DISTINCT ON (truck_id) truck_id, timestamp, latitude, longitude, speed_kmh, status, driver_name, engine_condition
                FROM enriched_truck_data 
                ORDER BY truck_id, timestamp DESC 
            """
            df = pd.read_sql_query(sql=query, con=conn.connection)

        if not df.empty:
            with placeholder.container():
                # --- ROW 1: Fleet Summary Metrics ---
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Current Speed", f"{round(df['speed_kmh'].iloc[0], 1)} km/h")
                col2.metric("Active Driver", df['driver_name'].iloc[0])
                col3.metric("Avg Fleet Speed", f"{round(df['speed_kmh'].mean(), 1)} km/h")
                col4.metric("Max Speed Detected", f"{round(df['speed_kmh'].max(), 1)} km/h")

                # --- ROW 2: Map + Fleet Health Side by Side ---
                col_map, col_chart = st.columns([2, 1])

                # Map
                with col_map:
                    st.subheader("Fleet GPS Tracking")
                    map_df = df[['latitude', 'longitude']].rename(columns={'latitude':'lat', 'longitude':'lon'})
                    st.map(map_df, zoom=10, use_container_width=True)

                # Donut chart + stats
                with col_chart:
                    st.subheader("Fleet Engine Health")
                    counts = df['engine_condition'].value_counts()
                    chart_dict = {"condition": list(counts.index.map(str)), "count": list(counts.values)}
                    chart_df = pd.DataFrame(chart_dict)

                    st.vega_lite_chart(
                        chart_df,
                        {
                            'mark': {'type': 'arc', 'innerRadius': 50, 'tooltip': True},
                            'encoding': {
                                'theta': {'field': 'count', 'type': 'quantitative'},
                                'color': {'field': 'condition', 'type': 'nominal',
                                          'scale': {'range': ['#2ecc71', '#f1c40f', '#e74c3c']}}
                            },
                        },
                        use_container_width=True,
                        height=300
                    )

                    st.write("**Quick Stats:**")
                    st.write(f"Healthy: {len(df[df['engine_condition'] == 'Good'])}")
                    st.write(f"Warning: {len(df[df['engine_condition'] != 'Good'])}")

                # --- ROW 3: Expandable Logs Table ---
                with st.expander("Recent Enriched Logs (Scrollable)"):
                    st.dataframe(
                        df[['truck_id','timestamp','speed_kmh','status','driver_name','engine_condition']],
                        use_container_width=True,
                        height=350
                    )

        else:
            st.warning("Waiting for live data from Spark...")

    except Exception as e:
        st.error(f"Connection Error: {e}")

    time.sleep(1)
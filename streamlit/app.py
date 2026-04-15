import os

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery


#### === PAGE CONFIG === ####

st.set_page_config(
    page_title="UK EV Takeup Dashboard",
    layout="wide",
)


#### === BIGQUERY HELPERS === ####

@st.cache_data
def run_query(query: str) -> pd.DataFrame:
    """Run a BigQuery query and return a dataframe."""
    client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
    return client.query(query).to_dataframe()


@st.cache_data
def load_scorecards() -> pd.DataFrame:
    """Load the scorecards mart."""
    query = """
        select *
        from `uk-ev-takeup-data-pipeline.uk_ev_analytics.mart_transition_scorecards`
    """
    return run_query(query)


@st.cache_data
def load_transition_ratios() -> pd.DataFrame:
    """Load the transition ratios mart."""
    query = """
        select *
        from `uk-ev-takeup-data-pipeline.uk_ev_analytics.mart_transition_ratios_quarterly`
        order by quarter_date
    """
    return run_query(query)


@st.cache_data
def load_energy_prices() -> pd.DataFrame:
    """Load the energy prices mart."""
    query = """
        select *
        from `uk-ev-takeup-data-pipeline.uk_ev_analytics.mart_energy_prices_quarterly`
        order by quarter_date
    """
    return run_query(query)


@st.cache_data
def load_new_registrations() -> pd.DataFrame:
    """Load new vehicle registrations by fuel group."""
    query = """
        select *
        from `uk-ev-takeup-data-pipeline.uk_ev_analytics.mart_vehicle_registrations_new_by_fuel_group`
        order by quarter_date, fuel_group
    """
    return run_query(query)


@st.cache_data
def load_all_registrations() -> pd.DataFrame:
    """Load all vehicle registrations by fuel group."""
    query = """
        select *
        from `uk-ev-takeup-data-pipeline.uk_ev_analytics.mart_vehicle_registrations_all_by_fuel_group`
        order by quarter_date, fuel_group
    """
    return run_query(query)


#### === APP SETUP === ####

load_dotenv()

st.title("UK EV Takeup Dashboard")
st.caption("Initial dashboard prototype reading directly from BigQuery marts.")


#### === LOAD DATA === ####

scorecards_df = load_scorecards()
ratios_df = load_transition_ratios()
energy_df = load_energy_prices()
new_regs_df = load_new_registrations()
all_regs_df = load_all_registrations()


#### === SCORECARDS === ####

st.subheader("Five-Year Transition Scorecards")

if not scorecards_df.empty:
    row = scorecards_df.iloc[0]

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "Avg % Change: Fossil / Electricity Ratio",
        f"{row['avg_pct_change_fossil_electricity_ratio_5y']:.1f}%",
    )

    col2.metric(
        "Avg % Change: New Plugin / Fossil Ratio",
        f"{row['avg_pct_change_new_plugin_fossil_ratio_5y']:.1f}%",
    )

    col3.metric(
        "Avg % Change: All Plugin / Fossil Ratio",
        f"{row['avg_pct_change_all_plugin_fossil_ratio_5y']:.1f}%",
    )
else:
    st.warning("No scorecard data found.")


#### === TRANSITION RATIOS CHART === ####

st.subheader("Transition Ratios Over Time")

if not ratios_df.empty:
    ratios_plot_df = ratios_df.rename(
        columns={
            "fossil_electricity_ratio_pct_change": "Fossil / Electricity",
            "new_plugin_fossil_ratio_pct_change": "New Plugin / Fossil",
            "all_plugin_fossil_ratio_pct_change": "All Plugin / Fossil",
        }
    )

    fig_ratios = px.line(
        ratios_plot_df,
        x="quarter_date",
        y=[
            "Fossil / Electricity",
            "New Plugin / Fossil",
            "All Plugin / Fossil",
        ],
        labels={
            "quarter_date": "Quarter",
            "value": "% change from 2020-Q3",
            "variable": "Series",
        },
        title="Percentage change from 2020-Q3 baseline",
    )

    st.plotly_chart(fig_ratios, width="stretch")
else:
    st.warning("No transition ratio data found.")


#### === ENERGY PRICES CHART === ####

st.subheader("Energy Prices Over Time")

if not energy_df.empty:
    energy_plot_df = energy_df.rename(
        columns={
            "premium_unleaded": "Premium unleaded",
            "diesel": "Diesel",
            "electricity_price_p_kwh": "Electricity (p/kWh)",
        }
    )

    fig_energy = px.line(
        energy_plot_df,
        x="quarter_date",
        y=[
            "Premium unleaded",
            "Diesel",
            "Electricity (p/kWh)",
        ],
        labels={
            "quarter_date": "Quarter",
            "value": "Price",
            "variable": "Series",
        },
        title="Quarterly energy price series",
    )

    st.plotly_chart(fig_energy, width="stretch")
else:
    st.warning("No energy price data found.")


#### === NEW VEHICLE REGISTRATIONS AREA CHART === ####

st.subheader("New Vehicle Registrations by Fuel Group")

if not new_regs_df.empty:
    fig_new_regs = px.area(
        new_regs_df,
        x="quarter_date",
        y="registered_licenses",
        color="fuel_group",
        category_orders={"fuel_group": ["fossil", "plugin", "total"]},
        labels={
            "quarter_date": "Quarter",
            "registered_licenses": "Registered licenses",
            "fuel_group": "Fuel group",
        },
        title="Quarterly new vehicle registrations",
    )

    st.plotly_chart(fig_new_regs, width="stretch")
else:
    st.warning("No new registration data found.")


#### === ALL VEHICLE REGISTRATIONS AREA CHART === ####

st.subheader("All Vehicle Registrations by Fuel Group")

if not all_regs_df.empty:
    fig_all_regs = px.area(
        all_regs_df,
        x="quarter_date",
        y="registered_licenses",
        color="fuel_group",
        category_orders={"fuel_group": ["fossil", "plugin", "total"]},
        labels={
            "quarter_date": "Quarter",
            "registered_licenses": "Registered licenses",
            "fuel_group": "Fuel group",
        },
        title="Quarterly all vehicle registrations",
    )

    st.plotly_chart(fig_all_regs, width="stretch")
else:
    st.warning("No all-registration data found.")

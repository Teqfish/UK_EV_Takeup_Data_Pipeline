import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from google.cloud import bigquery
from plotly.subplots import make_subplots


#### === PAGE CONFIG === ####

st.set_page_config(
    page_title="UK EV Takeup Dashboard",
    page_icon="icon_car.svg",
    layout="wide",
)


#### === BIGQUERY HELPERS === ####

@st.cache_data
def run_query(query: str) -> pd.DataFrame:
    """Run a BigQuery query and return a dataframe."""
    client = bigquery.Client(project=os.environ["GCP_PROJECT_ID"])
    return client.query(query).to_dataframe()


def analytics_table(table_name: str) -> str:
    """Build a fully qualified analytics table reference from environment variables."""
    project_id = os.environ["GCP_PROJECT_ID"]
    dataset_id = os.environ.get("BQ_ANALYTICS_DATASET", "uk_ev_analytics")
    return f"`{project_id}.{dataset_id}.{table_name}`"


@st.cache_data
def load_transition_ratios() -> pd.DataFrame:
    """Load the transition ratios mart."""
    query = f"""
        select *
        from {analytics_table("mart_transition_ratios_quarterly")}
        order by quarter_date
    """
    return run_query(query)


@st.cache_data
def load_energy_prices() -> pd.DataFrame:
    """Load the energy prices mart."""
    query = f"""
        select *
        from {analytics_table("mart_energy_prices_quarterly")}
        order by quarter_date
    """
    return run_query(query)


@st.cache_data
def load_new_registrations() -> pd.DataFrame:
    """Load new vehicle registrations by fuel group."""
    query = f"""
        select *
        from {analytics_table("mart_vehicle_registrations_new_by_fuel_group")}
        order by quarter_date, fuel_group
    """
    return run_query(query)


@st.cache_data
def load_all_registrations() -> pd.DataFrame:
    """Load all vehicle registrations by fuel group."""
    query = f"""
        select *
        from {analytics_table("mart_vehicle_registrations_all_by_fuel_group")}
        order by quarter_date, fuel_group
    """
    return run_query(query)


@st.cache_data
def load_vehicle_new_detailed() -> pd.DataFrame:
    """Load detailed new vehicle fuel types from staging."""
    query = f"""
        with filtered as (
            select
                safe_cast(regexp_extract(date_label, r'(\\d{{4}})') as int64) as year_num,
                concat('Q', regexp_extract(date_label, r'Q([1-4])')) as quarter_label,
                petrol,
                diesel,
                hybrid_electric_petrol,
                hybrid_electric_diesel,
                plugin_hybrid_electric_petrol,
                plugin_hybrid_electric_diesel,
                battery_electric,
                range_extended_electric,
                fuel_cell_electric,
                gas,
                others
            from {analytics_table("stg_dvla_veh1153")}
            where geography = 'United Kingdom'
              and body_type = 'Total'
              and keepership = 'Total'
              and date_interval = 'Quarterly'
              and safe_cast(regexp_extract(date_label, r'(\\d{{4}})') as int64) >= 2015
        )

        select
            date(
                year_num,
                case
                    when quarter_label = 'Q1' then 1
                    when quarter_label = 'Q2' then 4
                    when quarter_label = 'Q3' then 7
                    when quarter_label = 'Q4' then 10
                end,
                1
            ) as quarter_date,
            petrol,
            diesel,
            hybrid_electric_petrol,
            hybrid_electric_diesel,
            plugin_hybrid_electric_petrol,
            plugin_hybrid_electric_diesel,
            battery_electric,
            range_extended_electric,
            fuel_cell_electric,
            gas,
            others
        from filtered
        where year_num is not null
          and quarter_label in ('Q1', 'Q2', 'Q3', 'Q4')
        order by quarter_date
    """
    return run_query(query)


@st.cache_data
def load_vehicle_all_detailed() -> pd.DataFrame:
    """Load detailed all-vehicle fuel types from staging."""
    query = f"""
        with filtered as (
            select
                safe_cast(regexp_extract(date_label, r'(\\d{{4}})') as int64) as year_num,
                concat('Q', regexp_extract(date_label, r'Q([1-4])')) as quarter_label,
                petrol,
                diesel,
                hybrid_electric_petrol,
                hybrid_electric_diesel,
                plugin_hybrid_electric_petrol,
                plugin_hybrid_electric_diesel,
                battery_electric,
                range_extended_electric,
                fuel_cell_electric,
                gas,
                other_fuel_types
            from {analytics_table("stg_dvla_veh1103")}
            where geography = 'United Kingdom'
              and body_type = 'Total'
              and safe_cast(regexp_extract(date_label, r'(\\d{{4}})') as int64) >= 2015
        )

        select
            date(
                year_num,
                case
                    when quarter_label = 'Q1' then 1
                    when quarter_label = 'Q2' then 4
                    when quarter_label = 'Q3' then 7
                    when quarter_label = 'Q4' then 10
                end,
                1
            ) as quarter_date,
            petrol,
            diesel,
            hybrid_electric_petrol,
            hybrid_electric_diesel,
            plugin_hybrid_electric_petrol,
            plugin_hybrid_electric_diesel,
            battery_electric,
            range_extended_electric,
            fuel_cell_electric,
            gas,
            other_fuel_types
        from filtered
        where year_num is not null
          and quarter_label in ('Q1', 'Q2', 'Q3', 'Q4')
        order by quarter_date
    """
    return run_query(query)


#### === DISPLAY HELPERS === ####

def format_pct(value: float) -> str:
    """Format percentage values for display."""
    return f"{value:.1f}%"


def format_pp_delta(current: float, start: float) -> str:
    """Format percentage-point delta for display."""
    delta = current - start
    return f"{delta:+.1f} pp vs start"


def format_pct_delta(current: float, start: float) -> str:
    """Format percentage change delta for display."""
    if start == 0:
        return "n/a vs start"
    delta = ((current / start) - 1) * 100
    return f"{delta:+.1f}% vs start"


def render_big_title(text: str) -> None:
    """Render a large bold external title above a chart/section."""
    st.markdown(
        f"""
        <div style="
            font-size: 1.45rem;
            font-weight: 700;
            line-height: 1.25;
            margin: 0.2rem 0 0.8rem 0;
        ">
            {text}
        </div>
        """,
        unsafe_allow_html=True,
    )


def build_share_lookup(df: pd.DataFrame) -> pd.DataFrame:
    """Convert long registration data to plugin share by quarter."""
    pivoted = (
        df.pivot(index="quarter_date", columns="fuel_group", values="registered_licenses")
        .reset_index()
        .rename_axis(None, axis=1)
    )

    pivoted["plugin_share_pct"] = (pivoted["plugin"] / pivoted["total"]) * 100
    return pivoted.sort_values("quarter_date").reset_index(drop=True)


def rebase_series_from_start(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Rebase selected columns so the first visible row becomes 0%."""
    rebased = df.copy()

    if rebased.empty:
        return rebased

    first_row = rebased.iloc[0]

    for col in columns:
        start_value = first_row[col]
        if pd.isna(start_value) or start_value == 0:
            rebased[col] = pd.NA
        else:
            rebased[col] = ((rebased[col] / start_value) - 1) * 100

    return rebased


def reshape_vehicle_detailed(
    df: pd.DataFrame,
    value_name: str = "registered_licenses",
) -> pd.DataFrame:
    """Convert wide detailed vehicle dataframe to long form for plotting."""
    if df.empty:
        return df

    id_vars = ["quarter_date"]
    value_vars = [col for col in df.columns if col not in id_vars]

    long_df = df.melt(
        id_vars=id_vars,
        value_vars=value_vars,
        var_name="fuel_type",
        value_name=value_name,
    )

    long_df["fuel_type"] = (
        long_df["fuel_type"]
        .str.replace("_", " ")
        .str.title()
    )

    return long_df.sort_values(["quarter_date", "fuel_type"]).reset_index(drop=True)


def remove_total_series(df: pd.DataFrame, color_col: str) -> pd.DataFrame:
    """Remove total traces from stacked area charts."""
    if df.empty:
        return df

    excluded = {"total", "Total"}
    return df[~df[color_col].isin(excluded)].copy()


def get_series_order_by_end_share(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    color_col: str,
    y_col: str,
) -> list[str]:
    """Order traces by ascending share at the end of the selected window."""
    def end_share_map(df: pd.DataFrame) -> pd.Series:
        if df.empty:
            return pd.Series(dtype=float)

        latest_date = df["quarter_date"].max()
        latest_df = df[df["quarter_date"] == latest_date].copy()

        total = latest_df[y_col].sum()
        if total == 0:
            return pd.Series(dtype=float)

        shares = latest_df.groupby(color_col)[y_col].sum() / total
        return shares

    left_shares = end_share_map(left_df)
    right_shares = end_share_map(right_df)

    combined = pd.concat([left_shares.rename("left"), right_shares.rename("right")], axis=1).fillna(0)
    combined["avg_share"] = combined.mean(axis=1)

    return combined.sort_values("avg_share", ascending=True).index.tolist()


def build_vehicle_subplot(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    color_col: str,
    y_col: str,
    left_title: str,
    right_title: str,
    x_label: str,
    y_label: str,
) -> go.Figure:
    """Build side-by-side stacked area charts with one shared legend."""
    left_df = remove_total_series(left_df, color_col)
    right_df = remove_total_series(right_df, color_col)

    fig = make_subplots(
        rows=1,
        cols=2,
        shared_yaxes=False,
        subplot_titles=(left_title, right_title),
        horizontal_spacing=0.08,
    )

    categories = get_series_order_by_end_share(
        left_df=left_df,
        right_df=right_df,
        color_col=color_col,
        y_col=y_col,
    )

    palette = px.colors.qualitative.Plotly
    color_map = {cat: palette[i % len(palette)] for i, cat in enumerate(categories)}

    for cat in categories:
        left_cat = left_df[left_df[color_col] == cat]
        right_cat = right_df[right_df[color_col] == cat]

        fig.add_trace(
            go.Scatter(
                x=left_cat["quarter_date"],
                y=left_cat[y_col],
                mode="lines",
                stackgroup="left",
                name=cat,
                legendgroup=cat,
                showlegend=True,
                line=dict(color=color_map[cat]),
            ),
            row=1,
            col=1,
        )

        fig.add_trace(
            go.Scatter(
                x=right_cat["quarter_date"],
                y=right_cat[y_col],
                mode="lines",
                stackgroup="right",
                name=cat,
                legendgroup=cat,
                showlegend=False,
                line=dict(color=color_map[cat]),
            ),
            row=1,
            col=2,
        )

    fig.update_layout(
        xaxis_title=x_label,
        xaxis2_title=x_label,
        yaxis_title=y_label,
        legend_title_text=color_col.replace("_", " ").title(),
        legend=dict(
            font=dict(size=15),
            traceorder="normal",
        ),
        margin=dict(t=60),
    )

    return fig


#### === APP SETUP === ####

load_dotenv()

st.title("UK Electric Vehicle Takeup Dashboard", width="stretch", text_alignment="center")
st.caption(
    "A dashboard to compare energy prices with vehicle registrations by fuel type in the UK",
    width="stretch",
    text_alignment="center",
)


#### === LOAD DATA === ####

ratios_df = load_transition_ratios()
energy_df = load_energy_prices()
new_regs_df = load_new_registrations()
all_regs_df = load_all_registrations()
new_detailed_df = load_vehicle_new_detailed()
all_detailed_df = load_vehicle_all_detailed()

for df in [ratios_df, energy_df, new_regs_df, all_regs_df, new_detailed_df, all_detailed_df]:
    if not df.empty and "quarter_date" in df.columns:
        df["quarter_date"] = pd.to_datetime(df["quarter_date"])


#### === DATE RANGE SELECTION === ####

available_dates = sorted(ratios_df["quarter_date"].dropna().unique()) if not ratios_df.empty else []

if available_dates:
    min_date = pd.Timestamp(available_dates[0])
    max_date = pd.Timestamp(available_dates[-1])
    default_start = max(min_date, max_date - pd.DateOffset(years=5))

    render_big_title("Date range")

    selected_range = st.slider(
        "Select quarter range",
        min_value=min_date.to_pydatetime(),
        max_value=max_date.to_pydatetime(),
        value=(default_start.to_pydatetime(), max_date.to_pydatetime()),
        format="YYYY-MM-DD",
    )

    selected_start = pd.Timestamp(selected_range[0])
    selected_end = pd.Timestamp(selected_range[1])
else:
    selected_start = None
    selected_end = None


#### === FILTER DATA TO SELECTED WINDOW === ####

def filter_to_window(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or selected_start is None or selected_end is None:
        return df.copy()

    return df[
        (df["quarter_date"] >= selected_start)
        & (df["quarter_date"] <= selected_end)
    ].copy()


ratios_filtered = filter_to_window(ratios_df)
energy_filtered = filter_to_window(energy_df)
new_regs_filtered = filter_to_window(new_regs_df)
all_regs_filtered = filter_to_window(all_regs_df)
new_detailed_filtered = filter_to_window(new_detailed_df)
all_detailed_filtered = filter_to_window(all_detailed_df)


#### === BUILD SCORECARD INPUTS === ####

new_share_df = build_share_lookup(new_regs_filtered) if not new_regs_filtered.empty else pd.DataFrame()
all_share_df = build_share_lookup(all_regs_filtered) if not all_regs_filtered.empty else pd.DataFrame()

start_ratio_row = ratios_filtered.iloc[0] if not ratios_filtered.empty else None
end_ratio_row = ratios_filtered.iloc[-1] if not ratios_filtered.empty else None

start_energy_row = energy_filtered.iloc[0] if not energy_filtered.empty else None
end_energy_row = energy_filtered.iloc[-1] if not energy_filtered.empty else None

start_new_share_row = new_share_df.iloc[0] if not new_share_df.empty else None
end_new_share_row = new_share_df.iloc[-1] if not new_share_df.empty else None

start_all_share_row = all_share_df.iloc[0] if not all_share_df.empty else None
end_all_share_row = all_share_df.iloc[-1] if not all_share_df.empty else None


#### === SCORECARDS === ####

if (
    start_ratio_row is not None
    and end_ratio_row is not None
    and start_energy_row is not None
    and end_energy_row is not None
    and start_new_share_row is not None
    and end_new_share_row is not None
    and start_all_share_row is not None
    and end_all_share_row is not None
):
    start_label = pd.to_datetime(start_ratio_row["quarter_date"]).strftime("%b %Y")
    end_label = pd.to_datetime(end_ratio_row["quarter_date"]).strftime("%b %Y")

    st.write("")

    col1, col2, col3, col4 = st.columns(4)

    new_plugin_share_start = float(start_new_share_row["plugin_share_pct"])
    new_plugin_share_end = float(end_new_share_row["plugin_share_pct"])

    all_plugin_share_start = float(start_all_share_row["plugin_share_pct"])
    all_plugin_share_end = float(end_all_share_row["plugin_share_pct"])

    electricity_price_start = float(start_energy_row["electricity_price_p_kwh"])
    electricity_price_end = float(end_energy_row["electricity_price_p_kwh"])

    fossil_electricity_ratio_start = float(start_ratio_row["fossil_electricity_ratio"])
    fossil_electricity_ratio_end = float(end_ratio_row["fossil_electricity_ratio"])

    col1.metric(
        "New plugin share",
        format_pct(new_plugin_share_end),
        format_pp_delta(new_plugin_share_end, new_plugin_share_start),
        help=f"From {format_pct(new_plugin_share_start)} to {format_pct(new_plugin_share_end)}",
    )

    col2.metric(
        "All plugin share",
        format_pct(all_plugin_share_end),
        format_pp_delta(all_plugin_share_end, all_plugin_share_start),
        help=f"From {format_pct(all_plugin_share_start)} to {format_pct(all_plugin_share_end)}",
    )

    col3.metric(
        "Electricity price",
        f"{electricity_price_end:.2f} p/kWh",
        format_pct_delta(electricity_price_end, electricity_price_start),
        help=f"From {electricity_price_start:.2f} p/kWh to {electricity_price_end:.2f} p/kWh",
    )

    col4.metric(
        "Fossil / electricity ratio",
        f"{fossil_electricity_ratio_end:.1f}",
        format_pct_delta(fossil_electricity_ratio_end, fossil_electricity_ratio_start),
        help=f"From {fossil_electricity_ratio_start:.1f} to {fossil_electricity_ratio_end:.1f}",
    )
else:
    st.warning("Not enough data found to build the scorecards.")

st.divider()


#### === TRANSITION RATIOS CHART === ####

render_big_title("Relative % change of energy prices and car registrations over time")

if not ratios_filtered.empty:
    ratios_rebased = rebase_series_from_start(
        ratios_filtered,
        columns=[
            "fossil_electricity_ratio",
            "new_plugin_fossil_ratio",
            "all_plugin_fossil_ratio",
        ],
    )

    ratios_plot_df = ratios_rebased.rename(
        columns={
            "fossil_electricity_ratio": "Fossil vs Electricity Prices",
            "new_plugin_fossil_ratio": "New Electric vs Fossil Car Registrations",
            "all_plugin_fossil_ratio": "All Electric vs Fossil Car Registrations",
        }
    )

    fig_ratios = px.line(
        ratios_plot_df,
        x="quarter_date",
        y=[
            "Fossil vs Electricity Prices",
            "New Electric vs Fossil Car Registrations",
            "All Electric vs Fossil Car Registrations",
        ],
        color_discrete_sequence=["red", "#59ffff", "#0e12ff"],
        labels={
            "quarter_date": "Quarter",
            "value": "% change from selected start",
            "variable": "Series",
        },
    )

    fig_ratios.update_layout(
        legend=dict(font=dict(size=15)),
        legend_title=dict(font=dict(size=14)),
    )

    st.plotly_chart(fig_ratios, width="stretch")
else:
    st.warning("No transition ratio data found.")


#### === ENERGY CONTROLS + CHART === ####

render_big_title("Energy prices over time")

energy_view_mode = st.radio(
    "Energy price view",
    options=["Detailed breakdown", "Aggregated summary"],
    horizontal=True,
    key="energy_view_mode",
)

if not energy_filtered.empty:
    if energy_view_mode == "Aggregated summary":
        energy_plot_df = energy_filtered.rename(
            columns={
                # "premium_unleaded": "Premium unleaded",
                # "diesel": "Diesel",
                "fossil_avg": "Fossil average",
                "electricity_price_gbp_mwhe": "Electricity (GBP/MWhe)",
            }
        )

        fig_energy = px.line(
            energy_plot_df,
            x="quarter_date",
            y=[
                # "Premium unleaded",
                # "Diesel",
                "Fossil average",
                "Electricity (GBP/MWhe)",
            ],
            color_discrete_sequence=[
                # "#00ff11",
                "#FF4800",
                "#326AE5",
                ],
            labels={
                "quarter_date": "Quarter",
                "value": "Price",
                "variable": "Series",
            },
        )
    else:
        energy_plot_df = energy_filtered.rename(
            columns={
                "premium_unleaded": "Premium unleaded",
                "diesel": "Diesel",
                "crude_oil_index": "Crude oil index",
                "electricity_price_p_kwh": "Electricity (p/kWh)",
                "electricity_price_gbp_mwhe": "Electricity (GBP/MWhe)",
                # "fossil_avg": "Fossil average",
            }
        )

        fig_energy = px.line(
            energy_plot_df,
            x="quarter_date",
            y=[
                "Premium unleaded",
                "Diesel",
                "Crude oil index",
                # "Electricity (p/kWh)",
                "Electricity (GBP/MWhe)",
                # "Fossil average",
            ],
            color_discrete_sequence=[
                "#00ff11",
                "#FF4800",
                "#EAB300",
                "#326AE5",
                "#D010DB"
                ],
            labels={
                "quarter_date": "Quarter",
                "value": "Value",
                "variable": "Series",
            },
        )

        st.caption(
            "Detailed fossil sub-series are only partially available in the current marts. This view will be widened upstream later."
        )

    fig_energy.update_layout(
        legend=dict(font=dict(size=15)),
        legend_title=dict(font=dict(size=14)),
    )

    st.plotly_chart(fig_energy, width="stretch")
else:
    st.warning("No energy price data found.")


#### === VEHICLE CONTROLS + CHARTS === ####

render_big_title("Vehicle registrations")

vehicle_view_mode = st.radio(
    "Vehicle registration view",
    options=["Detailed fuel types","Aggregated fuel groups"],
    horizontal=True,
    key="vehicle_view_mode",
)

if vehicle_view_mode == "Aggregated fuel groups":
    if not new_regs_filtered.empty and not all_regs_filtered.empty:
        vehicle_fig = build_vehicle_subplot(
            left_df=new_regs_filtered.rename(columns={"fuel_group": "series"}),
            right_df=all_regs_filtered.rename(columns={"fuel_group": "series"}),
            color_col="series",
            y_col="registered_licenses",
            left_title="New registrations",
            right_title="All registrations",
            x_label="Quarter",
            y_label="Registered licenses",
        )

        vehicle_fig.update_layout(
            legend=dict(font=dict(size=15)),
            legend_title=dict(font=dict(size=14)),
        )

        st.plotly_chart(vehicle_fig, width="stretch")
    else:
        st.warning("Not enough aggregated vehicle registration data found.")
else:
    if not new_detailed_filtered.empty and not all_detailed_filtered.empty:
        new_detailed_long = reshape_vehicle_detailed(new_detailed_filtered).rename(columns={"fuel_type": "series"})
        all_detailed_long = reshape_vehicle_detailed(all_detailed_filtered).rename(columns={"fuel_type": "series"})

        vehicle_fig = build_vehicle_subplot(
            left_df=new_detailed_long,
            right_df=all_detailed_long,
            color_col="series",
            y_col="registered_licenses",
            left_title="New registrations",
            right_title="All registrations",
            x_label="Quarter",
            y_label="Registered licenses",
        )

        vehicle_fig.update_layout(
        legend=dict(font=dict(size=15)),
        legend_title=dict(font=dict(size=14)),
    )

        st.plotly_chart(vehicle_fig, width="stretch")
    else:
        st.warning("Not enough detailed vehicle registration data found.")

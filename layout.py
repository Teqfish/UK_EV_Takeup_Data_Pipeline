import marimo

__generated_with = "0.22.0"
app = marimo.App()


@app.cell
def _():
    ### IMPORTS

    import pandas as pd
    import numpy as np
    import plotly.express as px
    import openpyxl

    from plotly.subplots import make_subplots

    return make_subplots, pd, px


@app.cell
def _(pd, px):
    ### COMPLETE - Wholesale Electricity Prices (UK)

    # Load EUR-GBP Monthly Exchange Rate
    df_eur_gbp_fx = pd.read_csv("Datasets/raw_DESNZ/Bank of England Database.csv")

    # rename columns
    df_eur_gbp_fx.rename(
        columns={
            'Spot exchange rate, Euro into Sterling              [a] [a] [a]             XUDLERS': 'GBP/EUR rate'
        },
        inplace=True
    )

    # format/change column type
    df_eur_gbp_fx["Date"] = pd.to_datetime(df_eur_gbp_fx["Date"], format="%d %b %y", errors="coerce")
    df_eur_gbp_fx["GBP/EUR rate"] = pd.to_numeric(df_eur_gbp_fx["GBP/EUR rate"], errors="coerce")

    # sort by date
    df_eur_gbp_fx.sort_values("Date",inplace=True)

    # aggregate to monthly rate
    df_eur_gbp_fx_monthly = (
        df_eur_gbp_fx
        .groupby(df_eur_gbp_fx["Date"].dt.to_period("M"), as_index=False)
        .first()
    )

    # EU Electricity prices
    df_elec = pd.read_csv("Datasets/raw_DESNZ/european_wholesale_electricity_price_data_monthly.csv")

    # df_elec["Price (GBP/MWhe)"]
    df_elec_gb = df_elec[df_elec["ISO3 Code"] == "GBR"].copy()

    # force pricing dates to first of every month by removing per day granularity and then adding it back
    df_elec_gb["Date"] = pd.to_datetime(df_elec_gb["Date"], errors="coerce")
    df_elec_gb["Date"] = df_elec_gb["Date"].dt.to_period("M").dt.to_timestamp()

    df_eur_gbp_fx_monthly["Date"] = pd.to_datetime(df_eur_gbp_fx_monthly["Date"], errors="coerce")
    df_eur_gbp_fx_monthly["Date"] = df_eur_gbp_fx_monthly["Date"].dt.to_period("M").dt.to_timestamp()

    df_elec_gb["Price (EUR/MWhe)"] = pd.to_numeric(df_elec_gb["Price (EUR/MWhe)"], errors="coerce")
    df_eur_gbp_fx_monthly["GBP/EUR rate"] = pd.to_numeric(df_eur_gbp_fx_monthly["GBP/EUR rate"], errors="coerce")

    # join electricty prices to exchange rate
    df_elec_gb_gbp = df_elec_gb.merge(
        df_eur_gbp_fx_monthly[["Date", "GBP/EUR rate"]],
        on="Date",
        how="left"
    )

    # create GBP/MWh column
    df_elec_gb_gbp["Price (GBP/MWhe)"] = (
        df_elec_gb_gbp["Price (EUR/MWhe)"] / df_elec_gb_gbp["GBP/EUR rate"]
    ).round(2)

    # convert granularity to quarterly
    df_elec_gb_gbp["Date"] = pd.to_datetime(df_elec_gb_gbp["Date"], errors="coerce")
    df_elec_gb_gbp["Price (GBP/MWhe)"] = pd.to_numeric(df_elec_gb_gbp["Price (GBP/MWhe)"], errors="coerce")

    # agg to quarter
    df_elec_q = (
        df_elec_gb_gbp
        .assign(quarter_date=df_elec_gb_gbp["Date"].dt.to_period("Q").dt.start_time)
        .groupby("quarter_date", as_index=False)["Price (GBP/MWhe)"]
        .mean()
    )

    fig_elec_q = px.line(
        df_elec_q,
        x="quarter_date",
        y="Price (GBP/MWhe)",
        title="Wholesale Electricity Prices (UK)"
    )

    fig_elec_q.show()
    return (df_elec_q,)


@app.cell
def _(pd, px):
    ### COMPLETE - Wholesale Petroleum Products Prices (UK)

    # import table_411_413__6 - Typical retail prices of petroleum products and a crude oil price index (quarterly)
    df_411 = pd.read_excel("Datasets/raw_DESNZ/table_411_413__6_.xlsx",sheet_name="4.1.1 (Quarterly)",header=9)

    # convert monthly to quarterly
    quarter_map_411 = {
        "Jan to Mar": "Q1",
        "Apr to Jun": "Q2",
        "Jul to Sep": "Q3",
        "Oct to Dec": "Q4",
    }

    df_411["quarter_nb"] = (
        df_411["Quarter"]
        .map(quarter_map_411)
        .astype("string")
    )

    df_411["quarter_date"] = (
        df_411["Year"].astype("string")
        + "-"
        + df_411["quarter_nb"]
    )

    renaming_map_411 = {
        'Motor spirit: Premium unleaded / ULSP\n(Pence per litre)\n[Note 1, 2]':'Premium Unleaded',
        'Derv: Diesel / ULSD\n(Pence per litre)\n[Note 1, 2]':'Diesel',
        'Crude oil acquired by refineries \n2010 = 100\n[Note 4] [r]':'Crude Oil'}

    df_411.rename(columns=renaming_map_411, inplace=True)

    df_411.drop(columns=[
        'Motor spirit:\n4 star / LRP\n(Pence per litre\n[Note 1]',
        'Motor spirit: Super unleaded\n(Pence per litre)\n[Note 1]',
        'Standard grade burning oil\n(Pence per litre)\n[Note 1]',
        'Gas oil\n(Pence per litre)\n[Note 1, 3]',
        'ULSP to ULSD Differential (Pence per litre)',
        'Historic Indices: Crude oil acquired by refineries\n2005 = 100',
        'Historic Indices: Crude oil acquired by refineries\n2000 = 100',
        'Historic Indices: Crude oil acquired by refineries\n1995 = 100'],
                inplace=True)

    df_411.sort_values("quarter_date", inplace=True)

    # interpolate missing data
    price_cols_411 = ["Premium Unleaded", "Diesel", "Crude Oil"]

    for col in price_cols_411:
        df_411[col] = pd.to_numeric(df_411[col], errors="coerce")
        df_411[col] = df_411[col].interpolate(method="linear")

    mask_411 = df_411['Year'] >= 2015

    fig_411 = px.line(
        df_411[mask_411],
        x = "quarter_date",
        y = [
            "Premium Unleaded",
            "Diesel",
            "Crude Oil"
            ],
        labels={"value":"Price (GBP/ltr)"},
        title="Fuel prices over time"
    )

    fig_411.show()
    return (df_411,)


@app.cell
def _(df_411, df_elec_q, pd, px):
    ### COMPLETE - FOSSIL/ELECTRICITY RATIO

    # convert fuel table quarter_date to datetime
    quarter_start_month_map_411 = {
        "Q1": "01",
        "Q2": "04",
        "Q3": "07",
        "Q4": "10",
    }

    df_411["quarter_date_dt"] = pd.to_datetime(
        df_411["quarter_date"].astype(str).str[:4]
        + "-"
        + df_411["quarter_date"].astype(str).str[-2:].map(quarter_start_month_map_411)
        + "-01",
        errors="coerce"
    )

    # convert elec prices to p/MWhe
    df_elec_q["Price (p/kWh)"] = df_elec_q["Price (GBP/MWhe)"]*100/1000

    # join GBP/EUR rate
    df_elec_fossil = df_elec_q.merge(
        df_411[["quarter_date_dt", "fossil_avg"]],
        left_on="quarter_date",
        right_on="quarter_date_dt",
        how="left"
    )

    # create ratio
    df_elec_fossil["Fossil/Electricity Ratio"] = (df_elec_fossil["fossil_avg"]/df_elec_fossil["Price (p/kWh)"]).round(4)

    fig_elec_fossil = px.line(
        df_elec_fossil,
        x="quarter_date",
        y=["Fossil/Electricity Ratio"],
        title="Ratio of Average Price of Fossil Fuels vs Electricity",
        labels={"value":"ratio"}
    )

    fig_elec_fossil.show()
    return (df_elec_fossil,)


@app.cell
def _(df_elec_fossil, px):
    ### CHECK - Convert LX/Fossil to percentage change ratio

    baseline_quarter_elec_fossil = df_elec_fossil["quarter_date"].min()

    baseline_ratio_elec_fossil = (
        df_elec_fossil.loc[
            df_elec_fossil["quarter_date"] == baseline_quarter_elec_fossil,
            "Fossil/Electricity Ratio"
        ]
        .squeeze()
    )

    df_elec_fossil["fossil_electricity_ratio_pct_change"] = (
        (df_elec_fossil["Fossil/Electricity Ratio"] / baseline_ratio_elec_fossil) - 1
    ) * 100

    fig_elec_fossil_pct = px.line(
        df_elec_fossil,
        x="quarter_date",
        y="fossil_electricity_ratio_pct_change",
        title="Percentage change in Fossil/Electricity price ratio from baseline",
        labels={
            "quarter_date": "Quarter",
            "fossil_electricity_ratio_pct_change": "% change from baseline",
        },
    )

    fig_elec_fossil_pct.update_xaxes(tickformat="%Y-Q%q")
    fig_elec_fossil_pct.show()
    return


@app.cell
def _(pd):
    # import VEH1111 - Licensed vehicles at the end of the year by year of first use
    df_1111 = pd.read_excel("Datasets/raw_DVLA/veh1111.ods",sheet_name="VEH1111",header=4)
    return (df_1111,)


@app.cell
def _(df_1111, pd):
    # split and clean date column
    df_1111b = df_1111.rename(columns={"Date": "date_label","Geography [note 1]":"geography","Unknown [note 2]":"unknown"}).copy()
    df_1111_gb = df_1111b.loc[df_1111b["geography"] == "Great Britain"].copy()
    df_1111_gb["date_label"] = (
        df_1111_gb["date_label"]
        .astype(str)
        .str.extract(r"(\d{4})\s*Q([1-4])")[0]
        + "-Q"
        + df_1111_gb["date_label"].astype(str).str.extract(r"(\d{4})\s*Q([1-4])")[1]
    )

    # extract year and quarter directly from all rows
    df_1111_gb["year"] = (
        df_1111_gb["date_label"]
        .astype(str)
        .str.extract(r"(\d{4})")[0]
    )

    df_1111_gb["quarter"] = (
        df_1111_gb["date_label"]
        .astype(str)
        .str.extract(r"Q([1-4])")[0]
    )

    # format quarter as Q1, Q2, etc.
    df_1111_gb["quarter"] = "Q" + df_1111_gb["quarter"].astype(str)

    df_1111_gb["quarter_date"] = (
        df_1111_gb["year"].astype("string") + "-" +
        df_1111_gb["quarter"].astype("string").str.zfill(2)
    )

    # cast types
    df_1111_gb["year"] = pd.to_numeric(df_1111_gb["year"], errors="coerce").astype("Int64")
    df_1111_gb["quarter"] = df_1111_gb["quarter"].astype("string")

    # cast year column names to string and strip
    df_1111_gb.columns = df_1111_gb.columns.astype(str).str.strip()

    # create list of years
    first_use_year_cols_1111 = [c for c in df_1111_gb.columns if c.isdigit()]

    # cast back to numeric
    df_1111_gb[first_use_year_cols_1111] = df_1111_gb[first_use_year_cols_1111].apply(
        pd.to_numeric,
        errors="coerce"
    )

    # refresh after fragmentation
    df_1111_gba = df_1111_gb.copy()

    # agg on bodytype and drop columns
    agg_1111 = (
        df_1111_gba
        .drop(columns=["Units", "unknown","geography"], errors="ignore")
        .groupby(["date_label", "Fuel"], as_index=False)[first_use_year_cols_1111 + ["Total"]]
        .sum()
    )

    # unpivot
    long_1111 = agg_1111.melt(
        id_vars=["date_label", "Fuel", "Total"],
        value_vars=first_use_year_cols_1111,
        var_name="first_use_year",
        value_name="licensed_count"
    )

    # add enriched columns
    long_1111["first_use_year"] = pd.to_numeric(long_1111["first_use_year"], errors="coerce").astype("Int64")
    long_1111["licensed_count"] = pd.to_numeric(long_1111["licensed_count"], errors="coerce")

    long_1111["obs_year"] = pd.to_numeric(
        long_1111["date_label"].str.extract(r"(\d{4})")[0],
        errors="coerce"
    ).astype("Int64")

    long_1111["quarter"] = long_1111["date_label"].str.extract(r"Q([1-4])")[0].astype("string")
    long_1111["quarter"] = "Q" + long_1111["quarter"]

    quarter_start_month_map_1111 = {"Q1": 1, "Q2": 4, "Q3": 7, "Q4": 10}
    long_1111["quarter_start_month"] = (
        long_1111["quarter"]
        .map(quarter_start_month_map_1111)
        .astype("Int64")
    )
    long_1111["quarter_date"] = pd.to_datetime(
        long_1111["obs_year"].astype("string")
        + "-"
        + long_1111["quarter_start_month"].astype("string").str.zfill(2)
        + "-01",
        errors="coerce"
    )

    long_1111["vehicle_age"] = long_1111["obs_year"] - long_1111["first_use_year"]

    # drop 0 count rows
    long_1111_cln= long_1111.loc[
        long_1111["licensed_count"].notna()
        & (long_1111["licensed_count"] > 0)
        & long_1111["vehicle_age"].notna()
        & (long_1111["vehicle_age"] >= 0)
    ].copy()

    # combine fuel types to broader types
    fuel_family_map_1111 = {
        "PETROL": "Fossil",
        "DIESEL": "Fossil",
        "GAS": "Fossil",

        "BATTERY ELECTRIC": "Electrified",
        "FUEL CELL ELECTRIC": "Electrified",
        "RANGE EXTENDED ELECTRIC": "Electrified",

        "PLUG-IN HYBRID ELECTRIC (PETROL)": "Electrified",
        "PLUG-IN HYBRID ELECTRIC (DIESEL)": "Electrified",
        "HYBRID ELECTRIC (PETROL)": "Electrified",
        "HYBRID ELECTRIC (DIESEL)": "Electrified",

        "OTHER FUEL TYPES":"Other Fuel Types",

        "Total":"Total"
    }

    long_1111_cln["fuel_group"] = (
        long_1111_cln["Fuel"]
        .map(fuel_family_map_1111)
        .fillna("Unmapped")
    )

    long_1111_nttl = long_1111_cln[long_1111_cln["fuel_group"]!= "Total"].copy()

    return df_1111_gb, long_1111_nttl


@app.cell
def _(df_1111_gb):
    df_1111_gb["date_label"].unique()
    return


@app.cell
def _(long_1111_nttl, px):
    # Chart 4c - ratio of new Electrified vs Fossil licensed cars in UK

    new_1111 = long_1111_nttl.loc[
        (long_1111_nttl["vehicle_age"] == 0)
        & (long_1111_nttl["fuel_group"].isin(["Fossil", "Electrified"]))
    ].copy()

    ratio_1111_new = (
        new_1111.loc[new_1111["quarter_date"] >= "2014"]
        .groupby(["quarter_date", "fuel_group"], as_index=False)["licensed_count"]
        .sum()
        .pivot(index="quarter_date", columns="fuel_group", values="licensed_count")
        .reset_index()
    )

    ratio_1111_new["electrified_fossil_ratio"] = (
        ratio_1111_new["Electrified"] / ratio_1111_new["Fossil"]
    )

    fig_1111_new_ratio = px.line(
        ratio_1111_new,
        x="quarter_date",
        y="electrified_fossil_ratio",
        title="Ratio of new Electrified to Fossil licensed cars",
        labels={
            "quarter_date": "Quarter",
            "electrified_fossil_ratio": "Electrified / Fossil ratio",
        },
    )

    fig_1111_new_ratio.update_xaxes(tickformat="%Y-Q%q")
    fig_1111_new_ratio.show()
    return (ratio_1111_new,)


@app.cell
def _(long_1111_nttl, px):
    # Chart 4d - ratio of all other Electrified vs Fossil licensed cars in UK

    other_1111 = long_1111_nttl.loc[
        (long_1111_nttl["vehicle_age"] > 0)
        & (long_1111_nttl["fuel_group"].isin(["Fossil", "Electrified"]))
    ].copy()

    ratio_1111_other = (
        other_1111.loc[other_1111["quarter_date"] >= "2014"]
        .groupby(["quarter_date", "fuel_group"], as_index=False)["licensed_count"]
        .sum()
        .pivot(index="quarter_date", columns="fuel_group", values="licensed_count")
        .reset_index()
    )

    ratio_1111_other["electrified_fossil_ratio"] = (
        ratio_1111_other["Electrified"] / ratio_1111_other["Fossil"]
    )

    fig_1111_other_ratio = px.line(
        ratio_1111_other,
        x="quarter_date",
        y="electrified_fossil_ratio",
        title="Ratio of non-new Electrified to Fossil licensed cars",
        labels={
            "quarter_date": "Quarter",
            "electrified_fossil_ratio": "Electrified / Fossil ratio",
        },
    )

    fig_1111_other_ratio.update_xaxes(tickformat="%Y-Q%q")
    fig_1111_other_ratio.show()
    return (ratio_1111_other,)


@app.cell
def _(px, ratio_1111_new):
    ### Convert 4c from ratio to percentage change

    baseline_quarter_1111_new = ratio_1111_new["quarter_date"].min()

    baseline_ratio_1111_new = (
        ratio_1111_new.loc[
            ratio_1111_new["quarter_date"] == baseline_quarter_1111_new,
            "electrified_fossil_ratio"
        ]
        .squeeze()
    )

    ratio_1111_new["electrified_fossil_ratio_pct_change"] = (
        (ratio_1111_new["electrified_fossil_ratio"] / baseline_ratio_1111_new) - 1
    ) * 100

    fig_1111_new_ratio_pct = px.line(
        ratio_1111_new,
        x="quarter_date",
        y="electrified_fossil_ratio_pct_change",
        title="Percentage change in new Electrified/Fossil car ratio from 2015 Q1 baseline",
        labels={
            "quarter_date": "Quarter",
            "electrified_fossil_ratio_pct_change": "% change from 2015 Q1",
        },
    )

    fig_1111_new_ratio_pct.update_xaxes(tickformat="%Y-Q%q")
    fig_1111_new_ratio_pct.show()
    return


@app.cell
def _(px, ratio_1111_other):
    ### Convert 4d from ratio to percentage change

    baseline_quarter_1111_other = ratio_1111_other["quarter_date"].min()

    baseline_ratio_1111_other = (
        ratio_1111_other.loc[
            ratio_1111_other["quarter_date"] == baseline_quarter_1111_other,
            "electrified_fossil_ratio"
        ]
        .squeeze()
    )

    ratio_1111_other["electrified_fossil_ratio_pct_change"] = (
        (ratio_1111_other["electrified_fossil_ratio"] / baseline_ratio_1111_other) - 1
    ) * 100

    fig_1111_other_ratio_pct = px.line(
        ratio_1111_other,
        x="quarter_date",
        y="electrified_fossil_ratio_pct_change",
        title="Percentage change in non-new Electrified/Fossil car ratio from baseline",
        labels={
            "quarter_date": "Quarter",
            "electrified_fossil_ratio_pct_change": "% change from baseline",
        },
    )

    fig_1111_other_ratio_pct.update_xaxes(tickformat="%Y-Q%q")
    fig_1111_other_ratio_pct.show()
    return


@app.cell
def _(df_elec_fossil, px, ratio_1111_new, ratio_1111_other):
    df_combined_ratios = (
        ratio_1111_new[["quarter_date", "electrified_fossil_ratio_pct_change"]]
        .rename(columns={"electrified_fossil_ratio_pct_change": "new_ratio_pct_change"})
        .merge(
            ratio_1111_other[["quarter_date", "electrified_fossil_ratio_pct_change"]]
            .rename(columns={"electrified_fossil_ratio_pct_change": "other_ratio_pct_change"}),
            on="quarter_date",
            how="inner",
        )
        .merge(
            df_elec_fossil[["quarter_date", "fossil_electricity_ratio_pct_change"]],
            on="quarter_date",
            how="inner",
        )
        .sort_values("quarter_date")
        .copy()
    )

    fig_combined_ratios = px.line(
        df_combined_ratios,
        x="quarter_date",
        y=[
            "new_ratio_pct_change",
            "other_ratio_pct_change",
            "fossil_electricity_ratio_pct_change",
        ],
        title="Percentage change from baseline: vehicle ratios vs fossil/electricity price ratio",
        labels={
            "quarter_date": "Quarter",
            "value": "% change from baseline",
            "variable": "Series",
            "new_ratio_pct_change": "New Electrified/Fossil car ratio",
            "other_ratio_pct_change": "Other Electrified/Fossil car ratio",
            "fossil_electricity_ratio_pct_change": "Fossil/Electricity price ratio",
        },
        color_discrete_map={
            "new_ratio_pct_change": "red",
            "other_ratio_pct_change": "firebrick",
            "fossil_electricity_ratio_pct_change": "blue",
        },
    )

    fig_combined_ratios.update_xaxes(tickformat="%Y-Q%q")
    fig_combined_ratios.show()
    return (df_combined_ratios,)


@app.cell
def _(df_combined_ratios, make_subplots):

    import plotly.graph_objects as go

    fig_combined_ratios_secondary = make_subplots(specs=[[{"secondary_y": True}]])

    fig_combined_ratios_secondary.add_trace(
        go.Scatter(
            x=df_combined_ratios["quarter_date"],
            y=df_combined_ratios["new_ratio_pct_change"],
            name="New Electrified/Fossil car ratio",
            line=dict(color="red"),
        ),
        secondary_y=False,
    )

    fig_combined_ratios_secondary.add_trace(
        go.Scatter(
            x=df_combined_ratios["quarter_date"],
            y=df_combined_ratios["other_ratio_pct_change"],
            name="Other Electrified/Fossil car ratio",
            line=dict(color="firebrick"),
        ),
        secondary_y=False,
    )

    fig_combined_ratios_secondary.add_trace(
        go.Scatter(
            x=df_combined_ratios["quarter_date"],
            y=df_combined_ratios["fossil_electricity_ratio_pct_change"],
            name="Fossil/Electricity price ratio",
            line=dict(color="blue"),
        ),
        secondary_y=True,
    )

    fig_combined_ratios_secondary.update_layout(
        title="Percentage change from baseline: vehicle ratios vs fossil/electricity price ratio",
        xaxis_title="Quarter",
        legend_title="Series",
    )

    fig_combined_ratios_secondary.update_xaxes(tickformat="%Y-Q%q")
    fig_combined_ratios_secondary.update_yaxes(
        title_text="% change from baseline (vehicle ratios)",
        secondary_y=False,
    )
    fig_combined_ratios_secondary.update_yaxes(
        title_text="% change from baseline (price ratio)",
        secondary_y=True,
    )

    fig_combined_ratios_secondary.show()
    return


@app.cell
def _(ratio_1111_new):
    ratio_1111_new["quarter_date"].sort_values().tolist()[:10]

    return


@app.cell
def _(ratio_1111_other):
    ratio_1111_other["quarter_date"].sort_values().tolist()[:10]

    return


@app.cell
def _(df_elec_fossil):
    df_elec_fossil["quarter_date"].sort_values().tolist()[:10]

    return


@app.cell
def _(df_combined_ratios):
    df_combined_ratios["quarter_date"].sort_values().tolist()[:10]
    return


@app.cell
def _(df_1111_gb):
    df_1111_gb["date_label"].unique()
    return


@app.cell
def _(pd):
    # COMPLETE - IMPORT VEH1103 - All licensed vehicles by fuel type quarterly
    # import VEH1103 - all licensed vehicles by fuel type
    df_1103 = pd.read_excel("Datasets/raw_DVLA/veh1103.ods",sheet_name="VEH1103a_RoadUsing",header=4)
    return (df_1103,)


@app.cell
def _(df_1103, pd, px):
    # COMPLETE - PROCESS VEH1103 - All licensed vehicles by fuel type quarterly
    # split and clean date column
    df_1103.rename(columns={"Date": "date_label","Body Type":"BodyType"}, inplace=True)
    df_1103["year"] = pd.NA
    df_1103["quarter"] = pd.NA

    # extract year and quarter directly from all rows
    df_1103["year"] = (
        df_1103["date_label"]
        .astype(str)
        .str.extract(r"(\d{4})")[0]
    )

    df_1103["quarter"] = (
        df_1103["date_label"]
        .astype(str)
        .str.extract(r"Q([1-4])")[0]
    )

    # format quarter as Q1, Q2, etc.
    df_1103["quarter"] = "Q" + df_1103["quarter"].astype(str)

    # cast types
    df_1103["year"] = pd.to_numeric(df_1103["year"], errors="coerce").astype("Int64")
    df_1103["quarter"] = df_1103["quarter"].astype("string")

    df_1103["quarter_date"] = (
        df_1103["year"].astype("string") + "-" +
        df_1103["quarter"].astype("string").str.zfill(2)
    )

    # filter for UK, all vehicles, 2015+
    mask_1103 = (df_1103["year"] >= 2015) & (df_1103["BodyType"] == "Total") & (df_1103["Geography"] == "United Kingdom")
    filtered_1103 = df_1103[mask_1103]

    # Create Fossil total
    filtered_1103["Fossil"] = filtered_1103["Total"] - filtered_1103["Plug-in"]

    # Drop columns
    filtered_1103.drop(columns=[
        "Geography",
        "BodyType",
        "Units",
        "Petrol",
        "Diesel",
        "Hybrid electric - petrol",
        "Hybrid electric - diesel",
        "Plug-in hybrid electric - petrol",
        "Plug-in hybrid electric - diesel",
        "Battery electric",
        "Range extended electric",
        "Fuel cell electric",
        "Gas",
        "Other fuel types",
        "Zero emission"
        ], inplace=True)

    fuel_cols_1103 = [
        "Total",
        "Fossil",
        "Plug-in"
    ]

    long_1103 = filtered_1103.melt(
        id_vars=["quarter_date", "year", "quarter"],
        value_vars=fuel_cols_1103,
        var_name="fuel_type",
        value_name="registered_licenses",
    )

    sum_1103 = long_1103.groupby(by=["quarter_date","fuel_type"],as_index=False).sum(numeric_only=True).sort_values(by="quarter_date")


    fig_1103 = px.line(
        sum_1103,
        x="quarter_date",
        y=(sum_1103["registered_licenses"] * 1000),
        color="fuel_type",
        title="All licensed vehicles in UK fuel type",
        labels={"y":"registrations"}
    )

    fig_1103.show()
    return


@app.cell
def _(pd):
    # COMPLETE - IMPORT VEH1153 - First-time licensed vehicles by fuel type quarterly
    df_1153 = pd.read_excel("Datasets/raw_DVLA/veh1153.ods",sheet_name="VEH1153a_RoadUsing",header=4)
    return (df_1153,)


@app.cell
def _(df_1153, pd, px):
    # COMPLETE - PROCESS VEH1153 - First-time licensed vehicles by fuel type quarterly
    # split and clean date column
    df_1153.rename(columns={"Date": "date_label","Body Type":"BodyType"}, inplace=True)
    df_1153["year"] = pd.NA
    df_1153["quarter"] = pd.NA

    # extract year and quarter directly from all rows
    df_1153["year"] = (
        df_1153["date_label"]
        .astype(str)
        .str.extract(r"(\d{4})")[0]
    )

    df_1153["quarter"] = (
        df_1153["date_label"]
        .astype(str)
        .str.extract(r"Q([1-4])")[0]
    )

    # format quarter as Q1, Q2, etc.
    df_1153["quarter"] = "Q" + df_1153["quarter"].astype(str)

    # cast types
    df_1153["year"] = pd.to_numeric(df_1153["year"], errors="coerce").astype("Int64")
    df_1153["quarter"] = df_1153["quarter"].astype("string")

    df_1153["quarter_date"] = (
        df_1153["year"].astype("string") + "-" +
        df_1153["quarter"].astype("string").str.zfill(2)
    )

    # filter for UK, all vehicles, 2015+
    mask_1153 = (df_1153["year"] >= 2015) & (df_1153["BodyType"] == "Total") & (df_1153["Geography"] == "United Kingdom") & (df_1153["Keepership"] == "Total")
    filtered_1153 = df_1153[mask_1153]

    # Create Fossil total
    filtered_1153["Fossil"] = filtered_1153["Total"] - filtered_1153["Plugin"]

    # Drop columns
    filtered_1153.drop(columns=[
        "Geography",
        "date_label",
        "BodyType",
        "Units",
        "Keepership",
        "Petrol",
        "Diesel",
        "Hybrid Electric - Petrol",
        "Hybrid Electric - Diesel",
        "Plug-in Hybrid Electric - Petrol",
        "Plug-in Hybrid Electric - Diesel",
        "Battery Electric",
        "Range Extended Electric",
        "Fuel Cell Electric",
        "Gas",
        "Others",
        "ZEV"
        ], inplace=True)

    fuel_cols_1153 = [
        "Total",
        "Fossil",
        "Plugin"
    ]

    long_1153 = filtered_1153.melt(
        id_vars=["quarter_date", "year", "quarter"],
        value_vars=fuel_cols_1153,
        var_name="fuel_type",
        value_name="registered_licenses",
    )

    sum_1153 = long_1153.groupby(by=["quarter_date","fuel_type"],as_index=False).sum(numeric_only=True).sort_values(by="quarter_date")


    fig_1153 = px.line(
        sum_1153,
        x="quarter_date",
        y=(sum_1153["registered_licenses"] * 1000),
        color="fuel_type",
        title="All licensed vehicles in UK fuel type",
        labels={"y":"registrations"}
    )

    fig_1153.show()
    return


@app.cell
def _(pd):
    # split and clean date column
    df_1153 = df_1153.rename(columns={"Date Interval": "date_interval", "Date": "date_label"})
    df_1153["year"] = pd.NA
    df_1153["quarter"] = pd.NA
    df_1153["month"] = pd.NA

    # QUARTERLY
    quarterly_mask_1153 = df_1153["date_interval"].eq("Quarterly")

    df_1153.loc[quarterly_mask_1153, "year"] = (
        df_1153.loc[quarterly_mask_1153, "date_label"]
        .astype(str)
        .str.extract(r"(\d{4})")[0]
    )

    df_1153.loc[quarterly_mask_1153, "quarter"] = (
        df_1153.loc[quarterly_mask_1153, "date_label"]
        .astype(str)
        .str.extract(r"Q([1-4])")[0]
    )

    df_1153.loc[quarterly_mask_1153, "quarter"] = (
        "Q" + df_1153.loc[quarterly_mask_1153, "quarter"].astype(str)
    )

    df_1153["quarter_date"] = (
        df_1153["year"].astype("string") + "-" +
        df_1153["quarter"].astype("string")
    )

    df_1153["year"] = pd.to_numeric(df_1153["year"], errors="coerce").astype("Int64")
    df_1153["month"] = df_1153["month"].astype("string")

    df_1153["month_num"] = pd.to_datetime(
        df_1153["month"],
        format="%B",
        errors="coerce"
    ).dt.month.astype("Int64")

    df_1153["month_date"] = pd.to_datetime(
        df_1153["year"].astype("string") + "-" +
        df_1153["month_num"].astype("string").str.zfill(2) + "-01",
        errors="coerce"
    )

    return (df_1153,)


if __name__ == "__main__":
    app.run()

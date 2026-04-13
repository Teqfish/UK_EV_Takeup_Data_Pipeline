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
    ### COMPLETE - 411 - Wholesale Petroleum Products Prices (UK)

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

    petrol_weight_0220 = 0.8059
    diesel_weight_0220 = 0.1941
    df_411["fossil_avg"] = ((df_411["Premium Unleaded"]*petrol_weight_0220) + (df_411["Diesel"])*diesel_weight_0220).round(2)

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
    return (sum_1103,)


@app.cell
def _(make_subplots, pd, px, sum_1103):
    ### COMPLETE - VEH1103 - All licensed vehicles ratio
    # pivot to wide
    ratio_1103 = (
        sum_1103
        .pivot(
            index="quarter_date",
            columns="fuel_type",
            values="registered_licenses"
        )
        .reset_index()
    )

    ratio_1103["Plug-in/Fossil"] = ratio_1103["Plug-in"]/ratio_1103["Fossil"]*100

    # convert fuel table quarter_date to datetime
    quarter_start_month_map_1103 = {
        "Q1": "01",
        "Q2": "04",
        "Q3": "07",
        "Q4": "10",
    }

    ratio_1103["quarter_date_dt"] = pd.to_datetime(
        ratio_1103["quarter_date"].astype(str).str[:4]
        + "-"
        + ratio_1103["quarter_date"].astype(str).str[-2:].map(quarter_start_month_map_1103)
        + "-01",
        errors="coerce"
    )

    subfig_1103 = make_subplots(specs=[[{"secondary_y": True}]])

    #Put Dataframe in fig1 and fig2
    fig_fuels_1103_sub = px.line(
        ratio_1103,
        x="quarter_date",
        y=["Plug-in","Fossil"],
    )

    fig_ratio_1103_sub = px.line(
        ratio_1103,
        x="quarter_date",
        y="Plug-in/Fossil",
    )
    #Change the axis for fig2
    fig_ratio_1103_sub.update_traces(yaxis="y2")

    #Add the figs to the subplot figure
    subfig_1103.add_traces(fig_fuels_1103_sub.data + fig_ratio_1103_sub.data)

    #FORMAT subplot figure
    subfig_1103.update_layout(title="New registrations by fuel type + ratio", yaxis=dict(title="New registrations"), yaxis2=dict(title="% Percent"))

    #RECOLOR so as not to have overlapping colors
    subfig_1103.for_each_trace(lambda t: t.update(line=dict(color=t.marker.color)))
    return (ratio_1103,)


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

    sum_1153 = long_1153.groupby(
        by=["quarter_date","fuel_type"],
        as_index=False
        ).sum(numeric_only=True).sort_values(by="quarter_date"
    )


    fig_1153 = px.line(
        sum_1153,
        x="quarter_date",
        y=(sum_1153["registered_licenses"] * 1000),
        color="fuel_type",
        title="All licensed vehicles in UK fuel type",
        labels={"y":"registrations"}
    )

    fig_1153.show()
    return (sum_1153,)


@app.cell
def _(make_subplots, pd, px, sum_1153):
    ### COMPLETE - VEH1153 - All licensed vehicles ratio
    # pivot to wide
    ratio_1153 = (
        sum_1153
        .pivot(
            index="quarter_date",
            columns="fuel_type",
            values="registered_licenses"
        )
        .reset_index()
    )

    ratio_1153["Plug-in/Fossil"] = ratio_1153["Plugin"]/ratio_1153["Fossil"]*100

    # convert fuel table quarter_date to datetime
    quarter_start_month_map_1153 = {
        "Q1": "01",
        "Q2": "04",
        "Q3": "07",
        "Q4": "10",
    }

    ratio_1153["quarter_date_dt"] = pd.to_datetime(
        ratio_1153["quarter_date"].astype(str).str[:4]
        + "-"
        + ratio_1153["quarter_date"].astype(str).str[-2:].map(quarter_start_month_map_1153)
        + "-01",
        errors="coerce"
    )

    subfig_1153 = make_subplots(specs=[[{"secondary_y": True}]])

    #Put Dataframe in fig1 and fig2
    fig_fuels_1153_sub = px.line(
        ratio_1153,
        x="quarter_date",
        y=["Plugin","Fossil"],
    )

    fig_ratio_1153_sub = px.line(
        ratio_1153,
        x="quarter_date",
        y="Plug-in/Fossil",
    )
    #Change the axis for fig2
    fig_ratio_1153_sub.update_traces(yaxis="y2")

    #Add the figs to the subplot figure
    subfig_1153.add_traces(fig_fuels_1153_sub.data + fig_ratio_1153_sub.data)

    #FORMAT subplot figure
    subfig_1153.update_layout(title="New registrations by fuel type + ratio", yaxis=dict(title="New registrations"), yaxis2=dict(title="% Percent"))

    #RECOLOR so as not to have overlapping colors
    subfig_1153.for_each_trace(lambda t: t.update(line=dict(color=t.marker.color)))
    return (ratio_1153,)


@app.cell
def _(df_elec_fossil, ratio_1103, ratio_1153):
    df_ratios = ratio_1153.merge(
        df_elec_fossil,
        on="quarter_date_dt",
        how="left",
    )

    df_ratios.rename(columns={"Plug-in/Fossil":"New Plug-in/Fossil"},inplace=True)

    df_all_ratios = df_ratios.merge(
        ratio_1103,
        on="quarter_date_dt",
        how="left",
    )

    df_all_ratios.rename(columns={"Plug-in/Fossil":"All Plug-in/Fossil"},inplace=True)
    return (df_all_ratios,)


@app.cell
def _(df_all_ratios, px):
    fig_all_ratios = px.line(
        df_all_ratios[df_all_ratios["quarter_date"]>="2020-Q3"],
        x="quarter_date",
        y=["fossil_electricity_ratio_pct_change","All Plug-in/Fossil","New Plug-in/Fossil"],
        labels={"value":"% change"}
    )

    fig_all_ratios.data[0].name = 'Fossil/Electricty Price'

    fig_all_ratios.show()
    return


if __name__ == "__main__":
    app.run()

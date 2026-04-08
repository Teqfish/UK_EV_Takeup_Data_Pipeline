import marimo

__generated_with = "0.22.0"
app = marimo.App()


@app.cell
def _():
    import pandas as pd
    import numpy as np
    import plotly.express as px
    import openpyxl

    from plotly.subplots import make_subplots

    return make_subplots, np, pd, px


@app.cell
def _(pd):
    # import VEH0105
    df_0105 = pd.read_excel("Datasets/raw_DVLA/veh0105.ods",sheet_name="VEH0105",header=4)
    return (df_0105,)


@app.cell
def _(df_0105):
    df_0105.columns.to_list()
    return


@app.cell
def _(df_0105):
    df_0105["Fuel"].unique()
    return


@app.cell
def _(df_0105, pd):
    # Melt back to long table
    mask_geo_0105 = (df_0105["ONS Geography"] == "United Kingdom") & (df_0105["Fuel"] != "Total") & (df_0105["BodyType"] >= "Total") & (df_0105["Keepership"] >= "Total")
    id_cols_0105 = ["Fuel", "BodyType", "Keepership"]
    time_cols_0105 = [c for c in df_0105.columns if " Q" in str(c)]

    long_0105 = df_0105[mask_geo_0105].melt(
        id_vars=id_cols_0105,
        value_vars=time_cols_0105,
        var_name="quarter",
        value_name="licensed_vehicles"
    )

    long_0105["licensed_vehicles"] = pd.to_numeric(
        long_0105["licensed_vehicles"], errors="coerce"
    )

    # Group by quarter and sum
    sum_0105 = (
        long_0105
        .groupby(["quarter","Fuel"], as_index=False)["licensed_vehicles"]
        .sum(numeric_only=True)
    )

    sum_0105["licensed_vehicles_1000"] = sum_0105["licensed_vehicles"] * 1000
    return (sum_0105,)


@app.cell
def _(px, sum_0105):
    # Plot line chart
    mask_0105 = (sum_0105["quarter"] >= "2015")

    fig_0105 = px.line(
        sum_0105[mask_0105],
        x="quarter",
        y="licensed_vehicles_1000",
        color="Fuel",
        title="Licensed Vehicles in the UK by fuel",
        labels = {'licensed_vehicles_1000': 'number of vehicles'},
        )
    fig_0105.show()
    return (fig_0105,)


@app.cell
def _(pd):
    # import VEH0142
    df_0142 = pd.read_excel("Datasets/raw_DVLA/veh0142.ods",sheet_name="VEH0142",header=4)
    return (df_0142,)


@app.cell
def _(df_0142):
    df_0142["Fuel"].unique().tolist()
    return


@app.cell
def _(df_0142, pd):
    # Melt back to long table
    mask_geo_0142 = (df_0142["ONS Geography"] == "United Kingdom") & (df_0142["Fuel"] >= "Total") & (df_0142["BodyType"] >= "Total") & (df_0142["Keepership"] >= "Total")
    id_cols_0142 = ["Fuel", "BodyType", "Keepership"]
    time_cols_0142 = [c for c in df_0142.columns if " Q" in str(c)]

    long_0142 = df_0142[mask_geo_0142].melt(
        id_vars=id_cols_0142,
        value_vars=time_cols_0142,
        var_name="quarter",
        value_name="licensed_vehicles"
    )

    long_0142["licensed_vehicles"] = pd.to_numeric(
        long_0142["licensed_vehicles"], errors="coerce"
    )

    # Group by quarter and sum
    sum_0142 = (
        long_0142
        .groupby(["quarter","Fuel"], as_index=False)["licensed_vehicles"]
        .sum(numeric_only=True)
    )
    return (sum_0142,)


@app.cell
def _(px, sum_0142):
    mask_0142 = (
        sum_0142["quarter"] >= "2015")

    # Plot line chart
    fig_0142 = px.line(
        sum_0142[mask_0142],
        x="quarter",
        y="licensed_vehicles",
        color="Fuel",
        title="Licensed PIVs in the UK by fuel"
        )
    fig_0142.show()
    return


@app.cell
def _(fig_0105):
    fig_0105.show()
    return


@app.cell
def _(pd):
    # import VEH0171
    df_0171a = pd.read_excel("Datasets/raw_DVLA/veh0171.ods",sheet_name="VEH0171a_Fuel",header=4)
    return (df_0171a,)


@app.cell
def _(df_0171a, pd):
    # split and clean date column
    df_0171 = df_0171a.rename(columns={"Date Interval": "date_interval", "Date": "date_label"})
    df_0171["year"] = pd.NA
    df_0171["quarter"] = pd.NA
    df_0171["month"] = pd.NA

    # ANNUAL
    annual_mask = df_0171["date_interval"].eq("Annual")

    df_0171.loc[annual_mask, "year"] = (
        df_0171.loc[annual_mask, "date_label"]
        .astype(str)
        .str.extract(r"(\d{4})")[0]
    )

    # MONTHLY
    monthly_mask = df_0171["date_interval"].eq("Monthly")

    df_0171.loc[monthly_mask, "month"] = (
        df_0171.loc[monthly_mask, "date_label"]
        .astype(str)
        .str.extract(r"([A-Za-z]+)")[0]
    )

    df_0171.loc[monthly_mask, "year"] = (
        df_0171.loc[monthly_mask, "date_label"]
        .astype(str)
        .str.extract(r"(\d{4})")[0]
    )

    # QUARTERLY
    quarterly_mask = df_0171["date_interval"].eq("Quarterly")

    df_0171.loc[quarterly_mask, "year"] = (
        df_0171.loc[quarterly_mask, "date_label"]
        .astype(str)
        .str.extract(r"(\d{4})")[0]
    )

    df_0171.loc[quarterly_mask, "quarter"] = (
        df_0171.loc[quarterly_mask, "date_label"]
        .astype(str)
        .str.extract(r"Q([1-4])")[0]
    )

    df_0171.loc[quarterly_mask, "quarter"] = (
        "Q" + df_0171.loc[quarterly_mask, "quarter"].astype(str)
    )

    df_0171["quarter_date"] = (
        df_0171["year"].astype("string") + "-" +
        df_0171["quarter"].astype("string")
    )

    df_0171["year"] = pd.to_numeric(df_0171["year"], errors="coerce").astype("Int64")
    df_0171["month"] = df_0171["month"].astype("string")

    df_0171["month_num"] = pd.to_datetime(
        df_0171["month"],
        format="%B",
        errors="coerce"
    ).dt.month.astype("Int64")

    df_0171["month_date"] = pd.to_datetime(
        df_0171["year"].astype("string") + "-" +
        df_0171["month_num"].astype("string").str.zfill(2) + "-01",
        errors="coerce"
    )
    return (df_0171,)


@app.cell
def _(df_0171):
    fuel_cols = [
        "Battery electric",
        "Plug-in hybrid electric - petrol",
        "Plug-in hybrid electric - diesel",
        "Range extended electric",
        "Hybrid electric - petrol",
        "Hybrid electric - diesel",
        "Fuel cell electric",
        "Other fuel types",
        "Total"
    ]

    qtr_0171 = df_0171.where(df_0171["date_interval"] == "Quarterly").copy()

    long_0171 = qtr_0171.melt(
        id_vars=["quarter_date", "year", "BodyType"],
        value_vars=fuel_cols,
        var_name="fuel_type",
        value_name="registered_licenses",
    )

    sum_0171 = long_0171.groupby(by=["quarter_date","fuel_type"],as_index=False).sum(numeric_only=True)
    sum_0171
    return (sum_0171,)


@app.cell
def _(px, sum_0171):
    fig_0171 = px.line(
        sum_0171,
        x="quarter_date",
        y="registered_licenses",
        color="fuel_type",
        title="Licensed ULEV vehicles in UK fuel type",
    )

    fig_0171.show()
    return


@app.cell
def _(pd):
    # import VEH0181 - first-time plug-in registration
    df_0181a = pd.read_excel("Datasets/raw_DVLA/veh0181.ods",sheet_name="VEH0181a_Fuel",header=4)
    return (df_0181a,)


@app.cell
def _(df_0181a, pd):
    # split and clean date column
    df_0181 = df_0181a.rename(columns={"Date Interval": "date_interval", "Date": "date_label"})
    df_0181["year"] = pd.NA
    df_0181["quarter"] = pd.NA
    df_0181["month"] = pd.NA

    # ANNUAL
    annual_0181_mask = df_0181["date_interval"].eq("Annual")

    df_0181.loc[annual_0181_mask, "year"] = (
        df_0181.loc[annual_0181_mask, "date_label"]
        .astype(str)
        .str.extract(r"(\d{4})")[0]
    )

    # MONTHLY
    monthly_0181_mask = df_0181["date_interval"].eq("Monthly")

    df_0181.loc[monthly_0181_mask, "month"] = (
        df_0181.loc[monthly_0181_mask, "date_label"]
        .astype(str)
        .str.extract(r"([A-Za-z]+)")[0]
    )

    df_0181.loc[monthly_0181_mask, "year"] = (
        df_0181.loc[monthly_0181_mask, "date_label"]
        .astype(str)
        .str.extract(r"(\d{4})")[0]
    )

    # QUARTERLY
    quarterly_0181_mask = df_0181["date_interval"].eq("Quarterly")

    df_0181.loc[quarterly_0181_mask, "year"] = (
        df_0181.loc[quarterly_0181_mask, "date_label"]
        .astype(str)
        .str.extract(r"(\d{4})")[0]
    )

    df_0181.loc[quarterly_0181_mask, "quarter"] = (
        df_0181.loc[quarterly_0181_mask, "date_label"]
        .astype(str)
        .str.extract(r"Q([1-4])")[0]
    )

    df_0181.loc[quarterly_0181_mask, "quarter"] = (
        "Q" + df_0181.loc[quarterly_0181_mask, "quarter"].astype(str)
    )

    df_0181["quarter_date"] = (
        df_0181["year"].astype("string") + "-" +
        df_0181["quarter"].astype("string")
    )

    df_0181["year"] = pd.to_numeric(df_0181["year"], errors="coerce").astype("Int64")
    df_0181["month"] = df_0181["month"].astype("string")

    df_0181["month_num"] = pd.to_datetime(
        df_0181["month"],
        format="%B",
        errors="coerce"
    ).dt.month.astype("Int64")

    df_0181["month_date"] = pd.to_datetime(
        df_0181["year"].astype("string") + "-" +
        df_0181["month_num"].astype("string").str.zfill(2) + "-01",
        errors="coerce"
    )
    return (df_0181,)


@app.cell
def _(df_0181):
    fuel_cols_0181 = [
        "Battery electric",
        "Plug-in hybrid electric - petrol",
        "Plug-in hybrid electric - diesel",
        "Range extended electric",
        "Total"
    ]

    qrt_0181 = df_0181.where(df_0181["date_interval"] == "Quarterly").copy()

    long_0181 = qrt_0181.melt(
        id_vars=["quarter_date", "year", "BodyType"],
        value_vars=fuel_cols_0181,
        var_name="fuel_type",
        value_name="registered_licenses",
    )

    sum_0181 = long_0181.groupby(by=["quarter_date","fuel_type"],as_index=False).sum(numeric_only=True)
    sum_0181
    return (sum_0181,)


@app.cell
def _(px, sum_0181):
    fig_0181 = px.line(
        sum_0181,
        x="quarter_date",
        y="registered_licenses",
        color="fuel_type",
        title="First-time plug-in vehicles in UK fuel type",
    )

    fig_0181.show()
    return


@app.cell
def _(pd):
    # import VEH1103 - all licensed vehicles by fuel type
    df_1103a = pd.read_excel("Datasets/raw_DVLA/veh1103.ods",sheet_name="VEH1103a_RoadUsing",header=4)
    return (df_1103a,)


@app.cell
def _(df_1103a, pd):
    # split and clean date column
    df_1103 = df_1103a.rename(columns={"Date": "date_label","Body Type":"BodyType"})
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
    return (filtered_1103,)


@app.cell
def _(filtered_1103):
    filtered_1103.drop(columns=[
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
    filtered_1103.columns.to_list()
    return


@app.cell
def _(filtered_1103):
    fuel_cols_1103 = [
        "Total",
        "Fossil",
        "Plug-in"
    ]

    long_1103 = filtered_1103.melt(
        id_vars=["quarter_date", "year", "quarter", "BodyType"],
        value_vars=fuel_cols_1103,
        var_name="fuel_type",
        value_name="registered_licenses",
    )

    # # combine fuel types to broader types
    # fuel_family_map_1103 = {
    #     "Petrol": "Fossil",
    #     "Diesel": "Fossil",
    #     "Gas": "Fossil",

    #     "Battery electric": "Electrified",
    #     "Fuel cell electric": "Fossil",
    #     "Range extended electric": "Electrified",

    #     "Plug-in hybrid electric - petrol": "Electrified",
    #     "Plug-in hybrid electric - diesel": "Electrified",
    #     "Hybrid electric - petrol": "Fossil",
    #     "Hybrid electric - diesel": "Fossil",

    #     "Total":"Total"
    # }

    # long_1103["fuel_group"] = (
    #     long_1103["fuel_type"]
    #     .map(fuel_family_map_1103)
    #     .fillna("Other")
    # )

    sum_1103 = long_1103.groupby(by=["quarter_date","fuel_type"],as_index=False).sum(numeric_only=True).sort_values(by="quarter_date")
    sum_1103
    return (sum_1103,)


@app.cell
def _(sum_1103):
    sum_1103["quarter_date"].unique().tolist()
    return


@app.cell
def _(px, sum_1103):
    # mask_1103b = sum_1103["quarter_date"] >= "2015"

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
    # import VEH1111 - Licensed vehicles at the end of the year by year of first use
    df_1111 = pd.read_excel("Datasets/raw_DVLA/veh1111.ods",sheet_name="VEH1111",header=4)
    return (df_1111,)


@app.cell
def _(df_1111):
    print(df_1111.Fuel.unique())
    return


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
    return (df_1111_gb,)


@app.cell
def _(df_1111_gb, pd):
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
    return agg_1111, first_use_year_cols_1111


@app.cell
def _(agg_1111, first_use_year_cols_1111):
    # unpivot
    long_1111 = agg_1111.melt(
        id_vars=["date_label", "Fuel", "Total"],
        value_vars=first_use_year_cols_1111,
        var_name="first_use_year",
        value_name="licensed_count"
    )
    long_1111.head()
    return (long_1111,)


@app.cell
def _(long_1111, pd):
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
    return (long_1111_cln,)


@app.cell
def _(long_1111):
    print(long_1111.Fuel.unique())
    return


@app.cell
def _(long_1111):
    long_1111.value_counts("Fuel")
    return


@app.cell
def _(long_1111_cln):
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



    long_1111_cln.head()
    return


@app.cell
def _(long_1111_cln):
    long_1111_nttl = long_1111_cln[long_1111_cln["fuel_group"]!= "Total"].copy()
    return (long_1111_nttl,)


@app.cell
def _(long_1111_nttl):
    # have a look at the fuel groups
    long_1111_nttl[long_1111_nttl["fuel_group"] == "Total"]
    return


@app.cell
def _(long_1111_nttl, px):
    # Chart 1: Average vehicle age by fuel type over time
    long_1111_avg_age = (
        long_1111_nttl.assign(weighted_age=lambda d: d["vehicle_age"] * d["licensed_count"])
        .groupby(["quarter_date", "fuel_group"], as_index=False)[["weighted_age", "licensed_count"]]
        .sum()
    )

    long_1111_avg_age["avg_vehicle_age"] = (
        long_1111_avg_age["weighted_age"] / long_1111_avg_age["licensed_count"]
    )

    fig_1111_avg_age = px.line(
        long_1111_avg_age,
        x="quarter_date",
        y="avg_vehicle_age",
        color="fuel_group",
        title="Average age of licensed cars in Great Britain by fuel type over time",
        labels={
            "quarter_date": "Quarter",
            "avg_vehicle_age": "Average vehicle age (years)",
            "fuel_group": "Fuel type",
        },
    )

    fig_1111_avg_age.update_xaxes(tickformat="%Y-Q%q")
    fig_1111_avg_age.show()
    return


@app.cell
def _(long_1111_nttl, pd, px):
    # Chart 2 — Share of vehicles by age band and fuel type

    # create age bands
    long_1111_nttl["age_band"] = pd.cut(
        long_1111_nttl["vehicle_age"],
        bins=[-1, 3, 7, 11, 15, 100],
        labels=["0-3 years", "4-7 years", "8-11 years", "12-15 years", "16+ years"]
    )

    # aggregate age bands
    long_1111_age_band = (
        long_1111_nttl
        .groupby(["quarter_date", "fuel_group", "age_band"], as_index=False)["licensed_count"]
        .sum()
    )

    long_1111_age_band["fuel_total_at_date"] = (
        long_1111_age_band
        .groupby(["quarter_date", "fuel_group"])["licensed_count"]
        .transform("sum")
    )

    long_1111_age_band["age_band_share"] = (
        long_1111_age_band["licensed_count"] / long_1111_age_band["fuel_total_at_date"]
    )

    fig_1111_age_band = px.area(
        long_1111_age_band,
        x="quarter_date",
        y="age_band_share",
        color="age_band",
        facet_col="fuel_group",
        facet_col_wrap=1,
        facet_row_spacing=0.01,
        height=1750,
        title="Age profile of licensed cars over time by fuel type",
        labels={
            "quarter_date": "Quarter",
            "age_band_share": "Share of each fuel fleet",
            "age_band": "Vehicle age band",
            "fuel_group": "Fuel type",
        },
    )

    fig_1111_age_band.update_xaxes(tickformat="%Y-Q%q")
    fig_1111_age_band
    return


@app.cell
def _(long_1111_nttl, px):
    # Chart 3 — Heatmap of vehicle age distribution over time
    fuel_heat_1111 = "Fossil"

    long_1111_heat = (
        long_1111_nttl.loc[long_1111_nttl["fuel_group"] == fuel_heat_1111]
        .groupby(["quarter_date", "vehicle_age"], as_index=False)["licensed_count"]
        .sum()
    )

    fig_1111_heat = px.density_heatmap(
        long_1111_heat,
        x="quarter_date",
        y="vehicle_age",
        z="licensed_count",
        histfunc="sum",
        title=f"Age distribution of licensed {fuel_heat_1111.lower()} cars over time",
        labels={
            "quarter_date": "Quarter",
            "vehicle_age": "Vehicle age (years)",
            "licensed_count": "Licensed cars",
        },
    )

    fig_1111_heat.update_xaxes(tickformat="%Y-Q%q")
    fig_1111_heat
    return


@app.cell
def _(long_1111_nttl, px):
    # Chart 4 — Share of each fuel fleet that is “new” over time

    # define "new" as 3 years or younger
    long_1111_nttl["is_new_vehicle"] = long_1111_nttl["vehicle_age"] <= 3

    df_1111_new_share = (
        long_1111_nttl
        .groupby(["quarter_date", "fuel_group"], as_index=False)
        .agg(
            total_licensed=("licensed_count", "sum"),
            new_licensed=("licensed_count", lambda s: s[long_1111_nttl.loc[s.index, "is_new_vehicle"]].sum())
        )
    )

    # calculate share of fuel groups
    df_1111_new_share["new_vehicle_share"] = (
        df_1111_new_share["new_licensed"] / df_1111_new_share["total_licensed"]
    )

    fig_1111_new_share = px.line(
        df_1111_new_share,
        x="quarter_date",
        y="new_vehicle_share",
        color="fuel_group",
        title="Share of licensed cars that are 0-3 years old by fuel type",
        labels={
            "quarter_date": "Quarter",
            "new_vehicle_share": "Share aged 0-3 years",
            "fuel_group": "Fuel type",
        },
    )

    fig_1111_new_share.update_xaxes(tickformat="%Y-Q%q")
    fig_1111_new_share.show()
    return


@app.cell
def _(long_1111_nttl):
    # Chart 4b - fuel share of all young cars in UK
    young_1111 = long_1111_nttl.loc[long_1111_nttl["vehicle_age"] <= 3].copy()

    df_1111_young_market_share = (
        young_1111
        .groupby(["quarter_date", "fuel_group"], as_index=False)["licensed_count"]
        .sum()
    )

    df_1111_young_market_share["young_total_all_fuels"] = (
        df_1111_young_market_share
        .groupby("quarter_date")["licensed_count"]
        .transform("sum")
    )

    df_1111_young_market_share["young_market_share"] = (
        df_1111_young_market_share["licensed_count"]
        / df_1111_young_market_share["young_total_all_fuels"]
    )
    return (df_1111_young_market_share,)


@app.cell
def _(df_1111_young_market_share, px):
    df_1111_young_market_share.sort_values("young_market_share",ascending=False, inplace=True)

    fig_1111_young_market_share = px.area(
        df_1111_young_market_share,
        x="quarter_date",
        y="young_market_share",
        color="fuel_group",
        title="Fuel group share of licensed cars aged 0–3 years",
        labels={
            "quarter_date": "Quarter",
            "young_market_share": "Share of all young licensed cars",
            "fuel_group": "Fuel type",
        },
    )

    fig_1111_young_market_share.update_xaxes(tickformat="%Y-Q%q")
    fig_1111_young_market_share.show()
    return


@app.cell
def _(df_1111_young_market_share):
    df_1111_young_market_share["quarter_date"].min()
    return


@app.cell
def _(long_1111_nttl, np, px):
    # Chart 5 - fuel share within older vehicle bands

    # define "old" as 8 years or older
    long_1111_nttl["older_band_flag"] = np.where(long_1111_nttl["vehicle_age"] >= 8, "8+ years old", "Under 8 years old")

    df_1111_old_mix = (
        long_1111_nttl
        .loc[long_1111_nttl["older_band_flag"] == "8+ years old"]
        .groupby(["quarter_date", "fuel_group"], as_index=False)["licensed_count"]
        .sum()
    )

    df_1111_old_mix["older_total_at_date"] = (
        df_1111_old_mix
        .groupby("quarter_date")["licensed_count"]
        .transform("sum")
    )

    df_1111_old_mix["older_fleet_share"] = (
        df_1111_old_mix["licensed_count"] / df_1111_old_mix["older_total_at_date"]
    )

    df_1111_old_mix.sort_values("older_fleet_share",ascending=False, inplace=True)

    fig_1111_old_mix = px.area(
        df_1111_old_mix,
        x="quarter_date",
        y="older_fleet_share",
        color="fuel_group",
        title="Fuel mix of licensed cars aged 8+ years in Great Britain",
        labels={
            "quarter_date": "Quarter",
            "older_fleet_share": "Share of 8+ year-old fleet",
            "fuel_group": "Fuel type",
        },
    )

    fig_1111_old_mix.update_xaxes(tickformat="%Y-Q%q")
    fig_1111_old_mix
    return


@app.cell
def _(pd):
    # import VEH1153 - Vehicles registered for the first time by body type, fuel type and keepership (private and company)
    df_1153a = pd.read_excel("Datasets/raw_DVLA/veh1153.ods",sheet_name="VEH1153b_All",header=4)
    df_1153a.head(20)
    return (df_1153a,)


@app.cell
def _(df_1153a, pd):
    # split and clean date column
    df_1153 = df_1153a.rename(columns={"Date Interval": "date_interval", "Date": "date_label"})
    df_1153["year"] = pd.NA
    df_1153["quarter"] = pd.NA
    df_1153["month"] = pd.NA

    # ANNUAL
    annual_mask_1153 = df_1153["date_interval"].eq("Annual")

    df_1153.loc[annual_mask_1153, "year"] = (
        df_1153.loc[annual_mask_1153, "date_label"]
        .astype(str)
        .str.extract(r"(\d{4})")[0]
    )

    # MONTHLY
    monthly_mask_1153 = df_1153["date_interval"].eq("Monthly")

    df_1153.loc[monthly_mask_1153, "month"] = (
        df_1153.loc[monthly_mask_1153, "date_label"]
        .astype(str)
        .str.extract(r"([A-Za-z]+)")[0]
    )

    df_1153.loc[monthly_mask_1153, "year"] = (
        df_1153.loc[monthly_mask_1153, "date_label"]
        .astype(str)
        .str.extract(r"(\d{4})")[0]
    )

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
    df_1153.head()
    return (df_1153,)


@app.cell
def _(df_1153):
    fuel_cols_1153 = [
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
        "Total"
    ]

    qtr_1153 = df_1153.where(df_1153["date_interval"] == "Quarterly").copy()

    long_1153 = qtr_1153.melt(
        id_vars=["quarter_date", "year", "BodyType"],
        value_vars=fuel_cols_1153,
        var_name="fuel_type",
        value_name="registered_licenses",
    )

    # sum_1153 = long_1153.groupby(by=["quarter_date","fuel_type"],as_index=False).sum(numeric_only=True)
    long_1153[long_1153["fuel_type"] == "Total"].head()
    return (long_1153,)


@app.cell
def _(long_1153):
    # combine fuel types to broader types
    fuel_family_map_1153 = {
        "Petrol": "Fossil",
        "Diesel": "Fossil",
        "Gas": "Fossil",

        "Battery Electric": "Electrified",
        "Fuel Cell Electric": "Electrified",
        "Range Extended Electric": "Electrified",

        "Plug-in Hybrid Electric - Petrol": "Electrified",
        "Plug-in Hybrid Electric - Diesel": "Electrified",
        "Hybrid Electric - Petrol": "Electrified",
        "Hybrid Electric - Diesel": "Electrified",

        "Total":"Total"
    }

    long_1153["fuel_group"] = (
        long_1153["fuel_type"]
        .map(fuel_family_map_1153)
        .fillna("Other")
    )

    foss_elec_mask_1153 = (long_1153["fuel_group"] != "Total") & (long_1153["fuel_group"] != "Other")
    long_1153_foss_elec = long_1153[foss_elec_mask_1153]

    sum_1153 = long_1153_foss_elec.groupby(by=["quarter_date","fuel_group"],as_index=False).sum(numeric_only=True).sort_values(by="quarter_date")
    sum_1153
    return (sum_1153,)


@app.cell
def _(sum_1153):
    # pivot to wide
    ratio_1153 = (
        sum_1153
        .pivot(
            index="quarter_date",
            columns="fuel_group",
            values="registered_licenses"
        )
        .reset_index()
    )

    ratio_1153["Electrified/Fossil"] = ratio_1153["Electrified"]/ratio_1153["Fossil"]
    ratio_1153
    return (ratio_1153,)


@app.cell
def _(make_subplots, px, ratio_1153):
    range_1153 = ratio_1153[ratio_1153["quarter_date"] >= "2015"]
    subfig_1153 = make_subplots(specs=[[{"secondary_y": True}]])

    #Put Dataframe in fig1 and fig2
    fig_fuels_1153 = px.line(
        range_1153,
        x="quarter_date",
        y=["Electrified","Fossil"],
    )

    fig_ratio_1153a = px.line(
        range_1153,
        x="quarter_date",
        y="Electrified/Fossil",
    )
    #Change the axis for fig2
    fig_ratio_1153a.update_traces(yaxis="y2")

    #Add the figs to the subplot figure
    subfig_1153.add_traces(fig_fuels_1153.data + fig_ratio_1153a.data)

    #FORMAT subplot figure
    subfig_1153.update_layout(title="New registrations by fuel type + ratio", yaxis=dict(title="New registrations"), yaxis2=dict(title="Ratio"))

    #RECOLOR so as not to have overlapping colors
    subfig_1153.for_each_trace(lambda t: t.update(line=dict(color=t.marker.color)))
    return


@app.cell
def _(px, sum_1153):
    fig_1153 = px.line(
        sum_1153,
        x="quarter_date",
        y="registered_licenses",
        color="fuel_group",
        title="Vehicles registered for the first time",
    )

    fig_1153.show()
    return


@app.cell
def _(pd):
    # import VEH9901 - Vehicles registered for the first time by body type, fuel type and keepership (private and company)
    df_9901a = pd.read_excel("Datasets/raw_DVLA/veh9901.ods",sheet_name="VEH9901",header=4)
    return (df_9901a,)


@app.cell
def _(df_9901a):
    df_9901a.columns.to_list()
    return


@app.cell
def _(df_9901a):
    agg_9901 = df_9901a.groupby(by=["Fuel","YearFirstReg"],as_index=False).sum(numeric_only=True).sort_values("YearFirstReg")
    agg_9901.head(20)
    return (agg_9901,)


@app.cell
def _(agg_9901, px):
    fig_9901 = px.line(
        agg_9901,
        x="YearFirstReg",
        y="Number",
        color="Fuel",
        title="First-time vehicles in UK by fuel type",
    )

    fig_9901.show()
    return


@app.cell
def _(pd):
    # import VEH9902- Cars and light goods vehicles registered for the first time [note 1] by body type and fuel type [note 2], United Kingdom from January 2020.
    df_9902a = pd.read_excel("Datasets/raw_DVLA/veh9902.ods",sheet_name="VEH9902a",header=4)
    return (df_9902a,)


@app.cell
def _(df_9902a, pd):
    df_9902a["year"] = (
        pd.to_numeric(
            df_9902a["Date"].astype(str).str.extract(r"(\d{4})")[0],
            errors="coerce"
        )
        .astype("Int64")
    )

    df_9902a["month"] = (
        df_9902a["Date"].astype(str).str.extract(r"^([A-Za-z]+)")[0]
        .astype("string")
    )

    df_9902a["month_date"] = pd.to_datetime(
        df_9902a["year"].astype("string")
        + "-"
        + pd.to_datetime(df_9902a["month"], format="%B", errors="coerce").dt.month.astype("Int64").astype("string").str.zfill(2)
        + "-01",
        errors="coerce"
    )

    agg_9902 = df_9902a.groupby(by=["year","month_date","Fuel Type"],as_index=False).sum(numeric_only=True).sort_values("month_date")
    agg_9902.head(20)
    return (agg_9902,)


@app.cell
def _(agg_9902, px):
    fig_9902 = px.line(
        agg_9902,
        x="month_date",
        y="Number",
        color="Fuel Type",
        title="First-time vehicles in UK by fuel type",
    )

    fig_9902.show()
    return


@app.cell
def _(pd):
    # import df_VEH0220- Cars and light goods vehicles registered for the first time [note 1] by body type and fuel type [note 2], United Kingdom from January 2020.
    df_0220a = pd.read_csv("Datasets/raw_DVLA/df_VEH0220.csv")
    df_0220a
    return (df_0220a,)


@app.cell
def _(df_0220a, pd):
    df_0220a.columns = df_0220a.columns.str.strip().to_list()

    # create list of years
    year_cols_0220 = [c for c in df_0220a.columns if c.isdigit()]

    # cast back to numeric
    df_0220a[year_cols_0220] = df_0220a[year_cols_0220].apply(
        pd.to_numeric,
        errors="coerce"
    )

    # refresh after fragmentation
    df_0220b = df_0220a.copy()

    df_0220a.head()

    # agg on bodytype and drop columns
    agg_0220 = (
        df_0220b
        .drop(columns=["BodyType", "Make","GenModel", "Model","EngineSizeSimple","EngineSizeDesc"], errors="ignore")
        .groupby(["Fuel","LicenceStatus"], as_index=False)[year_cols_0220]
        .sum()
    )

    # unpivot
    long_0220 = agg_0220.melt(
        id_vars=["Fuel","LicenceStatus"],
        value_vars=year_cols_0220,
        var_name="date",
        value_name="licensed_count"
    )

    long_0220.head(10)
    return (long_0220,)


@app.cell
def _(long_0220, px):
    agg_0220b = long_0220.groupby(by=["date","Fuel"],as_index=False).sum(numeric_only=True)

    fig_0220 = px.line(
        agg_0220b,
        x="date",
        y="licensed_count",
        color="Fuel",
        # log_y=True,
        title="Registered vehicles in UK each year"
    )

    fig_0220.show()
    return


@app.cell
def _(long_0220):
    # find average ratio of petrol to diesel between 2015-2024
    annual_0220 = (
        long_0220.loc[
            long_0220["Fuel"].isin(["PETROL", "DIESEL"])
            & (long_0220["date"].astype(str) >= "2015")
            & (long_0220["date"].astype(str) <= "2024")
        ]
        .groupby(["date", "Fuel"], as_index=False)["licensed_count"]
        .sum()
    )

    petrol_avg_annual_0220 = (
        annual_0220.loc[annual_0220["Fuel"] == "PETROL", "licensed_count"]
        .mean()
    )

    diesel_avg_annual_0220 = (
        annual_0220.loc[annual_0220["Fuel"] == "DIESEL", "licensed_count"]
        .mean()
    )

    petrol_diesel_ratio_0220 = petrol_avg_annual_0220 / diesel_avg_annual_0220
    petrol_weight_0220 = (1/petrol_diesel_ratio_0220).round(4)
    diesel_weight_0220 = 1-(1/petrol_diesel_ratio_0220).round(4)

    print(petrol_avg_annual_0220)
    print(diesel_avg_annual_0220)
    print(petrol_diesel_ratio_0220.round(2))
    print(f"Petrol = {(1/petrol_diesel_ratio_0220).round(2)*100}%")
    print(f"Diesel = {100-(1/petrol_diesel_ratio_0220).round(2)*100}%")
    print(petrol_weight_0220)
    print(diesel_weight_0220)
    return diesel_weight_0220, petrol_weight_0220


@app.cell
def _(long_0220):
    long_0220.loc[long_0220["Fuel"] == "PETROL", "licensed_count"]
    return


@app.cell
def _(pd):
    # import table_411_413__6 - Typical retail prices of petroleum products and a crude oil price index (quarterly)
    df_411 = pd.read_excel("Datasets/raw_DESNZ/table_411_413__6_.xlsx",sheet_name="4.1.1 (Quarterly)",header=9)
    return (df_411,)


@app.cell
def _(df_411):
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
    return


@app.cell
def _(df_411, pd):
    price_cols_411 = ["Premium Unleaded", "Diesel", "Crude Oil"]

    df_411.sort_values("quarter_date", inplace=True)

    # interpolate missing data
    for col in price_cols_411:
        df_411[col] = pd.to_numeric(df_411[col], errors="coerce")
        df_411[col] = df_411[col].interpolate(method="linear")

    return


@app.cell
def _(df_411):
    renaming_map_411 = {'Motor spirit: Premium unleaded / ULSP\n(Pence per litre)\n[Note 1, 2]':'Premium Unleaded',
    'Derv: Diesel / ULSD\n(Pence per litre)\n[Note 1, 2]':'Diesel',
    'Crude oil acquired by refineries \n2010 = 100\n[Note 4] [r]':'Crude Oil'}

    df_411.rename(columns=renaming_map_411,inplace=True)
    df_411.head()
    return


@app.cell
def _(df_411, px):
    fig_411 = px.line(
        df_411,
        x = "quarter_date",
        y = ["Premium Unleaded",
            "Diesel",
            "Crude Oil"
            ],
        labels={"value":"Price (GBP/ltr)"},
        title="Fuel prices over time"
    )

    fig_411.show()
    return


@app.cell
def _(df_411, diesel_weight_0220, petrol_weight_0220):
    # average diesel/petrol prices and create index from 2015 q1
    df_411["fossil_avg"] = ((df_411["Premium Unleaded"]*petrol_weight_0220) + (df_411["Diesel"])*diesel_weight_0220).round(2)
    df_411.head()
    return


@app.cell
def _(pd):
    # UK Electricity system prices (p/kWh)
    df_elec_uk = pd.read_excel("Datasets/raw_DESNZ/electricitypricesdataset181225.xlsx",sheet_name="2.Monthly SP Electricity",header=4)
    return (df_elec_uk,)


@app.cell
def _(df_elec_uk):
    df_elec_uk.head()
    return


@app.cell
def _(df_elec_uk):
    df_elec_uk_q = (
        df_elec_uk
        .assign(quarter_date=df_elec_uk["Month"].dt.to_period("Q").dt.start_time)
        .groupby("quarter_date", as_index=False)["Monthly average"]
        .mean()
        .rename(columns={"Monthly average": "Quarterly average"})
    )
    return


@app.cell
def _(df_elec_uk, px):
    fig_elec_uk = px.line(
        df_elec_uk,
        x="Month",
        y="Monthly average"
    )

    fig_elec_uk.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ^^^ The above data doesn't go back far enough.  We want 2015 onwards.
    """)
    return


@app.cell
def _(pd):
    # EU Electricity prices
    df_elec = pd.read_csv("Datasets/raw_DESNZ/european_wholesale_electricity_price_data_monthly.csv")
    return (df_elec,)


@app.cell
def _(df_elec):
    # df_elec["Price (GBP/MWhe)"]
    df_elec_gb = df_elec[df_elec["ISO3 Code"] == "GBR"].copy()
    df_elec_gb.head()
    return (df_elec_gb,)


@app.cell
def _(df_elec_gb, px):
    fig_elec_gb = px.line(
        df_elec_gb,
        x="Date",
        y="Price (EUR/MWhe)"
    )

    fig_elec_gb.show()
    return


@app.cell
def _(pd):
    # GBP/EUR exchange rate
    df_gbp_eur = pd.read_csv("Datasets/raw_DESNZ/Bank of England Database.csv")
    return (df_gbp_eur,)


@app.cell
def _(df_gbp_eur, pd):
    df_gbp_eur.rename(
        columns={
            'Spot exchange rate, Euro into Sterling              [a] [a] [a]             XUDLERS': 'GBP/EUR rate'
        },
        inplace=True
    )

    df_gbp_eur["Date"] = pd.to_datetime(df_gbp_eur["Date"], format="%d %b %y", errors="coerce")
    df_gbp_eur["GBP/EUR rate"] = pd.to_numeric(df_gbp_eur["GBP/EUR rate"], errors="coerce")

    df_gbp_eur.sort_values("Date",inplace=True)

    df_gbp_eur_monthly = (
        df_gbp_eur
        .groupby(df_gbp_eur["Date"].dt.to_period("M"), as_index=False)
        .first()
    )

    df_gbp_eur_monthly.head()
    return (df_gbp_eur_monthly,)


@app.cell
def _(df_elec_gb, df_gbp_eur_monthly, pd):
    # force pricing dates to first of every month
    # remove per day granularity and then add it back
    df_elec_gb["Date"] = pd.to_datetime(df_elec_gb["Date"], errors="coerce")
    df_elec_gb["Date"] = df_elec_gb["Date"].dt.to_period("M").dt.to_timestamp()

    df_gbp_eur_monthly["Date"] = pd.to_datetime(df_gbp_eur_monthly["Date"], errors="coerce")
    df_gbp_eur_monthly["Date"] = df_gbp_eur_monthly["Date"].dt.to_period("M").dt.to_timestamp()

    df_elec_gb["Price (EUR/MWhe)"] = pd.to_numeric(df_elec_gb["Price (EUR/MWhe)"], errors="coerce")
    df_gbp_eur_monthly["GBP/EUR rate"] = pd.to_numeric(df_gbp_eur_monthly["GBP/EUR rate"], errors="coerce")

    # join GBP/EUR rate
    df_elec_gb_gbp = df_elec_gb.merge(
        df_gbp_eur_monthly[["Date", "GBP/EUR rate"]],
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

    df_elec_q.head()
    return (df_elec_q,)


@app.cell
def _(df_elec_q, px):
    fig_elec_q = px.line(
        df_elec_q,
        x="quarter_date",
        y="Price (GBP/MWhe)",
        title="Price of electricity in UK"
    )

    fig_elec_q.show()
    return


@app.cell
def _(df_411, df_elec_q, pd, px):
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

    df_elec_fossil["Fossil/Electricity Ratio"] = (df_elec_fossil["fossil_avg"]/df_elec_fossil["Price (p/kWh)"]).round(4)

    df_elec_fossil.head()

    fig_elec_fossil = px.line(
        df_elec_fossil,
        x="quarter_date",
        y=["Fossil/Electricity Ratio"]
    )

    fig_elec_fossil.show()
    return (df_elec_fossil,)


@app.cell
def _(ratio_1153):
    ratio_1153.columns
    return


@app.cell
def _(pd, px, ratio_1153):
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

    fig_ratio_1153 = px.line(
        ratio_1153,
        x="quarter_date_dt",
        y="Electrified/Fossil"
    )

    fig_ratio_1153.show()
    return


@app.cell
def _(df_elec_fossil, ratio_1153):
    baseline_ratio_elec_fossil = df_elec_fossil.loc[
        df_elec_fossil["quarter_date"] == "2016-Q3",
        "Fossil/Electricity Ratio"
    ].iloc[0]

    df_elec_fossil["Fossil/Electricity Ratio Index"] = (
        ((df_elec_fossil["Fossil/Electricity Ratio"] / baseline_ratio_elec_fossil) -1) * 100
    )

    baseline_ratio_1153 = ratio_1153.loc[
        ratio_1153["quarter_date"] == "2016-Q3",
        "Electrified/Fossil"
    ].iloc[0]

    ratio_1153["electrified_fossil_ratio_index"] = (
        ((ratio_1153["Electrified/Fossil"] / baseline_ratio_1153) -1) * 100
    )

    df_ratios = ratio_1153.merge(
        df_elec_fossil,
        on="quarter_date_dt",
        how="left",
    )
    return baseline_ratio_1153, baseline_ratio_elec_fossil, df_ratios


@app.cell
def _(df_ratios, px):
    df_ratios_2015_2025_mask = (df_ratios["quarter_date_x"] >= '2016-Q3') & (df_ratios["quarter_date_x"] <= '2025-Q3')
    df_ratios_2015_2025 = df_ratios[df_ratios_2015_2025_mask]
    fig_ratios = px.line(
        df_ratios_2015_2025,
        x="quarter_date_x",
        y=["electrified_fossil_ratio_index","Fossil/Electricity Ratio Index"],
        labels = {
            "quarter_date_x":"Date",
            "value":"Index",
            "electrified_fossil_ratio_index":"Electrified/Fossil Car Ratio Index",
            "Fossil/Electricity Ratio Index":"Fossil/Electricity Price Ratio Index"
            }
    )
    fig_ratios.show()
    return


@app.cell
def _(baseline_ratio_1153, baseline_ratio_elec_fossil, df_ratios):
    df_ratios["electrified_fossil_ratio_pct_change"] = (
        (df_ratios["Electrified/Fossil"] / baseline_ratio_elec_fossil) - 1
    ) * 100

    df_ratios["fossil_electricity_ratio_pct_change"] = (
        (df_ratios["Fossil/Electricity Ratio"] / baseline_ratio_1153) - 1
    ) * 100
    return


@app.cell
def _(df_ratios, px):
    df_ratios_2015_2025_maskb = (df_ratios["quarter_date_x"] >= '2016-Q3') & (df_ratios["quarter_date_x"] <= '2025-Q3')
    df_ratios_2015_2025b = df_ratios[df_ratios_2015_2025_maskb]

    fig_compare = px.line(
        df_ratios_2015_2025b,
        x="quarter_date_x",
        y=[
            "electrified_fossil_ratio_pct_change",
            "fossil_electricity_ratio_pct_change",
        ],
        title="Change from 2016 Q3 baseline: electrified/fossil registrations vs fossil/electricity price ratio",
        labels={
            "value": "% change from baseline",
            "quarter_date_x": "Date",
            "variable": "Series",
        },
    )

    fig_compare.show()
    return


@app.cell
def _(sum_1153):
    # compare 1153 and 1111
    # 1153 - new registrations by fuel type
    # 1111 - all registrations per year by first registration
    sum_1153.head()
    return


@app.cell
def _(long_1111_nttl):
    # Chart 4c - fuel share of all new cars in UK
    new_1111 = long_1111_nttl.loc[long_1111_nttl["vehicle_age"] <= 0].copy()

    df_1111_new_market_share = (
        new_1111[new_1111["quarter_date"] >= "2014"]
        .groupby(["quarter_date", "fuel_group"], as_index=False)["licensed_count"]
        .sum()
    )

    df_1111_new_market_share["new_total_all_fuels"] = (
        df_1111_new_market_share
        .groupby("quarter_date")["licensed_count"]
        .transform("sum")
    )

    df_1111_new_market_share["new_market_share"] = (
        df_1111_new_market_share["licensed_count"]
        / df_1111_new_market_share["new_total_all_fuels"]
    )
    return (df_1111_new_market_share,)


@app.cell
def _(df_1111_new_market_share, px):
    df_1111_new_market_share.sort_values("new_market_share",ascending=False, inplace=True)

    fig_1111_new_market_share = px.area(
        df_1111_new_market_share,
        x="quarter_date",
        y="new_market_share",
        color="fuel_group",
        title="Fuel group share of new licensed cars ",
        labels={
            "quarter_date": "Quarter",
            "new_market_share": "Share of all new licensed cars",
            "fuel_group": "Fuel type",
        },
    )

    fig_1111_new_market_share.update_xaxes(tickformat="%Y-Q%q")
    fig_1111_new_market_share.show()
    return


@app.cell
def _(long_1111_nttl):
    # Chart 4d - fuel share of all new cars in UK
    all_1111 = long_1111_nttl.loc[long_1111_nttl["vehicle_age"] >= 0].copy()

    df_1111_all_market_share = (
        all_1111[all_1111["quarter_date"] >= "2014"]
        .groupby(["quarter_date", "fuel_group"], as_index=False)["licensed_count"]
        .sum()
    )

    df_1111_all_market_share["new_total_all_fuels"] = (
        df_1111_all_market_share
        .groupby("quarter_date")["licensed_count"]
        .transform("sum")
    )

    df_1111_all_market_share["new_market_share"] = (
        df_1111_all_market_share["licensed_count"]
        / df_1111_all_market_share["new_total_all_fuels"]
    )
    return (df_1111_all_market_share,)


@app.cell
def _(df_1111_all_market_share, px):
    df_1111_all_market_share.sort_values("new_market_share",ascending=False, inplace=True)

    fig_1111_all_market_share = px.area(
        df_1111_all_market_share,
        x="quarter_date",
        y="new_market_share",
        color="fuel_group",
        title="Fuel group share of all licensed cars ",
        labels={
            "quarter_date": "Quarter",
            "new_market_share": "Share of all licensed cars",
            "fuel_group": "Fuel type",
        },
    )

    fig_1111_all_market_share.update_xaxes(tickformat="%Y-Q%q")
    fig_1111_all_market_share.show()
    return


if __name__ == "__main__":
    app.run()

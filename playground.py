import marimo

__generated_with = "0.23.1"
app = marimo.App()


@app.cell
def _():
    import pandas as pd
    df = pd.read_parquet("/Users/admin/Downloads/prepared_bank_of_england_eur_gbp_fx_prepared_bank_of_england_eur_gbp_fx.parquet")
    df.head()
    return


if __name__ == "__main__":
    app.run()

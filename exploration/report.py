import pathlib

import pandas as pd
from pandas_profiling import ProfileReport


def main():
    input_file = (
        pathlib.Path(__file__).parents[1] / "data" / "input" / "pp-complete.csv"
    )
    with input_file.open() as csv:
        df_report = pd.read_csv(
            csv,
            names=[
                "transaction_id",
                "price",
                "date_of_transfer",
                "postcode",
                "property_type",
                "old_new",
                "duration",
                "paon",
                "saon",
                "street",
                "locality",
                "town_city",
                "district",
                "county",
                "ppd_category",
                "record_status",
            ],
        )
    profile = ProfileReport(df_report, title="Price Paid Data", minimal=True)
    profile.to_file("price_paid_data_report.html")


if __name__ == "__main__":
    main()

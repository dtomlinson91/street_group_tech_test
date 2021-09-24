from importlib import resources

import pandas as pd
from pandas_profiling import ProfileReport


def main():
    with resources.path("analyse_properties.data", "pp-complete.csv") as csv_file:
        df_report = pd.read_csv(
            csv_file,
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

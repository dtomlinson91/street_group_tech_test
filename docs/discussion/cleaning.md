# Cleaning

In this page we discuss the cleaning stages and how best to prepare the data.

## Uniquely identify a property.

To uniquely identify a property with the data we have it is enough to have a Postcode and the PAON (or SAON or combination of both).

### Postcode

Because so few properties are missing a postcode (0.2% of all records) we will drop all rows that do not have one. We will drop some properties that could be identified uniquely with some more work, but the properties that are missing a postcode tend to be unusual/commercial/industrial (e.g a powerplant).

### PAON/SAON

The PAON has 3 possible formats:

- The street number.
- The building name.
- The building name and street number (comma delimited).

The SAON:

- Identifies the appartment/flat number for the building.
- If the SAON is present (only 11.7% of values) then the PAON will either be
    - The building name.
    - The building name and street number.

Because of the way the PAON and SOAN are defined, if any row is missing **both** of these columns we will drop it. As only having the postcode is not enough (generally speaking) to uniquely identify a property.

!!! tip
    In a production environment we could send these rows to a sink table (in BigQuery for example), rather than drop them outright. Collecting these rows over time might show some patterns on how we can uniquely identify properties that are missing these fields.

We split the PAON as part of the cleaning stage. If the PAON contains a comma then it contains the building name and street number. We keep the street number in the same position as the PAON and insert the building name as a new column at the end of the row. If the PAON does not contain a comma we insert a blank column at the end to keep the number of columns in the PCollection consistent.

### Unneeded columns

To try keep computation costs/time down, I decided to drop the categorical columns provided. These include:

- Property Type.
- Old/New.
- Duration.
- PPD Category Type.
- Record Status - monthly file only.

Initially I was attempting to work against the full dataset so dropping these columns would make a difference in the amount of data that needs processing.

These columns are also not consistent. E.g the property `63` `B16, 0AE` has three transactions. Two of these transactions have a property type of `Other` and one transaction has a property type of `Terraced`.

These columns do provide some relevant information (old/new, duration, property type) and these could be included back into the pipeline fairly easily. Due to time constraints I was unable to make this change.

In addition, I also dropped the transaction unique identifier column. I wanted the IDs calculated in the pipeline to be consistent in format, and hashing a string (md5) isn't that expensive to calculate with complexity $\mathcal{O}(n)$.

### General cleaning

#### Upper case

As all strings in the dataset are upper case, we convert everything in the row to upper case to enforce consistency across the dataset.

#### Strip leading/trailing whitespace

We strip all leading/trailing whitespace from each column to enforce consistency.

#### Repeated rows

Some of the data is repeated:

- Some rows are repeated, with the same date + price + address information but with a unique transaction id.

<details>
    <summary>Example (PCollection)</summary>

```json
[
  {
    "fd4634faec47c29de40bbf7840723b41": [
      "317500",
      "2020-11-13 00:00",
      "B90 3LA",
      "1",
      "",
      "VERSTONE ROAD",
      "SHIRLEY",
      "SOLIHULL",
      "SOLIHULL",
      "WEST MIDLANDS",
      ""
    ]
  },
  {
    "gd4634faec47c29de40bbf7840723b42": [
      "317500",
      "2020-11-13 00:00",
      "B90 3LA",
      "1",
      "",
      "VERSTONE ROAD",
      "SHIRLEY",
      "SOLIHULL",
      "SOLIHULL",
      "WEST MIDLANDS",
      ""
    ]
  }
]
```

</details>

These rows will be deduplicated as part of the pipeline.

- Some rows have the same date + address information, but different prices.

It would be very unusual to see multiple transactions on the same date for the same property. One reason could be that there was a data entry error, resulting in two different transactions with only one being the real price. As the date column does not contain the time (it is fixed at `00:00`) it is impossible to tell.

Another reason could be missing building/flat/appartment information in this entry.

We **keep** these in the data, resulting in some properties having multiple transactions with different prices on the same date. Without a time or more information to go on, it is difficult to see how these could be filtered out.

<details>
    <summary>Example (Output)</summary>

```json
[
  {
    "property_id": "20d5c335c8d822a40baab0ecd57e92a4",
    "readable_address": "53 PAVENHAM DRIVE\nBIRMINGHAM\nWEST MIDLANDS\nB5 7TN",
    "flat_appartment": "",
    "builing": "",
    "number": "53",
    "street": "PAVENHAM DRIVE",
    "locality": "",
    "town": "BIRMINGHAM",
    "district": "BIRMINGHAM",
    "county": "WEST MIDLANDS",
    "postcode": "B5 7TN",
    "property_transactions": [
      {
        "price": 270000,
        "transaction_date": "2020-04-23",
        "year": 2020
      },
      {
        "price": 364000,
        "transaction_date": "2020-04-23",
        "year": 2020
      }
    ],
    "latest_transaction_year": 2020
  }
]

```

</details>

# Results

The resulting output `.json` looks like (for the previous example using No. 1 `B90 3LA`):

```json
[
  {
    "property_id": "fe205bfe66bc7f18c50c8f3d77ec3e30",
    "readable_address": "1 VERSTONE ROAD\nSHIRLEY\nSOLIHULL\nWEST MIDLANDS\nB90 3LA",
    "flat_appartment": "",
    "builing": "",
    "number": "1",
    "street": "VERSTONE ROAD",
    "locality": "SHIRLEY",
    "town": "SOLIHULL",
    "district": "SOLIHULL",
    "county": "WEST MIDLANDS",
    "postcode": "B90 3LA",
    "property_transactions": [
      {
        "price": 317500,
        "transaction_date": "2020-11-13",
        "year": 2020
      }
    ],
    "latest_transaction_year": 2020
  }
]
```

The standard property information is included, we will briefly discuss the additional fields included in this output file.

## readable_address

The components that make up the address in the dataset are often repetitive, with the locality, town/city, district and county often sharing the same result. This can result in hard to read addresses if we just stacked all the components sequentially.

The `readable_address` provides an easy to read address that strips this repetiveness out, by doing pairwise comparisons to each of the four components and applying a mask. The result is an address that could be served to the end user, or easily displayed on a page.

This saves any user having to apply the same logic to simply display the address somewhere, the full address of a property should be easy to read and easily accessible.

## property_transactions

This array contains an object for each transaction for that property that has the price and year as an `int`, with the date having the `00:00` time stripped out.

## latest_transaction_year

The date of the latest transaction is extracted from the array of `property_transactions` and placed in the top level of the `json` object. This allows any end user to easily search for properties that haven't been sold in a period of time, without having to write this logic themselves.

A consumer should be able to use this data to answer questions like:

- Give me all properties in the town of Solihull that haven't been sold in the past 10 years.

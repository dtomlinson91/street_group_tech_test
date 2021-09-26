from datetime import datetime
import hashlib
import itertools
import pathlib

import apache_beam as beam

# from analyse_properties.debug import DebugShowEmptyColumn, DebugShowColumnWithValueIn


def slice_by_range(element, *ranges):
    """Slice a list with multiple ranges."""
    return itertools.chain(*(itertools.islice(element, *r) for r in ranges))


class DropRecordsSingleEmptyColumn(beam.DoFn):
    """If a given item in a list is empty, drop this entry from the PCollection."""

    def __init__(self, index):
        self.index = index

    def process(self, element):
        column = element[self.index]
        if len(column) == 0:
            return None
        yield element


class DropRecordsTwoEmptyColumn(beam.DoFn):
    """If two given items in a list are both empty, drop this entry from the PCollection."""

    def __init__(self, index_0, index_1):
        self.index_0 = index_0
        self.index_1 = index_1

    def process(self, element):
        column_0 = element[self.index_0]
        column_1 = element[self.index_1]
        if len(column_0) == 0 and len(column_1) == 0:
            return None
        yield element


class SplitColumn(beam.DoFn):
    """Split an item in a list into two separate items in the PCollection."""

    def __init__(self, index, split_char):
        self.index = index
        self.split_char = split_char

    def process(self, element):
        # If there is a split based on the split_char, then keep the first result in
        # place and append the second.
        try:
            part_0, part_1 = element[self.index].split(self.split_char)
            element[self.index] = part_1.strip()
            element.append(part_0.strip())
            yield element
        except ValueError:
            element.append("")
            yield element


class GenerateUniqueID(beam.DoFn):
    """
    Generate a unique ID for the PCollection, either for all the columns or for the
    uniquely identifying data only.
    """

    def __init__(self, all_columns=False):
        self.all_columns = all_columns

    def process(self, element):
        unique_string = (
            ",".join(element[2:]) if not self.all_columns else ",".join(element)
        )
        hashed_string = hashlib.md5(unique_string.encode())
        # append the hash to the end
        element.append(hashed_string.hexdigest())
        yield element


class DeduplicateByID(beam.DoFn):
    """
    If the PCollection has multiple entries after being grouped by ID for all columns,
    deduplicate the list to keep only one.
    """

    def process(self, element):
        if len(element[1]) > 0:
            deduplicated_element = (element[0], [element[1][0]])
            yield deduplicated_element
        else:
            yield element


class RemoveUniqueID(beam.DoFn):
    """Remove the unique ID from the PCollection, transforming it back into a list."""

    def process(self, element):
        element_no_id = element[-1][0]
        element_no_id.pop(-1)
        yield element_no_id


class ConvertDataToDict(beam.DoFn):
    """Convert the processed data into a dict to be exported as a JSON object."""

    @staticmethod
    def get_latest_transaction(transaction_dates):
        """Get the date of the latest transaction."""
        transaction_dates = [
            datetime.strptime(individual_transaction, "%Y-%m-%d")
            for individual_transaction in transaction_dates
        ]
        return max(transaction_dates).strftime("%Y")

    @staticmethod
    def get_readable_address(address_components: list, address_comparisons: list):
        # Get pairwise comparison to see if two locality/town/district/counties
        # are equivalent
        pairwise_comparison = [
            x == y
            for i, x in enumerate(address_comparisons)
            for j, y in enumerate(address_comparisons)
            if i > j
        ]
        # Create a mask to eliminate the redundant parts of the address
        mask = [True, True, True, True]
        if pairwise_comparison[0]:
            mask[1] = False
        if pairwise_comparison[1] or pairwise_comparison[2]:
            mask[2] = False
        if pairwise_comparison[3] or pairwise_comparison[4] or pairwise_comparison[5]:
            mask[3] = False
        # Apply the mask
        applied_mask = list(itertools.compress(address_comparisons, mask))
        # Filter out empty items in list
        deduplicated_address_part = list(filter(None, applied_mask))

        # Filter out any missing parts of the address components
        cleaned_address_components = list(filter(None, address_components))

        # Return the readable address
        return "\n".join(
            itertools.chain.from_iterable(
                [
                    cleaned_address_components[0:-1],
                    deduplicated_address_part,
                    [cleaned_address_components[-1]],
                ]
            )
        )

    def process(self, element):
        # Group together all the transactions for the property.
        property_transactions = [
            {
                "price": entry[0],
                "transaction_date": entry[1].replace(" 00:00", ""),
                "year": entry[1][0:4],
            }
            for entry in element[-1]
        ]

        # Create the dict to hold all the information about the property.
        json_object = {
            "property_id": element[0],
            "readable_address": None,
            "flat_appartment": element[-1][0][4],
            "builing": element[-1][0][10],
            "number": element[-1][0][3],
            "street": element[-1][0][5],
            "locality": element[-1][0][6],
            "town": element[-1][0][7],
            "district": element[-1][0][8],
            "county": element[-1][0][9],
            "postcode": element[-1][0][2],
            "property_transactions": property_transactions,
            "latest_transaction_year": self.get_latest_transaction(
                [
                    transaction["transaction_date"]
                    for transaction in property_transactions
                ]
            ),
        }

        # Create a human readable address to go in the dict.
        json_object["readable_address"] = self.get_readable_address(
            [
                json_object["flat_appartment"],
                json_object["builing"],
                f'{json_object["number"]} {json_object["street"]}',
                json_object["postcode"],
            ],
            [
                json_object["locality"],
                json_object["town"],
                json_object["district"],
                json_object["county"],
            ],
        )
        yield json_object


def main():
    # Load in the data from a csv file.
    input_file = (
        pathlib.Path(__file__).parents[1]
        / "data"
        / "input"
        / "pp-monthly-update-new-version.csv"
    )

    with beam.Pipeline() as pipeline:
        # Load the data
        load = (
            pipeline
            | "Read input data" >> beam.io.ReadFromText(str(input_file))
            | "Split by ','" >> beam.Map(lambda element: element.split(","))
            | "Remove leading and trailing quotes"
            >> beam.Map(lambda element: [el.strip('"') for el in element])
        )

        # Clean the data by dropping unneeded rows.
        clean_drop = (
            load
            | "Drop unneeded columns"
            >> beam.Map(lambda element: list(slice_by_range(element, (1, 4), (7, 14))))
            | "Convert to Upper Case"
            >> beam.Map(lambda element: [e.upper() for e in element])
            | "Strip leading/trailing whitespace"
            >> beam.Map(lambda element: [e.strip() for e in element])
            | "Drop Empty Postcodes" >> beam.ParDo(DropRecordsSingleEmptyColumn(2))
            | "Drop empty PAON if missing SAON"
            >> beam.ParDo(DropRecordsTwoEmptyColumn(3, 4))
            | "Split PAON into two columns if separated by comma"
            >> beam.ParDo(SplitColumn(3, ","))
        )

        # Clean the data by creating an ID, and deduplicating to eliminate repeated rows.
        clean_deduplicate = (
            clean_drop
            | "Generate unique ID for all columns"
            >> beam.ParDo(GenerateUniqueID(all_columns=True))
            | "Group by the ID for all columns"
            >> beam.GroupBy(lambda element: element[-1])
            | "Deduplicate by the ID for all columns" >> beam.ParDo(DeduplicateByID())
        )

        # Prepare the data by generating an ID using the uniquely identifying information only
        # and grouping them by this ID.
        prepare = (
            clean_deduplicate
            | "Remove previous unique ID" >> beam.ParDo(RemoveUniqueID())
            | "Generate unique ID ignoring price & date"
            >> beam.ParDo(GenerateUniqueID())
            | "Group by the ID ignoring price & date"
            >> beam.GroupBy(lambda element: element[-1])
        )

        # Format the data into a dict.
        formatted = (
            prepare
            | "Convert the prepared data into a dict object"
            >> beam.ParDo(ConvertDataToDict())
        )

        # Save the data to a .json file.
        output_file = (
            pathlib.Path(__file__).parents[1] / "data" / "output" / "pp-complete"
        )
        output = (
            formatted
            | "Combine into one PCollection" >> beam.combiners.ToList()
            | "Save to .json file"
            >> beam.io.WriteToText(
                file_path_prefix=str(output_file),
                file_name_suffix=".json",
            )
        )


if __name__ == "__main__":
    main()

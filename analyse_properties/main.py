import argparse
from datetime import datetime
import hashlib
import itertools
import json
import logging
import pathlib

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from analyse_properties.debug import *  # noqa

def slice_by_range(element, *ranges):
    """
    Slice a list with multiple ranges.

    Args:
        element : The element.
        *ranges (tuple): Tuples containing a start,end index to slice the element.
            E.g (0, 3), (5, 6) - Keeps columns 0,1,2,5. Drops everything else.

    Returns:
        list: The list sliced by the ranges
    """
    return itertools.chain(*(itertools.islice(element, *r) for r in ranges))


class DropRecordsSingleEmptyColumn(beam.DoFn):
    """
    Drop the entire row if a given column is empty.

    Args:
        index : The index of the column in the list.

    Returns:
        None: If the length of the column is 0, drop the element.

    Yields:
        element: If the length of the column is >0, keep the element.
    """

    def __init__(self, index):
        self.index = index

    def process(self, element):
        column = element[self.index]
        if len(column) == 0:
            return None
        yield element


class DropRecordsTwoEmptyColumn(beam.DoFn):
    """
    Drop the entire row if both of two given columns are empty.

    Args:
        index_0 : The index of the first column in the list.
        index_1 : The index of the second column in the list.

    Returns:
        None: If the length of both columns is 0, drop the element.

    Yields:
        element: If the length of both columns is >0, keep the element.
    """

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
    """
    Split one column into two columns by a character.

    Args:
        index : The index of the column in the list.
        split_char: The character to split the column by.
    """

    def __init__(self, index, split_char):
        self.index = index
        self.split_char = split_char

    def process(self, element):
        # If there is a split based on the split_char, then keep the second result in
        # place (street number) and append the first result (building) at the end.
        try:
            part_0, part_1 = element[self.index].split(self.split_char)
            element[self.index] = part_1.strip()
            element.append(part_0.strip())
            yield element
        except ValueError:
            # append a blank column to keep column numbers consistent.
            element.append("")
            yield element


class CreateMappingTable(beam.DoFn):
    """
    Create a mapping table to be used as a side-input.

    This mapping table has a key of an ID generated across all columns and a value of
    the raw property data.

    The table is used to populate the raw property data after a GroupByKey using
    only the IDs in order to reduce the amount of data processed in the GroupByKey operation.
    """

    def process(self, element):
        # Join the row into a string.
        unique_string = ",".join(element)
        # Hash the string.
        hashed_string = hashlib.md5(unique_string.encode())
        # Format the resulting PCollection with the key of id and value of raw data.
        new_element = (hashed_string.hexdigest(), list(element))
        yield new_element


class CreateUniquePropertyID(beam.DoFn):
    """
    Create a unique property ID which does not include the price and date of sale.

    Uses each row of the mapping table to create a PCollection with a key of the
    unique property ID and a value of the ID generated across all columns.
    """

    def process(self, element):
        unique_string = ",".join(element[-1][2:])
        hashed_string = hashlib.md5(unique_string.encode())
        new_element = (hashed_string.hexdigest(), element[0])
        yield new_element


class DeduplicateIDs(beam.DoFn):
    """Deduplicate a list of IDs."""

    def process(self, element):
        deduplicated_list = list(set(element[-1]))
        new_element = (element[0], deduplicated_list)
        yield new_element


def insert_data_for_id(element, mapping_table):
    """
    Replace the ID with the raw data from the mapping table.

    Args:
        element: The element.
        mapping_table (dict): The mapping table.

    Yields:
        The element with IDs replaced with raw data.
    """
    replaced_list = [mapping_table[data_id] for data_id in element[-1]]
    new_element = (element[0], replaced_list)
    yield new_element


class ConvertDataToDict(beam.DoFn):
    """Convert the processed data into a dict to be exported as a JSON object."""

    @staticmethod
    def get_latest_transaction(transaction_dates):
        """
        Get the date of the latest transaction for a list of dates.

        Args:
            transaction_dates (str): A date in the form "%Y-%m-%d".

        Returns:
            str: The year in the form "%Y" of the latest transaction date.
        """
        transaction_dates = [
            datetime.strptime(individual_transaction, "%Y-%m-%d")
            for individual_transaction in transaction_dates
        ]
        return max(transaction_dates).strftime("%Y")

    @staticmethod
    def get_readable_address(address_components, address_comparisons):
        """
        Create a human readable address from the locality/town/district/county columns.

        Args:
            address_components (list): The preceeding parts of the address (street, postcode etc.)
            address_comparisons (list): The locality/town/district/county.

        Returns:
            str: The complete address deduplicated & cleaned.
        """
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
                "price": int(entry[0]),
                "transaction_date": entry[1].replace(" 00:00", ""),
                "year": int(entry[1][0:4]),
            }
            for entry in element[-1]
        ]

        # Create the dict to hold all the information about the property.
        json_object = {
            "property_id": element[0],
            "readable_address": None,
            "flat_appartment": list(element[-1])[0][4],
            "builing": list(element[-1])[0][10],
            "number": list(element[-1])[0][3],
            "street": list(element[-1])[0][5],
            "locality": list(element[-1])[0][6],
            "town": list(element[-1])[0][7],
            "district": list(element[-1])[0][8],
            "county": list(element[-1])[0][9],
            "postcode": list(element[-1])[0][2],
            "property_transactions": property_transactions,
            "latest_transaction_year": int(self.get_latest_transaction(
                [
                    transaction["transaction_date"]
                    for transaction in property_transactions
                ]
            )),
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


def run(argv=None, save_main_session=True):
    """Entrypoint and definition of the pipeline."""
    logging.getLogger().setLevel(logging.INFO)

    # Default input/output files
    input_file = (
        pathlib.Path(__file__).parents[1]
        / "data"
        / "input"
        / "pp-2020.csv"
        # / "pp-complete.csv"
    )
    output_file = (
        pathlib.Path(__file__).parents[1]
        / "data"
        / "output"
        / "pp-2020"
        # / "pp-complete"
    )

    # Arguments
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        dest="input",
        default=str(input_file),
        help="Full path to the input file.",
    )
    parser.add_argument(
        "--output",
        dest="output",
        default=str(output_file),
        help="Full path to the output file without extension.",
    )
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Pipeline options. save_main_session needed for DataFlow for global imports.
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as pipeline:
        # Load the data
        load = (
            pipeline
            | "Read input data" >> beam.io.ReadFromText(known_args.input)
            | "Split by ','" >> beam.Map(lambda element: element.split(","))
            | "Remove leading and trailing quotes"
            >> beam.Map(lambda element: [el.strip('"') for el in element])
        )

        # Clean the data.
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

        # Create a mapping table
        mapping_table_raw = (
            clean_drop
            | "Create a mapping table with key of id_all_columns and value of cleaned data."
            >> beam.ParDo(CreateMappingTable())
        )

        # Condense mapping table into a single dict.
        mapping_table_condensed = (
            mapping_table_raw
            | "Condense mapping table into single dict" >> beam.combiners.ToDict()
        )

        # Prepare the data by creating IDs, grouping together and using mapping table
        # to reinsert raw data.
        prepared = (
            mapping_table_raw
            | "Create unique ID ignoring price & date"
            >> beam.ParDo(CreateUniquePropertyID())
            | "Group by ID"
            >> beam.GroupByKey()
            | "Deduplicate to eliminate repeated transactions"
            >> beam.ParDo(DeduplicateIDs())
            | "Insert the raw data using the mapping table"
            >> beam.FlatMap(
                insert_data_for_id, beam.pvalue.AsSingleton(mapping_table_condensed)
            )
        )

        # Format the data into a dict.
        formatted = (
            prepared
            | "Convert the prepared data into a dict object"
            >> beam.ParDo(ConvertDataToDict())
        )

        # Save the data to a .json file.
        (
            formatted
            | "Combine into one PCollection" >> beam.combiners.ToList()
            | "Format output" >> beam.Map(json.dumps, indent=2)
            | "Save to .json file"
            >> beam.io.WriteToText(
                file_path_prefix=known_args.output,
                file_name_suffix=".json",
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()

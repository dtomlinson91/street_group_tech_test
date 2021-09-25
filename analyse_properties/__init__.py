"""
Daniel Tomlinson (dtomlinson@panaetius.co.uk).

Technical test for Street Group.
"""

import csv
import hashlib
import io
from importlib import resources
from itertools import chain, islice

import apache_beam as beam
from apache_beam.io import fileio

from analyse_properties.debug import DebugShowEmptyColumn, DebugShowColumnWithValueIn


def csv_reader(csv_file):
    return csv.reader(io.TextIOWrapper(csv_file.open()))


def slice_by_range(element, *ranges):
    return chain(*(islice(element, *r) for r in ranges))


class DropRecordsSingleEmptyColumn(beam.DoFn):
    def __init__(self, index):
        self.index = index

    def process(self, element):
        column = element[self.index]
        if len(column) == 0:
            return None
        yield element


class DropRecordsTwoEmptyColumn(beam.DoFn):
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
    def __init__(self, index, split_char):
        self.index = index
        self.split_char = split_char

    def process(self, element):
        try:
            part_0, part_1 = element[self.index].split(self.split_char)
            element[self.index] = part_1.strip()
            element.append(part_0.strip())
            yield element
        except ValueError:
            element.append("")
            yield element


class GenerateUniqueID(beam.DoFn):
    def __init__(self, all_columns=False):
        self.all_columns = all_columns

    def process(self, element):
        unique_string = (
            ",".join(element[2:]) if not self.all_columns else ",".join(element)
        )
        hashed_string = hashlib.md5(unique_string.encode())
        element.append(hashed_string.hexdigest())
        yield element


class DeduplicateByGroup(beam.DoFn):
    def process(self, element):
        if len(element[1]) > 0:
            deduplicated_element = (element[0], [element[1][0]])
            yield deduplicated_element
        else:
            yield element


class RemoveUniqueID(beam.DoFn):
    def process(self, element):
        element_no_id = element[-1][0]
        element_no_id.pop(-1)
        yield element_no_id


class ConvertDataToDict(beam.DoFn):
    @property
    def dict_keys(self):
        return [
            "price",
            "transaction_date",
            "postcode",
            "number",
            "flat_appartment",
            "street",
            "locality",
            "town_city",
            "district",
            "county",
            "building",
            "property_id",
        ]

    def process(self, element):
        pass


def main():
    csv_data = resources.path(
        "analyse_properties.data",
        "pp-monthly-update-new-version.csv"
        # "analyse_properties.data", "pp-complete.csv"
    )

    with beam.Pipeline() as pipeline:
        # Load the data
        with csv_data as csv_data_file:
            # https://github.com/apache/beam/blob/v2.32.0/sdks/python/apache_beam/io/fileio_test.py#L155-L170
            load = (
                pipeline
                | fileio.MatchFiles(str(csv_data_file))
                | fileio.ReadMatches()
                | beam.FlatMap(csv_reader)
            )

        # Clean the data
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
            # | beam.ParDo(DebugShowColumnWithValueIn(3, ","))
            # | beam.ParDo(DebugShowColumnWithValueIn(2, "AL1 4SZ"))
            # | beam.ParDo(DebugShowColumnWithValueIn(2, "B16 0AE"))
            | "Split PAON into two columns if separated by comma"
            >> beam.ParDo(SplitColumn(3, ","))
        )

        clean_deduplicate = (
            clean_drop
            | "Generate unique ID for all columns"
            >> beam.ParDo(GenerateUniqueID(all_columns=True))
            | "Group by the ID for all columns"
            >> beam.GroupBy(lambda element: element[-1])
            | "Deduplicate by the ID for all columns"
            >> beam.ParDo(DeduplicateByGroup())
            # | beam.Map(print)
        )

        # Prepare the data
        prepare = (
            clean_deduplicate
            | "Remove previous unique ID" >> beam.ParDo(RemoveUniqueID())
            | "Generate unique ID ignoring price & date"
            >> beam.ParDo(GenerateUniqueID())
            | "Group by the ID ignoring price & date"
            >> beam.GroupBy(lambda element: element[-1])
            | beam.Map(print)
        )

        # Format the data
        formatted = (
            prepare
            # | "Convert list to dict object" >>
        )


if __name__ == "__main__":
    main()

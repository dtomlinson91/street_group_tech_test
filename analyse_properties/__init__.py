"""
Daniel Tomlinson (dtomlinson@panaetius.co.uk).

Technical test for Street Group.
"""

import csv
import io
from importlib import resources
from itertools import chain, islice

import apache_beam as beam
from apache_beam.io import fileio


def csv_reader(csv_file):
    return csv.reader(io.TextIOWrapper(csv_file.open()))


def slicer(element, *ranges):
    return chain(*(islice(element, *r) for r in ranges))


class SplitPAON(beam.DoFn):
    def process(self, element):
        paon_split = element[3].split(",")

        if len(paon_split) == 0:
            return element
        elif len(paon_split) == 1:
            pass
        else:
            pass


def main():
    csv_data = resources.path(
        "analyse_properties.data", "pp-monthly-update-new-version.csv"
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
        clean = (
            load
            | "Drop unneeded columns"
            >> beam.Map(lambda element: list(slicer(element, (1, 4), (7, 14))))
            | "Convert to Upper Case"
            >> beam.Map(lambda element: [e.upper() for e in element])
            | beam.Map(print)
        )


if __name__ == "__main__":
    main()

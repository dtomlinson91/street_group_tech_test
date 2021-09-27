# Usage

This page documents how to run the pipeline locally to complete the task for the [dataset for 2020](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads#section-1).

The pipeline also runs in GCP using DataFlow and is discussed further on but can be viewed here. We also discuss how to adapt the pipeline so it can run against [the full dataset](https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads#single-file).

## Download dataset

The input data by default should go in `./data/input`.

For convenience the data is available publicly in a GCP Cloud Storage bucket.

Run:

```bash
wget https://storage.googleapis.com/street-group-technical-test-dmot-euw1/input/pp-2020.csv -P data/input
```

to download the data for 2020 and place in the input directory above.

## Entrypoint

The entrypoint to the pipeline is `analyse-properties`.

## Available options

Running

```bash
analyse-properties --help
```

gives the following output:

```bash
usage: analyse-properties [-h] [--input INPUT] [--output OUTPUT]

optional arguments:
  -h, --help       show this help message and exit
  --input INPUT    Full path to the input file.
  --output OUTPUT  Full path to the output file without extension.
```

The default value for input is `./data/input/pp-2020.csv` and the default value for output is `./data/output/pp-2020`.

If passing in values for `input`/`output` these should be **full** paths to the files. The test will parse these inputs as a `str()` and pass this to `#!python beam.io.ReadFromText()`.

## Run the pipeline

To run the pipeline and complete the task run:

```bash
analyse-properties --runner DirectRunner
```

The pipeline will use the 2020 dataset located in `./data/input` and output the resulting `.json` to `./data/output`.

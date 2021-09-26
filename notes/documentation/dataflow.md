# DataFlow

<https://cloud.google.com/dataflow/docs/quickstarts/quickstart-python>

## Examples

Full example of beam pipeline on dataflow:

<https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/complete/juliaset>

## Setup

Export env variable:

`export GOOGLE_APPLICATION_CREDENTIALS="/home/dtomlinson/git-repos/work/street_group/street_group_tech_test/street-group-0c490d23a9d0.json"`

## Run pipeline

### Dataflow

#### Monthly dataset

```bash
python -m analyse_properties.main \
    --region europe-west1 \
    --input gs://street-group-technical-test-dmot-euw1/input/pp-monthly-update-new-version.csv \
    --output gs://street-group-technical-test-dmot-euw1/output/pp-monthly-update-new-version \
    --runner DataflowRunner \
    --project street-group \
    --temp_location gs://street-group-technical-test-dmot-euw1/tmp \
    --subnetwork=https://www.googleapis.com/compute/v1/projects/street-group/regions/europe-west1/subnetworks/europe-west-1-dataflow \
    --no_use_public_ips
```

#### Full dataset

```bash
python -m analyse_properties.main \
    --region europe-west1 \
    --input gs://street-group-technical-test-dmot-euw1/input/pp-complete.csv \
    --output gs://street-group-technical-test-dmot-euw1/output/pp-complete \
    --runner DataflowRunner \
    --project street-group \
    --temp_location gs://street-group-technical-test-dmot-euw1/tmp \
    --subnetwork=https://www.googleapis.com/compute/v1/projects/street-group/regions/europe-west1/subnetworks/europe-west-1-dataflow \
    --no_use_public_ips
```

### Locally

Run the pipeline locally:

`python -m analyse_properties.main --runner DirectRunner`

## Errors

Unsubscriptable error on window:

<https://stackoverflow.com/questions/42276520/what-does-object-of-type-unwindowedvalues-has-no-len-mean>

## Documentation

Running in its own private VPC without public IPs

- <https://stackoverflow.com/questions/58893082/which-compute-engine-quotas-need-to-be-updated-to-run-dataflow-with-50-workers>
- <https://cloud.google.com/dataflow/docs/guides/specifying-networks#subnetwork_parameter>

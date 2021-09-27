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

#### Yearly dataset

```bash
python -m analyse_properties.main \
    --runner DataflowRunner \
    --project street-group \
    --region europe-west1 \
    --input gs://street-group-technical-test-dmot-euw1/input/pp-2020.csv \
    --output gs://street-group-technical-test-dmot-euw1/output/pp-2020 \
    --temp_location gs://street-group-technical-test-dmot-euw1/tmp \
    --subnetwork=https://www.googleapis.com/compute/v1/projects/street-group/regions/europe-west1/subnetworks/europe-west-1-dataflow \
    --no_use_public_ips \
    --worker_machine_type=n1-highmem-2
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
    --no_use_public_ips \
    --worker_machine_type=n1-highmem-8 \
    --num_workers=3 \
    --autoscaling_algorithm=NONE
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

Error help

- <https://cloud.google.com/dataflow/docs/guides/common-errors>
- <https://cloud.google.com/dataflow/docs/guides/troubleshooting-your-pipeline>

Scaling

Using DataFlowPrime: <https://cloud.google.com/dataflow/docs/guides/enable-dataflow-prime#enable-prime>
Use `--experiments=enable_prime`

Deploying a pipeline (with scaling options): <https://cloud.google.com/dataflow/docs/guides/deploying-a-pipeline>

Available VM types (with pricing): <https://cloud.google.com/compute/vm-instance-pricing#n1_predefined>

Performance

Sideinput performance: <https://stackoverflow.com/questions/48242320/google-dataflow-apache-beam-python-side-input-from-pcollection-kills-perform>

Common use cases:

- Part 1 <https://cloud.google.com/blog/products/data-analytics/guide-to-common-cloud-dataflow-use-case-patterns-part-1>
- Part 2 <https://cloud.google.com/blog/products/data-analytics/guide-to-common-cloud-dataflow-use-case-patterns-part-2>

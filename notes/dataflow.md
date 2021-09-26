# DataFlow

<https://cloud.google.com/dataflow/docs/quickstarts/quickstart-python>

Export env variable:

`export GOOGLE_APPLICATION_CREDENTIALS="/home/dtomlinson/git-repos/work/street_group/street_group_tech_test/street-group-0c490d23a9d0.json"`

Run the pipeline:

python -m analyse_properties.main \
    --region europe-west2 \
    --input gs://street-group-technical-test-dmot/input/pp-monthly-update-new-version.csv \
    --output gs://street-group-technical-test-dmot/input/pp-monthly-update-new-version \
    --runner DataflowRunner \
    --project street-group \
    --temp_location gs://street-group-technical-test-dmot/tmp

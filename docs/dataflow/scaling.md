# Scaling to the full DataSet

As is the pipeline will not run against the full dataset. But with a little work done to the existing pipeline I believe it is possible to work against the full dataset of ~27 million rows.

## Mapping table

Using a mapping table as a side-input means that for the full dataset this table is going to be huge.

Side inputs are stored in memory on the workers, with such a huge table the machines are going to quickly run out of available memory when autoscaling is applied.

Running the pipeline against the full dataset resulted in the following error:

```text
"Out of memory: Killed process 2042 (python) total-vm:28616496kB, anon-rss:25684136kB, file-rss:0kB, shmem-rss:0kB, UID:0 pgtables:51284kB oom_score_adj:900"
```

with the pipeline job failing to process anything and the rows being processed per/sec gradually falling to zero as the workers killed the Python process to try free up more memory. This resulted in autoscaling down (as the CPU decreased) and the entire pipeline stagnated.

Using a higher tiered `worker_machine_type`, disabling autoscaling, and fixing the workers to the maximum number of vCPUs available to the quota results in pipeline options:

```bash
--worker_machine_type=n1-highmem-8 \
--num_workers=3 \
--autoscaling_algorithm=NONE
```

with 156GB of RAM available to the pipeline with 52GB on each worker.

The pipeline was able to progress further until Python threw an error and the pipeline failed and shut down:

```text
"Error message from worker: Traceback (most recent call last):
  File "/usr/local/lib/python3.7/site-packages/dataflow_worker/batchworker.py", line 651, in do_work
    work_executor.execute()
  ...
  File "/usr/local/lib/python3.7/multiprocessing/connection.py", line 393, in _send_bytes
    header = struct.pack("!i", n)
struct.error: 'i' format requires -2147483648 <= number <= 2147483647
```

The number 2147483647 being the maximum value for a 32bit integer.

As the side-input needs to be pickled (or serialised), this tells us that the table is far too large to be pickled and passed to the other workers. No amount of CPU/Memory can fix the problem.

## Patterns

Google have several patterns for large side-inputs which are documented here:

- Part 1 <https://cloud.google.com/blog/products/data-analytics/guide-to-common-cloud-dataflow-use-case-patterns-part-1>
- Part 2 <https://cloud.google.com/blog/products/data-analytics/guide-to-common-cloud-dataflow-use-case-patterns-part-2>

## Solution

A possible solution would be to leverage BigQuery to store the results of the mapping table in as the pipeline progresses. We can make use of BigQueries array type to literally store the raw array as we process each row.

In addition to creating the mapping table `(key, value)` pairs, we also save these pairs to BigQuery at this stage. We then yield the element as it is currently written to allow the subsequent stages to make use of this data.

Remove the condense mapping table stage as it is no longer needed.

Instead of using:

```python
beam.FlatMap(
    insert_data_for_id, beam.pvalue.AsSingleton(mapping_table_condensed)
)
```

to insert the results of the mapping table we write a new `DoFn` that takes the element, and for each `id_all_columns` in the array we make a call to BigQuery to get the array for this ID and insert it at this stage.

Because each `id_all_columns` and its corresponding data is only used once, there would be no need to cache the results from BigQuery, however some work could be done to see if we could pull back more than one row at a time and cache these, saving time/costs in calls to BigQuery.

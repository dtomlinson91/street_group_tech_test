# Data Exploration Report

A brief exploration was done on the **full** dataset using the module `pandas-profiling`. The module uses `pandas` to load a dataset and automatically produce quantile/descriptive statistics, common values, extreme values, skew, kurtosis etc. and produces a report `.html` file that can be viewed interatively in your browser.

The script used to generate this report is located in `./exploration/report.py` and can be viewed below.

<details>
	<summary>report.py</summary>
```python
--8<-- "exploration/report.py"
```
</details>

The report can be viewed by clicking the Data Exploration Report tab at the top of the page.

## Interesting observations

When looking at the report we are looking for data quality and missing observations. The statistics are interesting to see but are largely irrelevant for this task.

The data overall looks very good for a dataset of its size (~27 million records). For important fields there are no missing values:

- Every row has a price.
- Every row has a unique transaction ID.
- Every row has a transaction date.

Some fields that we will need are missing data:

- ~42,000 (0.2%) are missing a Postcode.
- ~4,000 (<0.1%) are missing a PAON (primary addressable object name).
- ~412,000 (1.6%) are missing a Street Name.

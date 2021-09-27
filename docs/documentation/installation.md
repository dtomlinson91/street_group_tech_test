# Installation

The task is written in Python 3.7.9 using Apache Beam 2.32.0. Python versions 3.6.14 and 3.8.11 should also be compatible but have not been tested.

The task has been tested on MacOS Big Sur and WSL2. The task should run on Windows but this wasn't tested.

For Beam 2.32.0 the supported versions of the Python SDK can be found [here](https://cloud.google.com/dataflow/docs/concepts/sdk-worker-dependencies#sdk-for-python).

## Poetry

The test uses [Poetry](https://python-poetry.org) for dependency management.

!!! info inline end
    If you already have Poetry installed globally you can go straight to the `poetry install` step.

In a virtual environment install poetry:

```bash
pip install poetry
```

From the root of the repo install the dependencies with:

```bash
poetry install --no-dev
```

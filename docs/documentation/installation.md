# Installation

The task is written in Python 3.7.9 using Apache Beam 2.32.0. Python versions 3.6.14 and 3.8.11 should also be compatible but have not been tested.

The task has been tested on MacOS Big Sur and WSL2. The task should run on Windows but this wasn't tested.

For Beam 2.32.0 the supported versions of the Python SDK can be found [here](https://cloud.google.com/dataflow/docs/concepts/sdk-worker-dependencies#sdk-for-python).

## Pip

In a virtual environment run from the root of the repo:

```bash
pip install -r requirements.txt
```

## Poetry (Alternative)

Install [Poetry](https://python-poetry.org) *globally*

From the root of the repo install the dependencies with:

```bash
poetry install --no-dev
```

Activate the shell with:

```bash
poetry shell
```

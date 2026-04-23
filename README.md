# Parflux - Efficient InfluxDB Data Downloader and Exporter

![Parflux Logo](parflux_logo.png)

Parflux is an open-source Python project that offers a command-line interface for efficiently downloading and exporting
large datasets from InfluxDB. The primary objective of Parflux is to store the query results in Parquet files, which
contributes to its name, and it leverages DuckDB internally for handling the heavy lifting.
The motivation behind using DuckDB is to overcome the limitations of the native InfluxDB Python client, which might not be as efficient when dealing with extensive datasets.

## Features

- Efficiently download and export large datasets from InfluxDB.
- Store query results in Parquet files for optimized storage.
- Utilize DuckDB for enhanced performance and handling of big data.
- Simple and easy-to-use command-line interface.
- Open-source and freely available for anyone to use and contribute.

## Installation

Before installing Parflux, ensure you have Python 3.x and pip installed on your system. To install Parflux, follow these steps:

```bash
pip install parflux
```

## Usage

Parflux comes with a user-friendly command-line interface that makes it easy to download/export data efficiently.

> **Attention**: parflux requires InfluxDB connection and authentication settings to be set up via environment variables:
>
> ```conf
> # .env
> INFLUXDB_V2_URL=http://192.168.52.12:8086
> INFLUXDB_V2_ORG=starcopter
> INFLUXDB_V2_TOKEN=super_secret_token
> INFLUXDB_V2_TIMEOUT=300000
> ```

As the CLI is still under heavy construction, refer to the command line help for usage information:

```shell
> parflux --help

 Usage: parflux [OPTIONS] COMMAND [ARGS]...

╭─ Options ──────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ --start                       TIMESTAMP  [default: None]                                                       │
│ --stop                        TIMESTAMP  [default: None]                                                       │
│ --verbose             -v      INTEGER    [default: 0]                                                          │
│ --reload-env          -r                                                                                       │
│ --install-completion          Install completion for the current shell.                                        │
│ --show-completion             Show completion for the current shell, to copy it or customize the installation. │
│ --help                        Show this message and exit.                                                      │
╰────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ─────────────────────────────────────────────────────────────────────────────────────────────────────╮
│ get                           Download Bucket or Single Measurement                                            │
╰────────────────────────────────────────────────────────────────────────────────────────────────────────────────╯
```

## Development

Parflux uses [`prek`](https://github.com/j178/prek) to run pre-commit hooks locally and in CI. Hooks are configured in `prek.toml`.

Set up local hooks:

```bash
uv sync --dev
uv run prek install --hook-type pre-commit --hook-type commit-msg
```

Run hooks manually across all files:

```bash
uv run prek run --all-files
```

### Testing

Parflux uses [`pytest`](https://docs.pytest.org/) with `pytest-cov` for coverage. Install the test dependencies and run the unit suite:

```bash
uv sync --group test
uv run pytest
```

This runs the unit tests, prints a coverage summary, and writes `coverage.xml` for CI consumption. Unit tests do not require a live InfluxDB instance.

A single opt-in integration test exercises an end-to-end download against a real InfluxDB v2 server. It requires the same `INFLUXDB_V2_URL`, `INFLUXDB_V2_ORG`, and `INFLUXDB_V2_TOKEN` environment variables used by the CLI, and only runs when `PARFLUX_RUN_INTEGRATION=1` is set:

```bash
PARFLUX_RUN_INTEGRATION=1 uv run pytest -m integration
```

### Continuous Integration

GitHub Actions runs on every pull request and push to `main`:

- `prek` ([`.github/workflows/prek.yml`](.github/workflows/prek.yml)) runs ruff, typos, and the other pre-commit hooks.
- `tests` ([`.github/workflows/tests.yml`](.github/workflows/tests.yml)) runs the unit suite against Python 3.10-3.13 and uploads `coverage.xml` as a build artifact. It also defines an on-demand `integration` job (triggered via Actions "Run workflow") that spins up InfluxDB 2.7 as a service container and executes the `integration`-marked tests against it.

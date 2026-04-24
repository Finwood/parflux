# Parflux

Parflux is a Linux-focused CLI for exporting large InfluxDB v2 datasets to parquet files.
It is designed for high-volume exports (including gigabyte-scale ranges), supports bucket-wide and
measurement-level queries, and targets fast batch workflows rather than library-style usage.

## Installation

```bash
$ uv tool install parflux

# or just use it directly
$ uvx parflux
```

## Usage

Set InfluxDB connection/authentication environment variables:
>
> ```conf
> # .env
> INFLUXDB_V2_URL=http://192.168.52.12:8086
> INFLUXDB_V2_ORG=starcopter
> INFLUXDB_V2_TOKEN=super_secret_token
> INFLUXDB_V2_TIMEOUT=300000
> ```

Inspect all CLI options:

```shell
$ parflux --help

 Usage: parflux [OPTIONS] QUERY...

 Export InfluxDB v2 data to parquet files.

 Provide one or more selectors as <bucket> or <bucket>/<measurement>.
 Results are written to <dest>/<bucket>/<measurement>.parquet and can be
 constrained by time range and optional Flux filters.

 Warning: Query input is used as-is and is not sanitized against Flux injection.
 Only run trusted queries.

╭─ Arguments ──────────────────────────────────────────────────────────────────────╮
│ *    query      QUERY...  One or more selectors in the form <bucket> or          │
│                           <bucket>/<measurement>.                                │
│                           [required]                                             │
╰──────────────────────────────────────────────────────────────────────────────────╯
╭─ Options ────────────────────────────────────────────────────────────────────────╮
│ --start       -s      START         Start timestamp (inclusive), e.g.            │
│                                     2026-04-24T14:45:00+02:00 or 2025-01-01. If  │
│                                     no timezone is specified, the local timezone │
│                                     is assumed. [default: END - 1 day]           │
│ --end         -e      END           End timestamp (exclusive). If no timezone is │
│                                     specified, the local timezone is assumed.    │
│                                     [default: now]                               │
│ --dest        -d      PATH          Destination base directory where parquet     │
│                                     files should be saved. [default: current     │
│                                     directory]                                   │
│ --filter      -f      TEXT          Additional flux filters to apply to the      │
│                                     query. The current record is available as r. │
│                                     Can be specified multiple times.             │
│                                     Example: r.host == 'h1' or r.env =~ /prod/   │
│ --verbose     -v                    Increase verbosity. Can be specified         │
│                                     multiple times.                              │
│ --reload-env  -r                    Reload environment variables from .env file. │
│ --batch-size          HOURS [x>=1]  Query batch size in hours. [default: 24]     │
│ --help                              Show this message and exit.                  │
╰──────────────────────────────────────────────────────────────────────────────────╯

```

Common examples:

```shell
# Export a full bucket for a time range
parflux my-bucket --start 2026-04-01 --end 2026-04-02

# Export one measurement with an extra Flux filter
parflux my-bucket/cpu --filter "r.host == 'h1'"
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

## Release

Releases are tag-driven and automated through `.github/workflows/release.yml`.
When a tag like `v0.2.0` is pushed, GitHub Actions will:
- verify tag version (`v0.2.0` -> `0.2.0`) matches `[project].version` in `pyproject.toml`
- build distributions with `uv build`
- publish to PyPI with `uv publish`
- create a GitHub Release and attach `dist/*`

### Prerequisites

- A `parflux` project exists on PyPI.
- Configure one of the following for publishing authentication:
  - **Preferred:** PyPI Trusted Publishing for this repository/workflow.
  - **Fallback:** repository secret `UV_PUBLISH_TOKEN` with a PyPI API token.
- GitHub Actions is enabled for the repository.

### Create a new release

1. Update version in `pyproject.toml`:

   ```toml
   [project]
   version = "0.2.0"
   ```

2. Run checks locally:

   ```bash
   uv sync --group test
   uv run pytest
   uv build
   ```

3. Commit and push the version bump:

   ```bash
   git add pyproject.toml
   git commit -m "chore: release 0.2.0"
   git push
   ```

4. Create and push the release tag:

   ```bash
   git tag v0.2.0
   git push origin v0.2.0
   ```

5. Monitor the `release` workflow in GitHub Actions and confirm:
   - package is published on PyPI
   - GitHub Release for `v0.2.0` is created

### Version mismatch behavior

If the tag and `pyproject.toml` version do not match, the workflow fails before publish.
Fix by updating `pyproject.toml` or using the correct tag, then push a corrected tag.

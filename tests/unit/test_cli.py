from datetime import datetime, timedelta, timezone
from pathlib import Path

from typer.testing import CliRunner

from parflux.cli import app
from parflux.core import DEFAULT_BATCH_SIZE

runner = CliRunner()


def test_cli_applies_defaults_and_calls_download(mocker):
    download = mocker.patch("parflux.cli.download")
    now = datetime(2024, 1, 2, 3, 4, 5, 123456, tzinfo=timezone.utc)
    mock_now = mocker.patch("parflux.cli._now")
    mock_now.return_value = now

    result = runner.invoke(app, ["bucket/cpu"])

    assert result.exit_code == 0
    download.assert_called_once()
    kwargs = download.call_args.kwargs
    assert kwargs["queries"] == ["bucket/cpu"]
    assert kwargs["basedir"] == Path(".")
    assert kwargs["filters"] == []
    assert kwargs["batch_size"] == DEFAULT_BATCH_SIZE
    assert kwargs["stop"].microsecond == 0
    assert kwargs["stop"].tzinfo is not None
    assert kwargs["start"] == kwargs["stop"] - timedelta(days=1)


def test_cli_forwards_explicit_values(mocker, tmp_path):
    download = mocker.patch("parflux.cli.download")
    start = "2024-01-01T00:00:00"
    stop = "2024-01-02T00:00:00"

    result = runner.invoke(
        app,
        [
            "bucket/cpu",
            "bucket/mem",
            "--dest",
            str(tmp_path),
            "--filter",
            'r.host == "h1"',
            "--filter",
            "r.env =~ /prod/",
            "--batch-size=6",
            "--start",
            start,
            "--stop",
            stop,
        ],
    )

    assert result.exit_code == 0
    kwargs = download.call_args.kwargs
    assert kwargs["queries"] == ["bucket/cpu", "bucket/mem"]
    assert kwargs["basedir"] == tmp_path
    assert kwargs["filters"] == ['r.host == "h1"', "r.env =~ /prod/"]
    assert kwargs["batch_size"] == timedelta(hours=6)
    assert kwargs["start"].year == 2024 and kwargs["start"].month == 1 and kwargs["start"].day == 1
    assert kwargs["stop"].year == 2024 and kwargs["stop"].month == 1 and kwargs["stop"].day == 2
    assert kwargs["start"].tzinfo is not None
    assert kwargs["stop"].tzinfo is not None


def test_cli_reload_env_flag_calls_load_dotenv(mocker):
    download = mocker.patch("parflux.cli.download")
    load_dotenv = mocker.patch("dotenv.load_dotenv")

    result = runner.invoke(app, ["bucket/cpu", "--reload-env"])

    assert result.exit_code == 0
    load_dotenv.assert_called_once()
    download.assert_called_once()

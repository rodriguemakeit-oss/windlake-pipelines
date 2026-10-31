"""This file configures pytest, initializes Databricks Connect, and provides fixtures for Spark and loading test data."""

import os, sys, pathlib, json, csv
from contextlib import contextmanager

import pytest
from pyspark.sql import SparkSession

try:
    from databricks.connect import DatabricksSession
    from databricks.sdk import WorkspaceClient
    _DATABRICKS_AVAILABLE = True
except ImportError:
    # local unit test run (uv run --only-group unit): no databricks-connect/sdk,
    # tests/unit/conftest.py provides its own local spark fixture instead.
    _DATABRICKS_AVAILABLE = False


@pytest.fixture()
def spark() -> SparkSession:
    """Provide a SparkSession fixture for tests.

    Minimal example:
        def test_uses_spark(spark):
            df = spark.createDataFrame([(1,)], ["x"])
            assert df.count() == 1
    """
    if _DATABRICKS_AVAILABLE:
        return DatabricksSession.builder.getOrCreate()
    return SparkSession.builder.master("local[*]").appName("ltsa-unit-tests").getOrCreate()


@pytest.fixture()
def load_fixture(spark: SparkSession):
    """Provide a callable to load JSON or CSV from fixtures/ directory.

    Example usage:

        def test_using_fixture(load_fixture):
            data = load_fixture("my_data.json")
            assert data.count() >= 1
    """

    def _loader(filename: str):
        path = pathlib.Path(__file__).parent.parent / "fixtures" / filename
        suffix = path.suffix.lower()
        if suffix == ".json":
            rows = json.loads(path.read_text())
            return spark.createDataFrame(rows)
        if suffix == ".csv":
            with path.open(newline="") as f:
                rows = list(csv.DictReader(f))
            return spark.createDataFrame(rows)
        raise ValueError(f"Unsupported fixture type for: {filename}")

    return _loader


def _enable_fallback_compute():
    """Enable serverless compute if no compute is specified."""
    try:
        conf = WorkspaceClient().config
    except Exception:
        return  # no credentials configured (e.g. local unit test run)
    if conf.serverless_compute_id or conf.cluster_id or os.environ.get("SPARK_REMOTE"):
        return

    url = "https://docs.databricks.com/dev-tools/databricks-connect/cluster-config"
    print("☁️ no compute specified, falling back to serverless compute", file=sys.stderr)
    print(f"  see {url} for manual configuration", file=sys.stdout)

    os.environ["DATABRICKS_SERVERLESS_COMPUTE_ID"] = "auto"


@contextmanager
def _allow_stderr_output(config: pytest.Config):
    """Temporarily disable pytest output capture."""
    capman = config.pluginmanager.get_plugin("capturemanager")
    if capman:
        with capman.global_and_fixture_disabled():
            yield
    else:
        yield


def pytest_configure(config: pytest.Config):
    """Configure pytest session."""
    if not _DATABRICKS_AVAILABLE:
        return  # local unit test run — nothing to configure
    with _allow_stderr_output(config):
        try:
            _enable_fallback_compute()
            if hasattr(DatabricksSession.builder, "validateSession"):
                DatabricksSession.builder.validateSession().getOrCreate()
            else:
                DatabricksSession.builder.getOrCreate()
        except Exception:
            pass  # no credentials — unit tests use their own local spark fixture

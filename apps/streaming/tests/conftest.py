import importlib.util
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

SPARK_VERSION = "4.0.5"


def _spark_home() -> str:
    """Path to the bundled pyspark, hiding `!` from Windows `spark-class2.cmd` delayed expansion."""
    home = Path(importlib.util.find_spec("pyspark").origin).parent
    if os.name == "nt" and "!" in str(home):
        link = Path(tempfile.gettempdir()) / f"qoe-pyspark-{SPARK_VERSION}"
        if not link.exists():
            subprocess.run(["cmd", "/c", "mklink", "/J", str(link), str(home)], check=True)  # noqa: S603, S607
        return str(link)
    return str(home)


@pytest.fixture(scope="session")
def spark():
    os.environ["SPARK_HOME"] = _spark_home()
    java = shutil.which("java")
    if java:
        os.environ["JAVA_HOME"] = str(Path(java).resolve().parents[1])
    # Python workers must run the interpreter that runs the tests (Windows has no `python3`).
    os.environ["PYSPARK_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .config("spark.jars.packages", f"org.apache.spark:spark-avro_2.13:{SPARK_VERSION}")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()

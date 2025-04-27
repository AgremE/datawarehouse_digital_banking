import pytest
from pyspark.sql import SparkSession
from io_helper import ReadWriteManager, build_struct_type
import yaml
from pathlib import Path


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("test").getOrCreate()


def test_build_struct_type_simple():
    schema_cfg = {"col1": "string", "col2": "int"}
    struct = build_struct_type(schema_cfg)
    assert struct["col1"].dataType.simpleString() == "string"
    assert struct["col2"].dataType.simpleString() == "int"


def test_load_cfg(tmp_path, spark):
    cfg_dir = tmp_path / "tables"
    cfg_dir.mkdir()
    table_cfg = {
        "read": {"format": "parquet", "path": "data/raw/"},
        "write": {"format": "parquet", "path": "data/staging/"},
    }
    cfg_file = cfg_dir / "test.yaml"
    cfg_file.write_text(yaml.dump(table_cfg))
    rw = ReadWriteManager(spark, str(cfg_dir))
    loaded = rw._load_cfg("test")
    assert loaded["read"]["format"] == "parquet"

import yaml
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
    DecimalType,
    DoubleType,
)
from pyspark.sql.functions import to_timestamp, col

TYPE_MAP = {"string": StringType(), "int": IntegerType(), "double": DoubleType()}


def build_struct_type(schema_cfg):
    fields = []
    for name, defn in schema_cfg.items():
        if isinstance(defn, str):
            spark_type = TYPE_MAP[defn]
        else:
            t = defn["type"]
            if t == "timestamp":
                spark_type = TimestampType()
            elif t == "decimal":
                spark_type = DecimalType(defn["precision"], defn["scale"])
            else:
                spark_type = TYPE_MAP[t]
        fields.append(StructField(name, spark_type, True))
    return StructType(fields)


class ReadWriteManager:
    def __init__(self, spark: SparkSession, config_dir: str):
        self.spark = spark
        self.config_dir = Path(config_dir)

    def _load_cfg(self, table_name: str):
        cfg_path = self.config_dir / f"{table_name}.yaml"
        if not cfg_path.exists():
            raise FileNotFoundError(
                f"Config for '{table_name}' not found at {cfg_path}"
            )
        return yaml.safe_load(cfg_path)

    def read(self, table_name: str):
        cfg = self._load_cfg(table_name)
        cfg = cfg["read"] if "read" in cfg else cfg["read_write"]
        reader = self.spark.read.format(cfg["format"]).options(**cfg.get("options", {}))
        if "schema" in cfg:
            struct = build_struct_type(cfg["schema"])
            df = reader.schema(struct).load(cfg["path"])
            for col_name, defn in cfg["schema"].items():
                if isinstance(defn, dict) and defn.get("type") == "timestamp":
                    df = df.withColumn(
                        col_name, to_timestamp(col(col_name), defn["format"])
                    )
            return df
        return reader.load(cfg["path"])

    def write(self, df, table_name: str):
        cfg = self._load_cfg(table_name)["write"]
        cfg = cfg["write"] if "write" in cfg else cfg["read_write"]
        if "schema" in cfg:
            df = df.select(*cfg["schema"])
        writer = df.write.format(cfg["format"]).mode(cfg.get("mode", "error"))
        if "partitionBy" in cfg:
            writer = writer.partitionBy(*cfg["partitionBy"])
        writer.save(cfg["path"])

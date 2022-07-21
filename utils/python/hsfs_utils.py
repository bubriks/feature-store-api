import argparse
import json

from python import hsfs
from python.hsfs.constructor import query
from typing import Dict, Any
from pydoop import hdfs
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, _parse_datatype_string, StructField
from python.hsfs.core import feature_view_engine


def read_job_conf(path: str) -> Dict[Any, Any]:
    """
    The configuration file is passed as path on HopsFS
    The path is a JSON containing different values depending on the op type
    """
    file_content = hdfs.load(path)
    return json.loads(file_content)


def setup_spark() -> SparkSession:
    return SparkSession.builder.enableHiveSupport().getOrCreate()


def get_feature_store_handle(feature_store: str = "") -> hsfs.feature_store:
    connection = hsfs.connection()
    return connection.get_feature_store(feature_store)


def sort_schema(fg_schema: StructType, csv_df_schema: StructType) -> StructType:
    # The schema order of the fg_schema needs to match the
    # order of the csv_df_schema
    csv_df_schema_indices = [
        csv_df_schema.names.index(field) for field in fg_schema.names
    ]
    fg_schema_sorted = sorted(
        zip(fg_schema.fields, csv_df_schema_indices), key=lambda x: x[1]
    )
    return StructType([f[0] for f in fg_schema_sorted])


def get_fg_spark_df(job_conf: Dict[Any, Any], fg_schema: StructType) -> Any:
    data_path = job_conf.pop("data_path")
    data_format = job_conf.pop("data_format")
    data_options = job_conf.pop("data_options")

    csv_df = spark.read.format(data_format).options(**data_options).load(data_path)

    schema = sort_schema(fg_schema, csv_df.schema)

    return (
        spark.read.format(data_format)
        .options(**data_options)
        .schema(schema)
        .load(data_path)
    )


def insert_fg(spark: SparkSession, job_conf: Dict[Any, Any]) -> None:
    """
    Insert data into a feature group.
    The data path, feature group name and versions are in the configuration file
    """
    feature_store = job_conf.pop("feature_store")
    fs = get_feature_store_handle(feature_store)

    fg = fs.get_feature_group(name=job_conf["name"], version=job_conf["version"])

    schema = StructType(
        [StructField(f.name, _parse_datatype_string(f.type), True) for f in fg.features]
    )

    df = get_fg_spark_df(job_conf, schema)
    fg.insert(df, write_options=job_conf.pop("write_options", {}) or {})


def create_td(job_conf: Dict[Any, Any]) -> None:
    # Extract the feature store handle
    feature_store = job_conf.pop("feature_store")
    fs = get_feature_store_handle(feature_store)

    # Extract the query object
    q = query.Query._hopsworks_json(job_conf.pop("query"))

    td = fs.get_training_dataset(name=job_conf["name"], version=job_conf["version"])
    td.insert(
        q,
        overwrite=job_conf.pop("overwrite", False) or False,
        write_options=job_conf.pop("write_options", {}) or {},
    )

def create_fv_td(job_conf: Dict[Any, Any]) -> None:
    # Extract the feature store handle
    feature_store = job_conf.pop("feature_store")
    fs = get_feature_store_handle(feature_store)

    fv = fs.get_feature_view(name=job_conf["name"], version=job_conf["version"])
    fv_engine = feature_view_engine.FeatureViewEngine(fv.featurestore_id)

    fv_engine.compute_training_dataset(
        fv,
        job_conf.pop("write_options", {}) or {},
        training_dataset_version=job_conf["td_version"],
    )

def compute_stats(job_conf: Dict[Any, Any]) -> None:
    """
    Compute/Update statistics on a feature group
    """
    feature_store = job_conf.pop("feature_store")
    fs = get_feature_store_handle(feature_store)

    entity_type = job_conf["type"]
    if entity_type == "fg":
        entity = fs.get_feature_group(
            name=job_conf["name"], version=job_conf["version"]
        )
    else:
        entity = fs.get_training_dataset(
            name=job_conf["name"], version=job_conf["version"]
        )

    entity.compute_statistics()


if __name__ == "__main__":
    # Setup spark first so it fails faster in case of args errors
    # Otherwise the resource manager will wait until the spark application master
    # registers, which never happens.
    spark = setup_spark()

    parser = argparse.ArgumentParser(description="HSFS Job Utils")
    parser.add_argument(
        "-op",
        type=str,
        choices=["insert_fg", "create_td", "create_fv_td", "compute_stats"],
        help="Operation type",
    )
    parser.add_argument(
        "-path",
        type=str,
        help="Location on HopsFS of the JSON containing the full configuration",
    )

    args = parser.parse_args()
    job_conf = read_job_conf(args.path)

    if args.op == "insert_fg":
        insert_fg(spark, job_conf)
    elif args.op == "create_td":
        create_td(job_conf)
    elif args.op == "create_fv_td":
        create_fv_td(job_conf)
    elif args.op == "compute_stats":
        compute_stats(job_conf)

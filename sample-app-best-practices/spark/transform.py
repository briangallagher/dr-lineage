"""Real PySpark transformation instrumented by the native OpenLineage listener."""

from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as functions


def spark_uri(uri: str) -> str:
    return "s3a://" + uri.removeprefix("s3://") if uri.startswith("s3://") else uri


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-uri", required=True)
    parser.add_argument("--output-uri", required=True)
    parser.add_argument("--fail", action="store_true")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("transform-documents")
        .config("spark.hadoop.fs.s3a.endpoint", os.environ["S3_ENDPOINT"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.EnvironmentVariableCredentialsProvider",
        )
        .getOrCreate()
    )
    try:
        source = (
            spark.read.option("header", True)
            .option("inferSchema", False)
            .csv(spark_uri(args.input_uri))
        )
        required = {"title", "category", "text"}
        if set(source.columns) != required:
            raise ValueError(f"Expected columns {sorted(required)}, got {sorted(source.columns)}")
        transformed = source.select(
            functions.trim("title").alias("title"),
            functions.lower(functions.trim("category")).alias("category"),
            functions.lower(functions.regexp_replace(functions.trim("text"), r"\s+", " ")).alias(
                "normalized_text"
            ),
            functions.size(functions.split(functions.trim("text"), r"\s+")).alias("word_count"),
        )
        if args.fail:
            # Fail inside the data-bearing write. A Python-only exception is
            # invisible to the native listener, and a failing collect has no
            # output dataset for this listener version to represent as lineage.
            transformed = transformed.withColumn(
                "word_count",
                functions.expr(
                    "raise_error('Intentional Spark failure requested by failure_mode')"
                ).cast("int"),
            )
        transformed.write.mode("errorifexists").parquet(spark_uri(args.output_uri))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

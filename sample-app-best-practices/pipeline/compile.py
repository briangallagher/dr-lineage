"""Compile the KFP pipeline against immutable OpenShift image references."""

from __future__ import annotations

import argparse
from pathlib import Path

from kfp import compiler

from pipeline.pipeline import build_pipeline


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--app-image", required=True)
    parser.add_argument("--spark-image", required=True)
    parser.add_argument("--output", default="build/pipeline.yaml")
    args = parser.parse_args()
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    compiler.Compiler().compile(
        pipeline_func=build_pipeline(args.app_image, args.spark_image),
        package_path=str(output),
    )
    print(output)


if __name__ == "__main__":
    main()

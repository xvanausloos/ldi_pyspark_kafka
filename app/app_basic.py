import argparse
import os
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from common.ldi_logger import LdiLogger
from common import spark_utils


def main() -> None:
    logger = LdiLogger.getlogger("ldi_pyspark_kafka")
    logger.setLevel("DEBUG")

    spark = spark_utils.get_spark_session("ldi")

    # read catalog path to .CSV as argument & path to JSON with uc2_models_process list to be processed
    # parser = argparse.ArgumentParser()
    # parser.add_argument("--json_string", required=True)  # a string containing a JSON
    # parser.add_argument("--stage", required=False)
    # args = parser.parse_args()

    application = Application(spark=spark)
    application.read_json("drivers.json")
    print("end")


class Application:
    def __init__(self, spark: SparkSession) -> None:
        self._logger = LdiLogger.getlogger("ldi_python")
        self._spark = spark

    def read_json(self, filename: str) -> DataFrame:
        print(f"read file {filename}")

        # Set working directory to the directory of the script
        os.chdir(os.path.dirname(os.path.realpath(__file__)))
        # Define the relative path
        relative_path = Path('data') / filename

        df = self._spark.read.json(str(relative_path), multiLine=True)
        print(df.head(n=100))
        return df


if __name__ == "__main__":
    main()

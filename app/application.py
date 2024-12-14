import argparse
import datetime
import json
import tempfile
from typing import Dict, Any

import pandas as pd
import pyspark.sql.functions as F
from pyspark.shell import spark
from pyspark.sql import DataFrame, SparkSession


def main() -> None:
    # read catalog path to .CSV as argument & path to JSON with uc2_models_process list to be processed
    parser = argparse.ArgumentParser()
    parser.add_argument('--catalog_path')  # in S3 bucket s3://mirakl-data-science-dev/spark-uc2/
    parser.add_argument('--stage', required=False)
    args = parser.parse_args()

    catalog_path = args.catalog_path
    stage = args.stage
    conf = []

    spark = SparkSession.builder \
        .appName("ldi_pyspark_kafka") \
        .config("spark.hadoop.fs.s3a.access.key", "AKIAYP4V5JFL4B6TFV5V") \
        .config("spark.hadoop.fs.s3a.secret.key", "ginYhT1WS7xUNvZvSuAW7Xq0Jy1qJz7pMBM15TZ") \
    .master("local[*]") \
        .getOrCreate()

    Application(spark=spark, config=conf).run(catalog_path)


class Application:

    def __init__(self, spark: SparkSession, config: Dict[str, Any]) -> None:
        self._spark = spark
        self._config = config


    def run(self, catalog_path: str) -> None:
        self.process_catalog(catalog_path)


    def process_catalog(self, catalog_path: str) -> None:
        df = spark.read.csv(catalog_path)
        df.show()

    def flatten_output_data(self, df_results: DataFrame) -> DataFrame:
        """ Take a Pyspark Dataframe having a column OUTPUT_DATA and generate new columns using STRUCT string
         Use of Pandas is required for inferring a schema. Did not succeed to use Pure Pyspark for doing this.
         _> potential issue as this code will run on one executor """
        if 'output_data' in df_results.columns:
            df_results = df_results.filter(F.col('output_data').isNotNull())
            df_results_pd = df_results.toPandas()
            if len(df_results_pd) > 0:
                df_results_pd = df_results_pd.join(df_results_pd['output_data'].apply(lambda x: pd.Series(json.loads(x))))
                df_results = self._spark.createDataFrame(df_results_pd)
                df_results = df_results.drop("output_data")
        return df_results

    def mini_rows_batch_model_processing(self, df_catalog: DataFrame) -> int:
        """ This function calculate the number of partitions in order to ensure that model will take at least 10000 rows
         for processing and predict """
        batch_size = self._config.get("uc2-models.processing.batch-size")

        row_count = df_catalog.count()
        partition_count = round(row_count / int(batch_size))
        if partition_count < 1:
            res = 1
        else:
            res = partition_count
        self._logger.info(f"mini_rows_batch_model_processing - Nb catalog rows : {row_count}, Nb partitions: {res} ")
        return res


if __name__ == '__main__':
    main()

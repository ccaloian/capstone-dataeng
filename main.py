from pyspark.sql import SparkSession

import os

import dataio
import etl


DATA_DIR = "./data/processed"

def create_session():
    """Create SparkSession.

    Returns:
        SparkSession
    """
    spark = (SparkSession.builder
                .master("local[*]")
                .appName("ETL")
                .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")
                .config("spark.driver.memory", "15g")
                .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
                .getOrCreate()
    )
    return spark

def main():
    """Run ETL pipeline and save files.

    Output directory is './data/processed/'.
    """

    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    # dem_etl_pipeline = etl.DemographicsPipeline()
    # dem_df = dem_etl_pipeline.run()
    # dem_df.write\
    #     .mode("overwrite")\
    #     .parquet(f"{DATA_DIR}/demographics/demographics.parquet")

    imm_etl_pipeline = etl.ImmigrationPipeline()
    imm_df = imm_etl_pipeline.run()
    imm_df.write\
        .mode("overwrite")\
        .partitionBy("i94mon")\
        .parquet(f"{DATA_DIR}/immigration/immigration.parquet")

if __name__ == "__main__":
    main()

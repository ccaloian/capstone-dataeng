from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, StructField, StructType
from pyspark.sql.functions import col


def i94_sas_to_parquet(sas_dir: str, parquet_dir: str) -> dict:
    """Convert I94 Immigration data from SAS format to Parquet.

    The data preserves the month partition as in the SAS format.

    Note:
        This function should be ran ONLY ONCE, using the Spark session defined above!
        In the Udacity workspace the following relative paths should be used:
        sas_dir='../../data/18-83510-I94-Data-2016'
        parquet_dir='i94_data.parquet'

    Args:
        sas_dir (str): Input directory of SAS data.
        parquet_dir (str): Output directory name for parquet data.

    Returns:
        sas_info (dict): Dictionary containing SAS data information
            such as number of rows and column names per partition.
    """

    months = [
        "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"
    ]
    sas_info = {}

    spark = (SparkSession.builder
                .config("spark.jars.repositories", "https://repos.spark-packages.org/")
                .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")
                .appName("DataIO")
                .enableHiveSupport()
                .getOrCreate()
    )

    for i, month in enumerate(months):
        print(f"Processing month {month}...", end="")
        sas_info[month] = {}
        
        # read sas data
        df_spark = (spark
                    .read
                    .format("com.github.saurfang.sas.spark")
                    .load(f"{sas_dir}/i94_{month}16_sub.sas7bdat")
        )
        
        # cast to correct type
        df_spark = (df_spark
                    .withColumn("cicid", col("cicid").cast(IntegerType()))
                    .withColumn("i94yr", col("i94yr").cast(IntegerType()))
                    .withColumn("i94mon", col("i94mon").cast(IntegerType()))
                    .withColumn(
                        "i94cit", 
                        col("i94cit").cast(IntegerType()).cast(StringType())
                    )
                    .withColumn(
                        "i94res", 
                        col("i94res").cast(IntegerType()).cast(StringType())
                    )
                    .withColumn("arrdate", col("arrdate").cast(IntegerType()))
                    .withColumn(
                        "i94mode", 
                        col("i94mode").cast(IntegerType()).cast(StringType())
                    )
                    .withColumn("depdate", col("depdate").cast(IntegerType()))
                    .withColumn("i94bir", col("i94bir").cast(IntegerType()))
                    .withColumn(
                        "i94visa", 
                        col("i94visa").cast(IntegerType()).cast(StringType())
                    )
                    .withColumn("count", col("count").cast(IntegerType()))
                    .withColumn("biryear", col("biryear").cast(IntegerType()))
                    .withColumn("insnum", col("insnum").cast(IntegerType()))
                    .withColumn("admnum", col("admnum").cast(IntegerType()))
        )
        
        # gather sas data info
        sas_info[month]["count"] = df_spark.count()
        sas_info[month]["columns"] = df_spark.columns

        # write to parquet
        df_spark.write.mode("append").parquet(f"{parquet_dir}/i94mon={i+1}")
        print("DONE")

        return sas_info


def read_demographics_raw(spark, filename):
    """Read US demographics unprocessed dataset.

    Args:
        spark (SparkSession): Spark session.
        filename (str): File name of the demographics data.
    Returns:
        pyspark.sql.dataframe.DataFrame
    """
    df = (spark
            .read
            .csv(filename, sep=";", header=True)
    )

    return df


def rad_immigration_raw(spark, filename):
    """Read US immigration (parquet) unprocessed dataset.

    Args:
        spark (SparkSession): Spark session.
        filename (str): File name of the immigration data (parquet).
            For a specific partition (e.g. 4), provide the filename as:
            filename='i94_data.parquet/i94mon=4'
    Returns:
        pyspark.sql.dataframe.DataFrame
    """
    schema = StructType([
                StructField('cicid', IntegerType()),
                StructField('i94yr', IntegerType()),
                StructField('i94mon', IntegerType()),
                StructField('i94cit', StringType()),
                StructField('i94res', StringType()),
                StructField('i94port', StringType()),
                StructField('arrdate', IntegerType()),
                StructField('i94mode', StringType()),
                StructField('i94addr', StringType()),
                StructField('depdate', IntegerType()),
                StructField('i94bir', IntegerType()),
                StructField('i94visa', StringType()),
                StructField('count', IntegerType()),
                StructField('dtadfile', StringType()),
                StructField('visapost', StringType()),
                StructField('occup', StringType()),
                StructField('entdepa', StringType()),
                StructField('entdepd', StringType()),
                StructField('entdepu', StringType()),
                StructField('matflag', StringType()),
                StructField('biryear', IntegerType()),
                StructField('dtaddto', StringType()),
                StructField('gender', StringType()),
                StructField('insnum', IntegerType()),
                StructField('airline', StringType()),
                StructField('admnum', IntegerType()),
                StructField('fltno', StringType()),
                StructField('visatype', StringType()),
    ])

    df = (spark
            .read
            .schema(schema)
            .parquet(filename)
    )

    return df

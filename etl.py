from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DateType

from itertools import chain
import datetime

import dataio


class DemographicsPipeline:

    def __init__(self):
        self.spark = (SparkSession.builder
                        .master("local[*]")
                        .appName("ETL")
                        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")
                        .config("spark.driver.memory", "15g")
                        .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
                        .getOrCreate()
        )
        self.filename = "./data/raw/demographics/us-cities-demographics.csv"

    def read_demographics(self):
        """Read raw demographics data and return Spark DataFrame."""
        return dataio.read_demographics_raw(self.spark, self.filename)

    def get_population_total(self, dem_df):
        """Extract total population information per city.
        
        Args:
            dem_df (Spark.DataFrame): Raw demographics data.

        Returns:
            Spark.DataFrame
        """
        df = (dem_df
                .select("City", "State", "State Code", "Total Population")
                .dropDuplicates()
        )

        return df

    def get_population_race(self, dem_df):
        """Extract population per race per city.

        Pivot the `Race` column to obtain a dataset with `City` and `State` 
        as rows and demographic information as columns.

        Args:
            dem_df (Spark.DataFrame): Raw demographics data.

        Returns:
            Spark.DataFrame
        """
        df = (dem_df
                .withColumn("Count", F.col("Count").cast("int"))
                .groupBy("City", "State")
                .pivot("Race")
                .sum("Count")
                )

        return df

    def transform(self, population_total_df, population_race_df):
        """Join total population with race population.

        Args:
            population_total_df (Spark.DataFrame): Total population per city.
            population_race_df (Spark.DataFrame): Total population per race 
                per city.
        Returns:
            Spark.DataFrame
        """
        df = population_total_df.join(
                population_race_df, 
                on=["City", "State"], how="outer"
        )

        df = (
            df
            .withColumnRenamed("State", "state")
            .withColumnRenamed("State Code", "state_code")
            .groupBy("state", "state_code")
            .agg(
                F.count("City").alias("num_cities"),
                F.sum("Total Population").alias("total_pop"),
                F.sum("American Indian and Alaska Native").alias("amind_pop"),
                F.sum("Asian").alias("asian_pop"),
                F.sum("Black or African-American").alias("afram_pop"),
                F.sum("Hispanic or Latino").alias("hispl_pop"),
                F.sum("White").alias("white_pop")
            )
        )

        return df

    def quality_checks(self, df):
        """Run quality checks on the final dataset.

        Args:
            df (Spark.DataFrame): Final dataset.
        """

        assert df.count() > 0, "Quality Check Failed! Dataset is empty."
        
        assert df.where(F.col("state_code").isNull()).count() == 0 , \
            "Quality Check Failed! 'state_code' contains NULLs."
        
        assert df.where(F.col("total_pop").isNull()).count() == 0 , \
            "Quality Check Failed! 'total_pop' contains NULLs."
        
        assert df.where(F.col("total_pop") < 0).count() == 0 , \
            "Quality Check Failed! 'total_pop' contains negative values."
        

    def run(self):
        """Run demographics ETL pipeline.

        Returns:
            Spark.DataFrame: Transformed demographics dataset.
        """
        dem_df = self.read_demographics()
        df = self.transform(
                    self.get_population_total(dem_df),
                    self.get_population_race(dem_df)
        )

        self.quality_checks(df)

        return df


class ImmigrationPipeline:

    def __init__(self):
        self.spark = (SparkSession.builder
                        .master("local[*]")
                        .appName("ETL")
                        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0")
                        .config("spark.driver.memory", "15g")
                        .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
                        .getOrCreate()
        )
        self.filename = "./data/raw/immigration/i94_data.parquet"

    @staticmethod
    def sas_code_mapper(f_content, idx):
        """Parse SAS file and extract dataset codes for index.

        Args:
            f_content (str): Content of `I94_SAS_Labels_Description.sas` file.
            idx (str): Substring indicating the mapping.

        Returns:
            dict: Mapping between codes and corresponding values.
        """
        f_content2 = f_content[f_content.index(idx):]
        f_content2 = f_content2[:f_content2.index(';')].split('\n')
        f_content2 = [i.replace("'", "") for i in f_content2]
        dic = [i.split('=') for i in f_content2[1:]]
        dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
        return dic

    @staticmethod
    def convert_sas_date_(days):
        """Convert date from SAS format to Python date.

        Args:
            days (int): Number of days from 1960-1-1

        Returns:
            datetime.date: Python date.
        """
        sas_ref_date = datetime.date(year=1960, month=1, day=1)
        if days:
            date = sas_ref_date + datetime.timedelta(days=days)
        else:
            date = None
        return date

    def read_immigration(self):
        """Read immigration dataset and keep only columns of interest.

        Returns:
            Spark.DataFrame
        """
        imm = dataio.read_immigration_raw(self.spark, self.filename)

        keep_cols = [
            'cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res', 'arrdate', 'i94addr', 'depdate', 'dtadfile',
            'i94bir', 'biryear', 'gender', 'count', 'dtaddto', 'i94visa', 'visatype', 'admnum'
        ]
        imm = imm.select(*keep_cols).na.drop(how="any")

        return imm

    def replace_codes(self, imm_df):
        """Replace codes with values provided in `I94_SAS_Labels_Descriptions.sas`.

        Args:
            imm_df (Spark.DataFrame): Immigration dataframe.

        Returns:
            Spark.DataFrame
        """
        with open('./data/raw/immigration/I94_SAS_Labels_Descriptions.sas') as f:
            f_content = f.read()
            f_content = f_content.replace('\t', '')
            
            i94cit_res = ImmigrationPipeline.sas_code_mapper(f_content, "i94cntyl")
            i94addr = ImmigrationPipeline.sas_code_mapper(f_content, "i94addrl")
            i94visa = {'1': 'Business', '2': 'Pleasure', '3': 'Student'}

        i94ctrs_map = F.create_map([F.lit(x) for x in chain(*i94cit_res.items())])
        i94addr_map = F.create_map([F.lit(x) for x in chain(*i94addr.items())])
        i94visa_map = F.create_map([F.lit(x) for x in chain(*i94visa.items())])

        imm_df = (imm_df
                    .withColumn("i94cit", i94ctrs_map[F.col("i94cit")])
                    .withColumn("i94res", i94ctrs_map[F.col("i94res")])
                    .withColumn("state", i94addr_map[F.col("i94addr")])
                    .withColumn("i94visa", i94visa_map[F.col("i94visa")])
        )

        return imm_df.na.drop(how="any")

    def convert_dates(self, imm_df):
        """Convert date columns to DateType.

        This includes two types of transformations:
            * Date in SAS format
            * Dates in string format

        Args:
            imm_df (Spark.DataFrame): Immigration dataframe.

        Returns:
            Spark.DataFrame
        """
        
        # convert dates in SAS format
        convert_sas_date = F.udf(
            lambda x: ImmigrationPipeline.convert_sas_date_(x), DateType()
        )

        imm_df = (imm_df
                    .withColumn("arrdate", convert_sas_date(F.col("arrdate")))
                    .withColumn("depdate", convert_sas_date(F.col("depdate")))
        )

        # convert string dates
        imm_df = (imm_df
                    .withColumn(
                        "dtaddto", 
                        F.when(F.substring(F.col("dtaddto"), -4, 4) == "9999", None)
                        .otherwise(F.to_date(F.col("dtaddto"), "MMddyyyy"))
                    )
                    .withColumn(
                        "dtadfile",
                        F.when(F.substring(F.col("dtadfile"), 0, 4) == "9999", None)
                        .otherwise(F.to_date(F.col("dtadfile"), "yyyyMMdd"))
                    )
        )

        return imm_df.na.drop(how="any")

    def transform(self, imm_df):
        """Apply all transformations to the immigration dataset.

        Args:
            imm_df (Spark.DataFrame): Immigration dataframe.

        Returns:
            Spark.DataFrame
        """
        imm_df = self.replace_codes(imm_df)
        imm_df = self.convert_dates(imm_df)

        return imm_df

    def quality_checks(self, df):
        """Run quality checks on the final dataset.

        Args:
            df (Spark.DataFrame): Final dataset.
        """

        assert df.count() > 0, "Quality Check Failed! Dataset is empty."
        
        assert df.where(F.col("i94cit").isNull()).count() == 0 , \
            "Quality Check Failed! 'i94cit' contains NULLs."
        
        assert df.where(F.col("i94visa").isNull()).count() == 0 , \
            "Quality Check Failed! 'i94visa' contains NULLs."
        
        assert df.where(F.col("state").isNull()).count() == 0 , \
            "Quality Check Failed! 'state' contains NULLs."
        

    def run(self):
        """Run immigration ETL pipeline.

        Returns:
            Spark.DataFrame: Transformed immigration dataset.
        """
        imm_df = self.read_immigration()

        df = self.transform(imm_df)
        
        self.quality_checks(df)

        return df

from pyspark.sql import SparkSession


class DemographicsPipeline:

    def __init__(self):
        self.spark_session = (SparkSession.builder
                                          .master("local[*]")
                                          .appName("EnglandCouncilsJob")
                                          .getOrCreate())
        self.input_directory = "data"

    def extract_councils(self):
        pass

    def extract_avg_price(self):
        pass

    def extract_sales_volume(self):
        pass

    def transform(self, councils_df, avg_price_df, sales_volume_df):
        pass

    def run(self):
        return self.transform(self.extract_councils(),
                              self.extract_avg_price(),
                              self.extract_sales_volume())


class ImmigrationPipeline:

    def __init__(self):
        self.spark_session = (SparkSession.builder
                                          .master("local[*]")
                                          .appName("EnglandCouncilsJob")
                                          .getOrCreate())
        self.input_directory = "data"

    def extract_councils(self):
        pass

    def extract_avg_price(self):
        pass

    def extract_sales_volume(self):
        pass

    def transform(self, councils_df, avg_price_df, sales_volume_df):
        pass

    def run(self):
        return self.transform(self.extract_councils(),
                              self.extract_avg_price(),
                              self.extract_sales_volume())

from pyspark import SparkContext
from pyspark.sql import SparkSession


class SparkHiveContext:

    def __init__(self, app_name, metastore_adr):
        SparkContext.setSystemProperty("hive.metastore.uris", metastore_adr)
        self.app_name = app_name
        self.spark_session = SparkSession.builder.appName(self.app_name).enableHiveSupport().getOrCreate()

    def get_session(self):
        return self.spark_session

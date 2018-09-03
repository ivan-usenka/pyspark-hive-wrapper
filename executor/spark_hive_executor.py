from executor.context import SparkHiveContext


class SparkHiveExecutor:

    __app_name = 'app'
    __hive_metastore_adr = 'thrift://sandbox-hdp.hortonworks.com:9083'
    __sc = SparkHiveContext(__app_name, __hive_metastore_adr)

    def __init__(self):
        pass

    @staticmethod
    def execute_sql(sql_string):
        df = SparkHiveExecutor.__sc.get_session().sql(sql_string)
        return df

from executor.context import spark_context as sc


class SparkHiveExecutor:

    def __init__(self):
        pass

    @staticmethod
    def execute_sql(sql_string):
        return sc.get_session().sql(sql_string)

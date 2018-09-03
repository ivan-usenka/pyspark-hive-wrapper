from executor import SparkHiveExecutor
from collector import SparkHiveDataFrameCollector


class SparkHiveApiService:
    def __init__(self):
        pass

    @staticmethod
    def execute_sql(sql_string):
        df = SparkHiveExecutor.execute_sql(sql_string=sql_string)
        json_df = SparkHiveDataFrameCollector.collect_to_json(df)
        return SparkHiveApiService.print_data_frame(json_df)

    # TODO fill response correctly
    @staticmethod
    def print_data_frame(dataframe):
        for part in dataframe:
            print(part)

class SparkHiveDataFrameCollector:
    def __init__(self):
        pass

    @staticmethod
    def collect_to_json(dataframe):
        return dataframe.toJSON().collect()

    @staticmethod
    def save_to_json(dataframe, path):
        return dataframe.write.json(path)

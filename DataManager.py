def show_df(df):
    df.show()


class DataManager:
    ROOT_PATH = "C:/BigDataLocalSetup/data/tables/"

    def __init__(self, spark_session):
        self.spark = spark_session

    def read_parquet_files(self, table_name):
        file_path = self.ROOT_PATH + table_name
        return self.spark.read.parquet(file_path)

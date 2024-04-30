import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

spark = SparkSession.builder.appName("integrity-tests").getOrCreate()

# テーブル操作関数
def create_table_schema(columns):
    schema = ""
    for column in columns:
        schema += f"{column[0]} {column[1]},\n"
    return schema[:-2]  # 最後のカンマと改行を削除

def create_schema(schema_name):
    query = f"CREATE SCHEMA IF NOT EXISTS {schema_name};"
    spark.sql(query)
    return True

def truncate_table(schema_name, table_name):
    query = f"TRUNCATE TABLE {schema_name}.{table_name};"
    spark.sql(query)
    return True

def drop_table(schema_name, table_name):
    query = f"DROP TABLE {schema_name}.{table_name};"
    spark.sql(query)
    return True


# TODO
    # CSVを用意
    # insertの初期化時にテーブルをログに表示
    # 結果のテーブル結果想定テーブルも表示
    # パイプラインと結果確認のコードを１つのジョブとする
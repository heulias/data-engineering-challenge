from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name


DATA_PATH = "/opt/airflow/data/yellow_tripdata_2022-*.parquet.gz"

JDBC_URL = "jdbc:postgresql://postgres:5432/taxi_db"

DB_PROPERTIES = {
    "user": "wecogno",
    "password": "wecogno",
    "driver": "org.postgresql.Driver",
}


def main():

    spark = (
        SparkSession.builder
        .appName("NYC-Taxi-RAW")
        .getOrCreate()
    )

    print("=== INICIANDO CAMADA RAW ===")

    # Leitura dos arquivos Parquet
    df = spark.read.parquet(DATA_PATH)

    # Metadados de ingestão
    df = (
        df
        .withColumn("source_file", input_file_name())
        .withColumn("ingestion_timestamp", current_timestamp())
    )

    total = df.count()

    print(f"Total de registros encontrados: {total}")

    # Gravação no PostgreSQL
    (
        df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", "raw.yellow_taxi")
        .option("user", DB_PROPERTIES["user"])
        .option("password", DB_PROPERTIES["password"])
        .option("driver", DB_PROPERTIES["driver"])
        .option("batchsize", "10000")
        .option("numPartitions", "8")
        .mode("overwrite")
        .save()
    )

    print("=== CAMADA RAW FINALIZADA ===")

    spark.stop()


if __name__ == "__main__":
    main()
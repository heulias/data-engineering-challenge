
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

# ==========================================
# CONFIGURACOES DO POSTGRESQL
# ==========================================

JDBC_URL = "jdbc:postgresql://postgres:5432/taxi_db"

DB_PROPERTIES = {
    "user": "wecogno",
    "password": "wecogno",
    "driver": "org.postgresql.Driver"
}

TRUSTED_TABLE = "trusted.yellow_taxi"
REFINED_TABLE = "refined.yellow_taxi"


def main():

    # ==========================================
    # INICIALIZACAO DO SPARK
    # ==========================================

    spark = (
        SparkSession.builder
        .appName("NYC-Taxi-REFINED")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=== INICIANDO CAMADA REFINED ===", flush=True)

    try:

        # ==========================================
        # LEITURA JDBC PARTICIONADA
        # ==========================================

        print("Lendo dados da camada TRUSTED...", flush=True)

        df = (
            spark.read
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", TRUSTED_TABLE)
            .option("user", DB_PROPERTIES["user"])
            .option("password", DB_PROPERTIES["password"])
            .option("driver", DB_PROPERTIES["driver"])
            .option("partitionColumn", "tpep_pickup_datetime")
            .option("lowerBound", "2022-01-01 00:00:00")
            .option("upperBound", "2023-01-01 00:00:00")
            .option("numPartitions", "12")
            .option("fetchsize", "1000")
            .load()
        )

        print(
            "Leitura JDBC configurada com 12 particoes.",
            flush=True
        )

        # ==========================================
        # TRANSFORMACOES DA CAMADA REFINED
        # ==========================================

        print("Aplicando transformacoes...", flush=True)

        refined_df = (
            df
            .withColumn(
                "pickup_date",
                to_date(col("tpep_pickup_datetime"))
            )
            .withColumn(
                "dropoff_date",
                to_date(col("tpep_dropoff_datetime"))
            )
            .withColumn(
                "trip_duration_minutes",
                (
                    col("tpep_dropoff_datetime").cast("long")
                    - col("tpep_pickup_datetime").cast("long")
                ) / 60.0
            )
        )

        # ==========================================
        # GRAVACAO DA CAMADA REFINED
        # ==========================================

        print("Gravando dados na camada REFINED...", flush=True)

        (
            refined_df.write
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", REFINED_TABLE)
            .option("user", DB_PROPERTIES["user"])
            .option("password", DB_PROPERTIES["password"])
            .option("driver", DB_PROPERTIES["driver"])
            .option("batchsize", "5000")
            .option("numPartitions", "4")
            .mode("overwrite")
            .save()
        )

        print("=== CAMADA REFINED FINALIZADA ===", flush=True)

    except Exception as error:
        print(f"ERRO NA CAMADA REFINED: {error}", flush=True)
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

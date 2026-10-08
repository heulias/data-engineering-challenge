
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ==========================================
# CONFIGURACOES DO POSTGRESQL
# ==========================================

JDBC_URL = "jdbc:postgresql://postgres:5432/taxi_db"

DB_PROPERTIES = {
    "user": "wecogno",
    "password": "wecogno",
    "driver": "org.postgresql.Driver"
}

RAW_TABLE = "raw.yellow_taxi"
TRUSTED_TABLE = "trusted.yellow_taxi"


def main():

    # ==========================================
    # INICIALIZACAO DO SPARK
    # ==========================================

    spark = (
        SparkSession.builder
        .appName("NYC-Taxi-TRUSTED")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("=== INICIANDO CAMADA TRUSTED ===", flush=True)

    try:

        # ==========================================
        # LEITURA JDBC PARTICIONADA
        # ==========================================

        print("Lendo dados da camada RAW...", flush=True)

        df = (
            spark.read
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", RAW_TABLE)
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

        print("Leitura JDBC configurada com 12 particoes.", flush=True)

        # ==========================================
        # VALIDACAO E LIMPEZA DOS DADOS
        # ==========================================

        print("Aplicando regras de qualidade...", flush=True)

        trusted_df = (
            df
            .filter(col("tpep_pickup_datetime").isNotNull())
            .filter(col("tpep_dropoff_datetime").isNotNull())
            .filter(col("trip_distance").isNotNull())
            .filter(col("trip_distance") >= 0)
            .filter(
                col("tpep_dropoff_datetime")
                >= col("tpep_pickup_datetime")
            )
        )

        # ==========================================
        # GRAVACAO DA CAMADA TRUSTED
        # ==========================================

        print("Gravando dados na camada TRUSTED...", flush=True)

        (
            trusted_df.write
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", TRUSTED_TABLE)
            .option("user", DB_PROPERTIES["user"])
            .option("password", DB_PROPERTIES["password"])
            .option("driver", DB_PROPERTIES["driver"])
            .option("batchsize", "5000")
            .option("numPartitions", "4")
            .mode("overwrite")
            .save()
        )

        print("=== CAMADA TRUSTED FINALIZADA ===", flush=True)

    except Exception as error:
        print(f"ERRO NA CAMADA TRUSTED: {error}", flush=True)
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()

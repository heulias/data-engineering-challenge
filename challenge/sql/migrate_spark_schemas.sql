-- EXECUTAR UMA UNICA VEZ, somente com a DAG parada.
-- APAGA DEFINITIVAMENTE os schemas antigos raw/trusted/refined
-- e renomeia os schemas que ja contem os dados processados com Spark.
BEGIN;
DROP SCHEMA IF EXISTS raw CASCADE;
DROP SCHEMA IF EXISTS trusted CASCADE;
DROP SCHEMA IF EXISTS refined CASCADE;
ALTER SCHEMA raw_spark RENAME TO raw;
ALTER SCHEMA trusted_spark RENAME TO trusted;
ALTER SCHEMA refined_spark RENAME TO refined;
COMMIT;

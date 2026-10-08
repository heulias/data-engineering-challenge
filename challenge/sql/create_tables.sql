-- Schemas oficiais do pipeline PySpark.
-- As tabelas sao criadas pelos jobs Spark via JDBC (mode=overwrite).
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS trusted;
CREATE SCHEMA IF NOT EXISTS refined;

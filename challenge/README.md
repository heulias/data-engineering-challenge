# Desafio de Engenharia de Dados — WeCogno

Pipeline de ETL para os dados de corridas **NYC Yellow Taxi (2022)**, com orquestração no **Apache Airflow**, processamento em **PySpark** e persistência no **PostgreSQL**, executados localmente com **Docker Compose**.

## 1. Tecnologias

- Docker e Docker Compose — provisionamento do ambiente.
- Apache Airflow 2.10.5 — orquestração da DAG e acompanhamento de execução.
- Apache Spark / PySpark 3.5.1 — leitura, validação e transformação dos dados.
- PostgreSQL 16 — armazenamento das camadas e metadados do Airflow (em instâncias separadas).
- Python e SQL — implementação e validação dos resultados.

## 2. Arquitetura e fluxo

```text
NYC Yellow Taxi 2022 (12 arquivos Parquet compactados)
                         |
                         v
               Airflow: nyc_taxi_pipeline
                         |
                         v
              PySpark RAW (ingestão)
                         |
                         v
               PostgreSQL: raw.yellow_taxi
                         |
                         v
          PySpark TRUSTED (regras de qualidade)
                         |
                         v
             PostgreSQL: trusted.yellow_taxi
                         |
                         v
           PySpark REFINED (campos derivados)
                         |
                         v
             PostgreSQL: refined.yellow_taxi
                         |
                         v
                  Consultas SQL
```

A DAG executa sequencialmente `start_pipeline >> raw >> trusted >> refined >> end_pipeline`, impedindo o início da próxima camada antes do sucesso da anterior.

### RAW — ingestão

- Leitura dos 12 arquivos `yellow_tripdata_2022-*.parquet.gz` com Spark.
- Inclusão de `source_file` para rastrear o arquivo de origem e `ingestion_timestamp` para registrar a ingestão.
- Escrita JDBC em `raw.yellow_taxi`.

### TRUSTED — qualidade

Os dados são lidos da RAW por JDBC e submetidos às seguintes validações:

- `tpep_pickup_datetime` não nulo;
- `tpep_dropoff_datetime` não nulo;
- `trip_distance` não nulo;
- `trip_distance >= 0`;
- `tpep_dropoff_datetime >= tpep_pickup_datetime`.

As linhas aprovadas são armazenadas em `trusted.yellow_taxi`.

### REFINED — enriquecimento

A partir da TRUSTED, são criadas as colunas:

- `pickup_date`: data de início da viagem;
- `dropoff_date`: data de término da viagem;
- `trip_duration_minutes`: duração em minutos, calculada pela diferença entre os timestamps.

A saída final é `refined.yellow_taxi`.

## 3. Organização dos arquivos

```text
data-engineering-challenge/
├── nyc-tlc-data/
│   └── yellow_tripdata_2022-*.parquet.gz
└── challenge/
    ├── README.md
    ├── docker-compose.yml
    ├── Dockerfile
    ├── dags/
    │   └── nyc_taxi_pipeline.py
    ├── jobs/
    │   ├── raw.py
    │   ├── trusted.py
    │   └── refined.py
    ├── sql/
    │   └── create_tables.sql
    └── logs/                  # Gerado localmente
```

Os arquivos Parquet devem estar no diretório `nyc-tlc-data` na raiz do repositório. A montagem de volume os disponibiliza aos containers em `/opt/airflow/data`.

## 4. Como executar

**Pré-requisitos:** Docker com Docker Compose e recursos suficientes para processar aproximadamente 40 milhões de registros.

No terminal, a partir da raiz do repositório:

```bash
cd challenge
mkdir -p logs
docker compose up -d --build
```

Acesse o Airflow em **http://localhost:8080** (credenciais de desenvolvimento configuradas no `docker-compose.yml`).

Ative a DAG `nyc_taxi_pipeline` e execute-a manualmente pela interface. Alternativamente:

```bash
docker compose exec airflow-scheduler airflow dags unpause nyc_taxi_pipeline
docker compose exec airflow-scheduler airflow dags trigger nyc_taxi_pipeline
```

Acompanhe as execuções:

```bash
docker compose exec airflow-scheduler airflow dags list-runs -d nyc_taxi_pipeline
```

Para abrir o PostgreSQL:

```bash
docker compose exec postgres psql -U wecogno -d taxi_db
```

> **Importante:** caso o ambiente esteja sendo criado do zero, os schemas `raw`, `trusted` e `refined` devem existir antes da primeira execução. Consulte o script de inicialização SQL do projeto. Não use `docker compose down -v` se desejar preservar os dados dos volumes.

## 5. Decisões técnicas

1. **Camadas RAW, TRUSTED e REFINED:** separação entre ingestão, validação e disponibilização dos dados para análise.
2. **PySpark:** processamento dos arquivos e transformações com API de DataFrames.
3. **Leitura JDBC particionada:** leitura das camadas intermediárias com particionamento para trabalhar com grandes volumes.
4. **Gravação JDBC em lotes:** controle do volume por lote e do paralelismo para adequação aos recursos do ambiente local.
5. **Carga completa com `overwrite`:** a execução substitui as tabelas de destino, simplificando a reexecução integral do desafio; não implementa carga incremental.
6. **Rastreabilidade:** colunas de arquivo de origem e timestamp de ingestão na RAW.
7. **Regras explícitas de qualidade:** apenas as condições descritas na seção TRUSTED são utilizadas para exclusão de linhas.

**Observação sobre qualidade:** os arquivos incluem registros com datas fora de 2022 e valores extremos de distância. Como não foi definida uma regra de negócio para eliminar esses casos, eles foram preservados quando atendiam às validações implementadas. A maior distância registrada deve ser interpretada como um valor presente na fonte, não como uma viagem necessariamente plausível.

## 6. Validação das camadas

Consultas realizadas no PostgreSQL após o processamento:

```sql
SELECT 'RAW' AS camada, COUNT(*) AS total FROM raw.yellow_taxi
UNION ALL
SELECT 'TRUSTED', COUNT(*) FROM trusted.yellow_taxi
UNION ALL
SELECT 'REFINED', COUNT(*) FROM refined.yellow_taxi;
```

| Camada | Registros |
|---|---:|
| RAW | 39.656.098 |
| TRUSTED | 39.642.485 |
| REFINED | 39.642.485 |

**13.613 registros** não passaram pelas regras de qualidade. A quantidade da REFINED corresponde à quantidade da TRUSTED.

## 7. Respostas às questões do desafio

As consultas abaixo usam a tabela final `refined.yellow_taxi` e foram executadas no PostgreSQL.

### Questão 1 — Qual o total de registros na tabela final?

```sql
SELECT COUNT(*) AS total_registros
FROM refined.yellow_taxi;
```

**Resposta: 39.642.485 registros.**

### Questão 2 — Qual o total de viagens iniciadas e finalizadas em 17 de junho?

Foi considerado **17 de junho de 2022**; início e término são contados independentemente.

```sql
SELECT
    COUNT(*) FILTER (
        WHERE tpep_pickup_datetime >= '2022-06-17'
          AND tpep_pickup_datetime < '2022-06-18'
    ) AS viagens_iniciadas,
    COUNT(*) FILTER (
        WHERE tpep_dropoff_datetime >= '2022-06-17'
          AND tpep_dropoff_datetime < '2022-06-18'
    ) AS viagens_finalizadas
FROM refined.yellow_taxi;
```

**Resposta: 125.586 viagens iniciadas e 125.342 viagens finalizadas.**

### Questão 3 — Qual foi o dia da viagem mais longa percorrida?

A interpretação adotada é a viagem com a **maior distância registrada** em `trip_distance`.

```sql
SELECT
    tpep_pickup_datetime::date AS data_viagem,
    trip_distance AS maior_distancia
FROM refined.yellow_taxi
ORDER BY trip_distance DESC
LIMIT 1;
```

**Resposta: 28/10/2022, com distância registrada de 389.678,46 milhas.** Esse é um valor extremo da fonte e foi mantido conforme as regras adotadas.

### Questão 4 — Média, desvio padrão, mínimo, máximo e quartis da distância percorrida

```sql
SELECT
    ROUND(AVG(trip_distance)::numeric, 4) AS media,
    ROUND(STDDEV_SAMP(trip_distance)::numeric, 4) AS desvio_padrao,
    MIN(trip_distance) AS minimo,
    PERCENTILE_CONT(0.25) WITHIN GROUP (
        ORDER BY trip_distance
    ) AS q1,
    PERCENTILE_CONT(0.50) WITHIN GROUP (
        ORDER BY trip_distance
    ) AS mediana,
    PERCENTILE_CONT(0.75) WITHIN GROUP (
        ORDER BY trip_distance
    ) AS q3,
    MAX(trip_distance) AS maximo
FROM refined.yellow_taxi;
```

| Estatística | Resultado (milhas) |
|---|---:|
| Média | 5,9594 |
| Desvio padrão amostral | 599,2936 |
| Mínimo | 0 |
| Primeiro quartil (Q1) | 1,10 |
| Segundo quartil (mediana) | 1,90 |
| Terceiro quartil (Q3) | 3,56 |
| Máximo | 389.678,46 |

O desvio padrão foi calculado com `STDDEV_SAMP`, e os quartis com `PERCENTILE_CONT`.

## 8. Considerações finais

O pipeline foi executado com dados dos 12 meses de 2022 e os resultados das quatro questões foram conferidos por consultas SQL no PostgreSQL. Como evoluções possíveis, destacam-se testes automatizados de qualidade, parametrização de credenciais, cargas incrementais e regras adicionais para tratamento de anomalias.

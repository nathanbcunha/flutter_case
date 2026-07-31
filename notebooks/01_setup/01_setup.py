# Databricks notebook source
# MAGIC %md
# MAGIC # 01_setup — Ambiente e utilitários compartilhados
# MAGIC
# MAGIC Depende de `00_config` já ter sido executado no mesmo escopo (path relativo abaixo).
# MAGIC Cria o catálogo/schemas (se estiver no Databricks) e define funções reutilizadas por
# MAGIC todos os notebooks de bronze/silver/gold — para não duplicar lógica de escrita/log em
# MAGIC cada um dos ~15 notebooks de tabela.

# COMMAND ----------

# MAGIC %run ../00_config/00_config

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import datetime

spark = SparkSession.builder.appName("flutter_martech_case").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Criação de catálogo/schema (somente quando roda em Databricks/Unity Catalog)

# COMMAND ----------

def setup_catalog_and_schemas():
    """Garante que catalog e os 3 schemas medalhão existem antes de qualquer escrita.
    No-op em execução local (sem Unity Catalog) — nesse caso as tabelas viram
    diretórios de arquivo Parquet sob outputs/, conforme STORAGE_FORMAT."""
    if not RUNNING_ON_DATABRICKS:
        print("[setup] execução local — pulando criação de catálogo Unity Catalog")
        return
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    for schema in (BRONZE_SCHEMA, SILVER_SCHEMA, GOLD_SCHEMA):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
    print(f"[setup] catalog '{CATALOG}' e schemas bronze/silver/gold prontos")


setup_catalog_and_schemas()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Escrita padronizada de tabela (Delta em Databricks / Parquet em execução local)
# MAGIC
# MAGIC Centralizar a escrita garante que TODA tabela do pipeline segue o mesmo padrão de
# MAGIC metadata (coluna de auditoria `_ingested_at`, mesmo modo de escrita, mesmo tratamento
# MAGIC de path) sem cada notebook reimplementar isso.

# COMMAND ----------

# Também relativo à raiz do repositório — mesma premissa de working directory do RAW_DATA_PATH.
LOCAL_OUTPUT_ROOT = "outputs"


def write_table(df: DataFrame, layer: str, table_name: str, mode: str = "overwrite",
                 add_audit_columns: bool = True) -> str:
    """Escreve um DataFrame na camada especificada, seguindo o padrão medalhão do projeto.

    layer: 'bronze' | 'silver' | 'gold'
    Retorna o identificador/path onde a tabela foi escrita, para logging.
    """
    assert layer in ("bronze", "silver", "gold"), f"camada inválida: {layer}"

    if add_audit_columns:
        # Toda tabela carrega quando foi processada — essencial para auditoria (camada
        # bronze existe justamente para rastreabilidade) e para debugar problemas de
        # execução incremental em produção.
        df = df.withColumn("_ingested_at", F.current_timestamp())

    if RUNNING_ON_DATABRICKS:
        schema = {"bronze": BRONZE_SCHEMA, "silver": SILVER_SCHEMA, "gold": GOLD_SCHEMA}[layer]
        full_name = f"{CATALOG}.{schema}.{table_name}"
        (df.write.format("delta").mode(mode).option("mergeSchema", "true")
           .saveAsTable(full_name))
        print(f"[write_table] {full_name} ({df.count()} linhas)")
        return full_name
    else:
        path = f"{LOCAL_OUTPUT_ROOT}/{layer}/{table_name}"
        df.write.format(STORAGE_FORMAT).mode(mode).save(path)
        # Também grava um CSV legível para revisão humana rápida do case (o Parquet/Delta
        # não é diretamente abrível em Excel/Notepad). Em produção real isso não existiria
        # — é uma concessão só para tornar a entrega do case revisável fora do Databricks.
        csv_path = f"{LOCAL_OUTPUT_ROOT}/{layer}/{table_name}_csv"
        df.coalesce(1).write.format("csv").mode(mode).option("header", "true").save(csv_path)
        n = df.count()
        print(f"[write_table] {path} ({n} linhas)")
        return path


def read_table(layer: str, table_name: str) -> DataFrame:
    """Leitura simétrica a write_table — usada pelos notebooks de silver/gold para ler
    a camada anterior sem cada um saber o detalhe de path/catálogo."""
    if RUNNING_ON_DATABRICKS:
        schema = {"bronze": BRONZE_SCHEMA, "silver": SILVER_SCHEMA, "gold": GOLD_SCHEMA}[layer]
        return spark.table(f"{CATALOG}.{schema}.{table_name}")
    else:
        path = f"{LOCAL_OUTPUT_ROOT}/{layer}/{table_name}"
        return spark.read.format(STORAGE_FORMAT).load(path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logging simples de execução
# MAGIC
# MAGIC Em produção isso seria substituído por integração com o sistema de observabilidade
# MAGIC (ex.: Databricks Lakehouse Monitoring, ou logs estruturados para o Datadog/Splunk da
# MAGIC empresa). Aqui, um print padronizado já é suficiente para o escopo do case e deixa
# MAGIC claro onde a instrumentação de produção entraria.

# COMMAND ----------

def log_step(notebook: str, message: str):
    ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
    print(f"[{ts}] [{notebook}] {message}")


print("[01_setup] utilitários carregados: write_table, read_table, log_step")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conversão de câmbio para BRL
# MAGIC
# MAGIC Centralizado aqui (não duplicado em silver_bets e silver_deposits) porque a regra —
# MAGIC "qual taxa usar para qual data/moeda" — é uma única decisão de negócio que precisa
# MAGIC valer igual para toda tabela monetária do pipeline.

# COMMAND ----------

def convert_to_brl(df: DataFrame, amount_col: str, currency_col: str, ts_col: str,
                    output_col: str) -> DataFrame:
    """Adiciona `output_col` = valor de `amount_col` convertido para BRL, usando a cotação
    do dia da transação (bronze.fx_rates_dense_calendar, já com forward-fill de fim de
    semana/feriado aplicado).

    Regras:
      - moeda já BRL: fator de conversão = 1 (sem join necessário, sem risco de erro de
        arredondamento por uma cotação "BRL->BRL" artificial).
      - USD: taxa derivada localmente (eur_to_brl / eur_to_usd) — ver bronze_fx_rates.
      - moeda fora de EXPECTED_CURRENCIES: NÃO convertida (fica NULL) — melhor sinalizar
        um dado suspeito explicitamente do que aplicar uma taxa inventada.
    """
    df_fx = read_table("bronze", "fx_rates_dense_calendar")

    df_with_date = df.withColumn("_tx_date", F.to_date(F.col(ts_col)))

    df_joined = (
        df_with_date
        .join(df_fx, df_with_date["_tx_date"] == df_fx["rate_date"], "left")
        .withColumn(
            "_fx_factor",
            F.when(F.col(currency_col) == "BRL", F.lit(1.0))
             .when(F.col(currency_col) == "EUR", F.col("eur_to_brl"))
             .when(F.col(currency_col) == "USD", F.col("eur_to_brl") / F.col("eur_to_usd"))
             .otherwise(F.lit(None).cast("double")),
        )
        .withColumn(output_col, F.round(F.col(amount_col) * F.col("_fx_factor"), 2))
        .drop("_tx_date", "_fx_factor", "rate_date", "eur_to_brl", "eur_to_usd",
              "was_forward_filled", "_ingested_at", "_source_file")
    )
    return df_joined

# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — deposits
# MAGIC
# MAGIC **Grão pretendido:** 1 linha por depósito (`deposit_id`). O profiling encontrou **25
# MAGIC `deposit_id` duplicados exatos** (todas as colunas idênticas, não é conflito de dado).
# MAGIC
# MAGIC **Decisão de design: NÃO deduplicar aqui.** A Bronze existe para preservar o dado
# MAGIC exatamente como chegou da origem, duplicatas inclusive — é a garantia de auditoria do
# MAGIC medalhão ("o que a origem nos mandou, literalmente"). O dedup é uma regra de limpeza de
# MAGIC negócio e pertence à camada Prata, onde fica documentado e visível quantas linhas foram
# MAGIC removidas e por quê.

# COMMAND ----------

# MAGIC %run ../00_config/00_config

# COMMAND ----------

# MAGIC %run ../01_setup/01_setup

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

deposits_schema = StructType([
    StructField("deposit_id", StringType(), True),
    StructField("player_id", StringType(), True),
    StructField("deposit_ts", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("status", StringType(), True),
])

df_deposits_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(deposits_schema)
    .load(RAW_FILES["deposits"])
)

df_deposits_bronze = df_deposits_raw.withColumn("_source_file", F.col("_metadata.file_path"))

write_table(df_deposits_bronze, layer="bronze", table_name="deposits")

n_total = df_deposits_bronze.count()
n_distinct = df_deposits_bronze.select("deposit_id").distinct().count()
log_step("bronze_deposits",
          f"{n_total} linhas carregadas ({n_total - n_distinct} duplicatas de deposit_id "
          f"preservadas intencionalmente — dedup acontece na Prata)")

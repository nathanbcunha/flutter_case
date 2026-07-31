# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — campaign_touchpoints
# MAGIC
# MAGIC **Grão:** 1 linha por evento de touchpoint (`touchpoint_id`) — sent/open/click de uma
# MAGIC campanha para um jogador. Sem duplicatas de `touchpoint_id` no profiling; 100% de
# MAGIC integridade referencial com `players` e `campaigns` (nenhum órfão).
# MAGIC
# MAGIC **Achado do profiling que fica registrado aqui, tratado na Prata:** 2 touchpoints têm
# MAGIC `event_ts` posterior à data de referência do negócio (2024-04-01) — não removemos nada
# MAGIC na Bronze, apenas note-se para tratamento explícito adiante.

# COMMAND ----------

# MAGIC %run ../00_config/00_config

# COMMAND ----------

# MAGIC %run ../01_setup/01_setup

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

touchpoints_schema = StructType([
    StructField("touchpoint_id", StringType(), True),
    StructField("player_id", StringType(), True),
    StructField("campaign_id", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("event_ts", StringType(), True),
    StructField("event_type", StringType(), True),
])

df_touchpoints_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(touchpoints_schema)
    .load(RAW_FILES["campaign_touchpoints"])
)

df_touchpoints_bronze = df_touchpoints_raw.withColumn("_source_file", F.col("_metadata.file_path"))

write_table(df_touchpoints_bronze, layer="bronze", table_name="campaign_touchpoints")

log_step("bronze_campaign_touchpoints",
          f"{df_touchpoints_bronze.count()} linhas carregadas de {RAW_FILES['campaign_touchpoints']}")

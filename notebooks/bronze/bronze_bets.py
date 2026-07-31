# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — bets
# MAGIC
# MAGIC **Grão:** 1 linha por aposta (`bet_id`, chave primária — sem duplicatas confirmadas no
# MAGIC profiling). `stake` = valor apostado, `payout` = valor retornado ao jogador (0.00 quando
# MAGIC perde). GGR (receita da casa) só é calculado na Prata/Gold — aqui ficamos com os valores
# MAGIC crus, nas moedas originais (BRL/USD/EUR).

# COMMAND ----------

# MAGIC %run ../00_config/00_config

# COMMAND ----------

# MAGIC %run ../01_setup/01_setup

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

bets_schema = StructType([
    StructField("bet_id", StringType(), True),
    StructField("player_id", StringType(), True),
    StructField("bet_ts", StringType(), True),
    StructField("stake", StringType(), True),
    StructField("currency", StringType(), True),
    StructField("product", StringType(), True),
    StructField("payout", StringType(), True),
])

df_bets_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(bets_schema)
    .load(RAW_FILES["bets"])
)

df_bets_bronze = df_bets_raw.withColumn("_source_file", F.input_file_name())

write_table(df_bets_bronze, layer="bronze", table_name="bets")

log_step("bronze_bets", f"{df_bets_bronze.count()} linhas carregadas de {RAW_FILES['bets']}")

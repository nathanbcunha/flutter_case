# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — players
# MAGIC
# MAGIC **Grão:** 1 linha por jogador (`player_id`, chave primária confirmada no profiling —
# MAGIC sem duplicatas na origem).
# MAGIC
# MAGIC **Princípio da camada Bronze:** carregar exatamente como veio, sem corrigir nada — nem
# MAGIC os 22 valores nulos em `acquisition_channel` que já identificamos no profiling. Se
# MAGIC precisarmos auditar "o dado sempre teve esse buraco?" no futuro, a resposta vive aqui,
# MAGIC intacta. Toda limpeza (tratamento de nulo, etc.) acontece na Prata, nunca aqui.

# COMMAND ----------

# MAGIC %run ../00_config/00_config

# COMMAND ----------

# MAGIC %run ../01_setup/01_setup

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

# Schema explícito em vez de inferSchema=True: schema inferido é não-determinístico entre
# execuções (o Spark pode mudar a inferência se a ordem de leitura de arquivos variar) e é
# uma fonte clássica de bug silencioso em produção. Tudo como string na Bronze de propósito:
# a Bronze não deve tomar decisão de tipagem de negócio (isso é decisão da Prata, onde
# validamos e convertemos com regras explícitas).
players_schema = StructType([
    StructField("player_id", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("acquisition_channel", StringType(), True),
    StructField("country", StringType(), True),
    StructField("preferred_currency", StringType(), True),
    StructField("kyc_status", StringType(), True),
    StructField("self_excluded", StringType(), True),
])

df_players_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(players_schema)
    .load(RAW_FILES["players"])
)

# Coluna de rastreabilidade da origem — essencial em auditoria: de qual arquivo/carga
# exatamente essa linha veio.
df_players_bronze = df_players_raw.withColumn("_source_file", F.input_file_name())

write_table(df_players_bronze, layer="bronze", table_name="players")

log_step("bronze_players", f"{df_players_bronze.count()} linhas carregadas de {RAW_FILES['players']}")

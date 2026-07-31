# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — campaigns
# MAGIC
# MAGIC **Grão:** 1 linha por campanha (`campaign_id`). Esta é a tabela mais "suja" do case de
# MAGIC propósito — `campaign_name` tem separadores inconsistentes (`_` vs `-`), case
# MAGIC inconsistente, erros de digitação, segmentos faltando, ordem trocada, e até 1 nome vazio
# MAGIC e 1 texto livre sem relação nenhuma com o padrão oficial.
# MAGIC
# MAGIC **Nada disso é tratado aqui.** O parser de taxonomia (regex + vocabulário controlado)
# MAGIC vive no notebook `silver_campaigns` — Bronze só guarda o texto cru para que o parser
# MAGIC seja auditável e re-executável sobre a fonte original sempre que a lógica de parsing for
# MAGIC refinada, sem precisar re-ingerir do CSV de novo.

# COMMAND ----------

# MAGIC %run ../00_config/00_config

# COMMAND ----------

# MAGIC %run ../01_setup/01_setup

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

campaigns_schema = StructType([
    StructField("campaign_id", StringType(), True),
    StructField("campaign_name", StringType(), True),
    StructField("created_date", StringType(), True),
    StructField("status", StringType(), True),
])

df_campaigns_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(campaigns_schema)
    .load(RAW_FILES["campaigns"])
)

df_campaigns_bronze = df_campaigns_raw.withColumn("_source_file", F.input_file_name())

write_table(df_campaigns_bronze, layer="bronze", table_name="campaigns")

log_step("bronze_campaigns", f"{df_campaigns_bronze.count()} linhas carregadas de {RAW_FILES['campaigns']}")

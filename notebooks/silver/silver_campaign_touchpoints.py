# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — campaign_touchpoints
# MAGIC
# MAGIC **Imperfeição tratada:** 2 touchpoints com `event_ts` posterior à data de referência do
# MAGIC negócio (2024-04-01) — ex.: 2024-04-07.
# MAGIC
# MAGIC **Decisão:** NÃO removemos essas linhas — são um evento real que aconteceu (o dado é
# MAGIC legítimo, só está "no futuro" relativo à nossa régua de análise). Adicionamos a flag
# MAGIC `is_future_dated`, e qualquer agregação de comportamento *até* a data de referência
# MAGIC (ex.: "quantos touchpoints o jogador recebeu antes de ficar dormente") filtra por essa
# MAGIC flag explicitamente na Gold. Isso evita vazamento de informação futura ("data leakage")
# MAGIC na análise de dormência sem descartar dado que pode ser útil para outras perguntas.

# COMMAND ----------

# MAGIC %run ../00_config/00_config

# COMMAND ----------

# MAGIC %run ../01_setup/01_setup

# COMMAND ----------

df_touchpoints_bronze = read_table("bronze", "campaign_touchpoints")

df_touchpoints_silver = (
    df_touchpoints_bronze
    .dropDuplicates(["touchpoint_id"])  # defensivo — nenhuma encontrada no profiling
    .withColumn("event_ts", F.col("event_ts").cast("timestamp"))
    .withColumn("channel", F.lower(F.trim(F.col("channel"))))
    .withColumn("event_type", F.lower(F.trim(F.col("event_type"))))
    .withColumn(
        "is_future_dated",
        F.to_date(F.col("event_ts")) > F.to_date(F.lit(REFERENCE_DATE)),
    )
    .drop("_source_file", "_ingested_at")
)

write_table(df_touchpoints_silver, layer="silver", table_name="campaign_touchpoints")

n_future = df_touchpoints_silver.filter("is_future_dated").count()
log_step("silver_campaign_touchpoints",
          f"{df_touchpoints_silver.count()} touchpoints processados | {n_future} com "
          f"data futura em relação a {REFERENCE_DATE} (mantidos, flagados)")

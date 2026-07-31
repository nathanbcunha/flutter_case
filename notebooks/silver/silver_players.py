# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — players
# MAGIC
# MAGIC **Imperfeição tratada:** 22 jogadores (8.8%) com `acquisition_channel` nulo.
# MAGIC
# MAGIC **Decisão:** substituímos nulo por `'unknown'` explícito, em vez de descartar essas
# MAGIC linhas ou de tentar inferir um canal. Motivo de negócio: esses jogadores continuam
# MAGIC tendo depósitos/apostas e podem estar entre os dormentes de maior valor — descartá-los
# MAGIC quebraria o LTV por canal de aquisição (subseção 3b do case) silenciosamente. Um canal
# MAGIC `'unknown'` explícito é visível em qualquer agregação por canal, o que é preferível a
# MAGIC um dado ausente que passa despercebido.

# COMMAND ----------

# MAGIC %run ../00_config/00_config

# COMMAND ----------

# MAGIC %run ../01_setup/01_setup

# COMMAND ----------

df_players_bronze = read_table("bronze", "players")

df_players_silver = (
    df_players_bronze
    # Defensivo: profiling não achou duplicatas de player_id, mas dropDuplicates aqui
    # protege contra uma futura carga incremental que reintroduza o mesmo jogador.
    .dropDuplicates(["player_id"])
    .withColumn("signup_date", F.col("signup_date").cast("date"))
    .withColumn(
        "acquisition_channel",
        F.when(
            (F.col("acquisition_channel").isNull()) | (F.trim(F.col("acquisition_channel")) == ""),
            F.lit("unknown"),
        ).otherwise(F.trim(F.col("acquisition_channel"))),
    )
    .withColumn("self_excluded", F.col("self_excluded").cast("boolean"))
    .withColumn("country", F.upper(F.trim(F.col("country"))))
    .withColumn("preferred_currency", F.upper(F.trim(F.col("preferred_currency"))))
    .withColumn("kyc_status", F.lower(F.trim(F.col("kyc_status"))))
    # Flag de elegibilidade de negócio (não é limpeza de dado — é regra de compliance/risco
    # combinada no README): usada por silver/gold adiante para filtrar quem pode ser alvo
    # de campanha, sem apagar a linha do jogador (ele continua existindo para fins de
    # relatório histórico, só não é "elegível" para reativação).
    .withColumn(
        "is_eligible_for_targeting",
        (~F.coalesce(F.col("self_excluded"), F.lit(False)))
        & (F.col("kyc_status") != "rejected"),
    )
    .drop("_source_file", "_ingested_at")
)

write_table(df_players_silver, layer="silver", table_name="players")

n_unknown = df_players_silver.filter("acquisition_channel = 'unknown'").count()
n_ineligible = df_players_silver.filter("NOT is_eligible_for_targeting").count()
log_step("silver_players",
          f"{n_unknown} jogadores com canal 'unknown' | {n_ineligible} não-elegíveis "
          f"para targeting (autoexcluídos ou KYC rejeitado)")

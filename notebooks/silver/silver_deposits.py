# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — deposits
# MAGIC
# MAGIC **Imperfeição tratada: 25 `deposit_id` duplicados.** Confirmado no profiling que são
# MAGIC duplicatas **exatas** (todas as colunas idênticas) — consistente com reenvio/replay de
# MAGIC evento na origem, não com conflito de dado real. `dropDuplicates()` sobre a linha
# MAGIC inteira resolve com segurança (se fossem duplicatas com valores diferentes, a decisão
# MAGIC teria que ser outra — ex.: manter o registro mais recente — e isso estaria documentado
# MAGIC aqui como tal).
# MAGIC
# MAGIC **status do depósito:** mantemos as 3 categorias (`confirmed`/`pending`/`failed`) lado a
# MAGIC lado — decisão alinhada com o time: `confirmed` é o valor monetário "real" usado em
# MAGIC LTV, mas `pending`/`failed` ficam visíveis para quem quiser investigar fricção de
# MAGIC pagamento por canal/oferta.

# COMMAND ----------

# MAGIC %run ../00_config/00_config

# COMMAND ----------

# MAGIC %run ../01_setup/01_setup

# COMMAND ----------

df_deposits_bronze = read_table("bronze", "deposits")

n_before = df_deposits_bronze.count()

df_deposits_dedup = df_deposits_bronze.dropDuplicates(
    ["deposit_id", "player_id", "deposit_ts", "amount", "currency", "status"]
)

n_after = df_deposits_dedup.count()

df_deposits_typed = (
    df_deposits_dedup
    .withColumn("deposit_ts", F.col("deposit_ts").cast("timestamp"))
    .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
    .withColumn("currency", F.upper(F.trim(F.col("currency"))))
    .withColumn("status", F.lower(F.trim(F.col("status"))))
    .withColumn("is_currency_valid", F.col("currency").isin(EXPECTED_CURRENCIES))
    .withColumn("is_confirmed", F.col("status") == DEPOSIT_VALID_STATUS)
    .drop("_source_file", "_ingested_at")
)

df_deposits_silver = convert_to_brl(
    df_deposits_typed, amount_col="amount", currency_col="currency", ts_col="deposit_ts",
    output_col="amount_brl",
)

write_table(df_deposits_silver, layer="silver", table_name="deposits")

log_step("silver_deposits",
          f"{n_before} → {n_after} linhas após dedup exato ({n_before - n_after} "
          f"duplicatas removidas)")

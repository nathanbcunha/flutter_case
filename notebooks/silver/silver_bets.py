# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — bets
# MAGIC
# MAGIC **Grão preservado:** 1 linha por aposta. Nenhuma duplicata ou valor negativo encontrado
# MAGIC no profiling — a limpeza aqui é essencialmente tipagem + normalização de câmbio.
# MAGIC
# MAGIC **GGR (Gross Gaming Revenue) por aposta** = `stake − payout`. É a métrica padrão de
# MAGIC iGaming para "quanto a casa reteve daquela aposta" — negativo quando o jogador ganha
# MAGIC mais do que apostou. Calculado já em BRL para poder ser somado diretamente entre
# MAGIC jogadores de moedas diferentes na Gold, sem repetir a lógica de câmbio lá.

# COMMAND ----------

# MAGIC %run ../00_config/00_config

# COMMAND ----------

# MAGIC %run ../01_setup/01_setup

# COMMAND ----------

df_bets_bronze = read_table("bronze", "bets")

df_bets_typed = (
    df_bets_bronze
    .dropDuplicates(["bet_id"])  # defensivo — nenhuma encontrada no profiling
    .withColumn("bet_ts", F.col("bet_ts").cast("timestamp"))
    .withColumn("stake", F.col("stake").cast("decimal(18,2)"))
    .withColumn("payout", F.col("payout").cast("decimal(18,2)"))
    .withColumn("currency", F.upper(F.trim(F.col("currency"))))
    .withColumn("product", F.lower(F.trim(F.col("product"))))
    # Quarentena explícita: moeda fora do vocabulário esperado (não observada no
    # profiling, mas dado de produção real pode trazer isso a qualquer momento).
    .withColumn("is_currency_valid", F.col("currency").isin(EXPECTED_CURRENCIES))
    .drop("_source_file", "_ingested_at")
)

df_bets_converted = convert_to_brl(
    df_bets_typed, amount_col="stake", currency_col="currency", ts_col="bet_ts",
    output_col="stake_brl",
)
df_bets_converted = convert_to_brl(
    df_bets_converted, amount_col="payout", currency_col="currency", ts_col="bet_ts",
    output_col="payout_brl",
)

df_bets_silver = df_bets_converted.withColumn(
    "ggr_brl", F.round(F.col("stake_brl") - F.col("payout_brl"), 2)
)

write_table(df_bets_silver, layer="silver", table_name="bets")

n_invalid_currency = df_bets_silver.filter("NOT is_currency_valid").count()
log_step("silver_bets",
          f"{df_bets_silver.count()} apostas processadas | {n_invalid_currency} com moeda "
          f"fora do vocabulário esperado (não convertidas)")

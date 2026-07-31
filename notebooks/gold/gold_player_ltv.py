# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — player_ltv
# MAGIC
# MAGIC Tabela-base de toda a análise (3b) e recomendação (3c): 1 linha por jogador, com valor
# MAGIC financeiro consolidado em BRL, status de dormência e elegibilidade para campanha.
# MAGIC
# MAGIC ## Definição de dormência adotada (ver README para o racional completo)
# MAGIC
# MAGIC Um jogador é **dormente** se o tempo desde sua última atividade (o mais recente entre
# MAGIC depósito confirmado e aposta) excede um limiar de dias. O limiar é:
# MAGIC - **Pessoal**, quando o jogador tem depósitos confirmados suficientes
# MAGIC   (`DORMANCY_MIN_DEPOSITS_FOR_PERSONAL_CADENCE`) para calcular sua cadência histórica
# MAGIC   com alguma confiança — mediana do intervalo entre depósitos consecutivos, multiplicada
# MAGIC   por `DORMANCY_CADENCE_MULTIPLIER`, com piso/teto para evitar outliers de amostra
# MAGIC   pequena.
# MAGIC - **Padrão de mercado (60 dias)**, para os demais — a maioria da base, dado o volume
# MAGIC   amostral deste dataset (ver 00_config para o detalhe estatístico que motivou essa
# MAGIC   escolha).
# MAGIC
# MAGIC Jogadores sem NENHUM depósito confirmado e NENHUMA aposta no histórico são
# MAGIC `never_converted` — não são "dormentes" no sentido de reativação (nunca converteram),
# MAGIC então ficam num status de ciclo de vida separado e fora do público de reativação.

# COMMAND ----------

# MAGIC %run ../00_config/00_config

# COMMAND ----------

# MAGIC %run ../01_setup/01_setup

# COMMAND ----------

df_players = read_table("silver", "players")
df_bets = read_table("silver", "bets")
df_deposits = read_table("silver", "deposits")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregados financeiros por jogador (apostas)

# COMMAND ----------

df_bets_agg = df_bets.groupBy("player_id").agg(
    F.sum("stake_brl").alias("turnover_brl"),
    F.sum("ggr_brl").alias("ggr_brl"),
    F.max("bet_ts").alias("last_bet_ts"),
    F.count("*").alias("n_bets"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agregados financeiros por jogador (depósitos) — 3 status lado a lado

# COMMAND ----------

df_deposits_agg = df_deposits.groupBy("player_id").agg(
    F.sum(F.when(F.col("is_confirmed"), F.col("amount_brl"))).alias("net_deposits_confirmed_brl"),
    F.sum(F.when(F.col("status") == "pending", F.col("amount_brl"))).alias("deposits_pending_brl"),
    F.sum(F.when(F.col("status") == "failed", F.col("amount_brl"))).alias("deposits_failed_brl"),
    F.max(F.when(F.col("is_confirmed"), F.col("deposit_ts"))).alias("last_confirmed_deposit_ts"),
    F.count(F.when(F.col("is_confirmed"), True)).alias("n_confirmed_deposits"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cadência pessoal de depósito (para dormência adaptativa)
# MAGIC
# MAGIC Mediana do intervalo (em dias) entre depósitos confirmados consecutivos do mesmo
# MAGIC jogador. Só é calculada de fato — os passos seguintes decidem se ela é confiável o
# MAGIC suficiente para uso.

# COMMAND ----------

w_player_time = Window.partitionBy("player_id").orderBy("deposit_ts")

df_deposit_intervals = (
    df_deposits.filter("is_confirmed")
    .withColumn("prev_deposit_ts", F.lag("deposit_ts").over(w_player_time))
    .withColumn("interval_days", F.datediff(F.col("deposit_ts"), F.col("prev_deposit_ts")))
    .filter(F.col("interval_days").isNotNull())
)

df_personal_cadence = df_deposit_intervals.groupBy("player_id").agg(
    F.expr("percentile_approx(interval_days, 0.5)").alias("personal_cadence_days")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Montagem da tabela final

# COMMAND ----------

df_gold = (
    df_players
    .join(df_bets_agg, "player_id", "left")
    .join(df_deposits_agg, "player_id", "left")
    .join(df_personal_cadence, "player_id", "left")
    .fillna(0, subset=["turnover_brl", "ggr_brl", "n_bets", "net_deposits_confirmed_brl",
                        "deposits_pending_brl", "deposits_failed_brl", "n_confirmed_deposits"])
)

df_gold = (
    df_gold
    .withColumn("last_activity_ts", F.greatest("last_bet_ts", "last_confirmed_deposit_ts"))
    .withColumn("is_never_converted", F.col("last_activity_ts").isNull())
    .withColumn(
        "days_since_last_activity",
        F.when(F.col("last_activity_ts").isNotNull(),
               F.datediff(F.to_date(F.lit(REFERENCE_DATE)), F.col("last_activity_ts"))),
    )
    .withColumn(
        "has_reliable_personal_cadence",
        F.col("n_confirmed_deposits") >= DORMANCY_MIN_DEPOSITS_FOR_PERSONAL_CADENCE,
    )
    .withColumn(
        "dormancy_threshold_days",
        F.when(
            F.col("has_reliable_personal_cadence"),
            F.greatest(
                F.lit(DORMANCY_THRESHOLD_FLOOR_DAYS),
                F.least(
                    F.lit(DORMANCY_THRESHOLD_CAP_DAYS),
                    F.round(F.col("personal_cadence_days") * DORMANCY_CADENCE_MULTIPLIER),
                ),
            ),
        ).otherwise(F.lit(DORMANCY_FALLBACK_THRESHOLD_DAYS)),
    )
    .withColumn(
        "player_lifecycle_status",
        F.when(F.col("is_never_converted"), F.lit("never_converted"))
         .when(F.col("days_since_last_activity") > F.col("dormancy_threshold_days"), F.lit("dormant"))
         .otherwise(F.lit("active")),
    )
    # --- Métricas de valor combinado (GGR + depósitos + turnover), conforme premissa
    # alinhada: "combino os três para gerar eficiência de monetização / desgaste teórico
    # da carteira". Ver README para a discussão completa de cada métrica.
    .withColumn(
        "hold_rate",
        F.when(F.col("turnover_brl") > 0, F.round(F.col("ggr_brl") / F.col("turnover_brl"), 4)),
    )
    .withColumn(
        "wallet_burn_rate",
        F.when(F.col("net_deposits_confirmed_brl") > 0,
               F.round(F.col("ggr_brl") / F.col("net_deposits_confirmed_brl"), 4)),
    )
    # PROXY, não saldo real: não há tabela de saques neste dataset. Superestima o saldo de
    # qualquer jogador que já sacou dinheiro. Ver README, seção de limitações.
    .withColumn(
        "theoretical_balance_brl",
        F.round(F.col("net_deposits_confirmed_brl") - F.col("ggr_brl"), 2),
    )
)

write_table(df_gold, layer="gold", table_name="player_ltv")

# COMMAND ----------

summary = df_gold.groupBy("player_lifecycle_status").agg(
    F.count("*").alias("n_players"),
    F.round(F.sum("ggr_brl"), 2).alias("total_ggr_brl"),
)
if RUNNING_ON_DATABRICKS:
    display(summary)
else:
    summary.show(truncate=False)

n_reliable_cadence = df_gold.filter("has_reliable_personal_cadence").count()
log_step("gold_player_ltv",
          f"{n_reliable_cadence} jogadores com cadência pessoal confiável "
          f"(>= {DORMANCY_MIN_DEPOSITS_FOR_PERSONAL_CADENCE} depósitos); "
          f"os demais usam threshold padrão de {DORMANCY_FALLBACK_THRESHOLD_DAYS} dias")

# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — dormant_segments
# MAGIC
# MAGIC Responde diretamente à pergunta da liderança de Growth: **quais jogadores dormentes
# MAGIC vale a pena reativar?** Segmenta por valor, usando apenas jogadores dormentes E
# MAGIC elegíveis (não autoexcluídos, KYC não rejeitado).
# MAGIC
# MAGIC ## Achado de sanidade importante: GGR agregado é negativo neste dataset
# MAGIC
# MAGIC No `gold_player_ltv`, o GGR total do segmento dormente é **negativo** (jogadores, em
# MAGIC conjunto, ganharam mais do que apostaram). Isso foge do padrão real de operação de
# MAGIC apostas — a casa normalmente tem vantagem estatística positiva. Não é um bug do
# MAGIC pipeline: é uma característica esperada de dados fictícios gerados sem impor essa
# MAGIC vantagem estatisticamente. **Decisão:** por isso, para ranquear valor de segmentação,
# MAGIC usamos `net_deposits_confirmed_brl` (dinheiro que o jogador de fato colocou na operação)
# MAGIC como métrica primária — é mais estável e menos sujeita a essa distorção do dataset do
# MAGIC que GGR. GGR continua exposto na tabela para quem quiser essa lente, com o caveat.

# COMMAND ----------

# MAGIC %run ../00_config/00_config

# COMMAND ----------

# MAGIC %run ../01_setup/01_setup

# COMMAND ----------

df_ltv = read_table("gold", "player_ltv")

df_dormant_eligible = df_ltv.filter(
    (F.col("player_lifecycle_status") == "dormant") & (F.col("is_eligible_for_targeting"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Segmentação de valor — quartis sobre depósitos confirmados (BRL)
# MAGIC
# MAGIC Usamos quartis (não faixas fixas em R$) porque o dataset é pequeno (130 dormentes) e
# MAGIC quartis se adaptam à distribuição real observada, em vez de faixas arbitrárias que
# MAGIC poderiam deixar um segmento vazio ou desbalanceado. Em produção, com mais dados/tempo,
# MAGIC valeria testar faixas fixas alinhadas a metas de negócio (ex.: "VIP > R$5.000").

# COMMAND ----------

quantiles = df_dormant_eligible.approxQuantile("net_deposits_confirmed_brl", [0.25, 0.5, 0.75], 0.01)
q25, q50, q75 = quantiles
log_step("gold_dormant_segments", f"quartis de net_deposits_confirmed_brl: q25={q25} q50={q50} q75={q75}")

df_segments = df_dormant_eligible.withColumn(
    "value_segment",
    F.when(F.col("net_deposits_confirmed_brl") >= q75, F.lit("alto_valor"))
     .when(F.col("net_deposits_confirmed_brl") >= q50, F.lit("medio_valor"))
     .when(F.col("net_deposits_confirmed_brl") >= q25, F.lit("baixo_valor"))
     .otherwise(F.lit("valor_minimo")),
)

write_table(df_segments, layer="gold", table_name="dormant_segments")

# COMMAND ----------

summary = df_segments.groupBy("value_segment").agg(
    F.count("*").alias("n_jogadores"),
    F.round(F.sum("net_deposits_confirmed_brl"), 2).alias("total_depositado_brl"),
    F.round(F.avg("days_since_last_activity"), 1).alias("media_dias_dormente"),
).orderBy(F.desc("total_depositado_brl"))

if RUNNING_ON_DATABRICKS:
    display(summary)
else:
    summary.show(truncate=False)

n_excluded_self = df_ltv.filter(
    (F.col("player_lifecycle_status") == "dormant") & (~F.col("is_eligible_for_targeting"))
).count()
log_step("gold_dormant_segments",
          f"{n_excluded_self} jogadores dormentes excluídos do público de campanha "
          f"(autoexcluídos ou KYC rejeitado)")

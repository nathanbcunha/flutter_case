# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — channel_offer_product_ltv
# MAGIC
# MAGIC Responde ao item 3b.3: **valor (LTV) por canal de aquisição, com quebras por oferta e
# MAGIC produto, em BRL.**
# MAGIC
# MAGIC ## Premissa de atribuição de oferta — importante, decisão explícita
# MAGIC
# MAGIC `acquisition_channel` (como o jogador chegou: organic, paid_social, affiliate...) é um
# MAGIC atributo direto do jogador em `players.csv`. Já **oferta** (`bonus50`, `freebet`, etc.)
# MAGIC não é — ela vive em `campaigns`, e um jogador pode ter recebido touchpoints de várias
# MAGIC campanhas/ofertas diferentes ao longo do tempo.
# MAGIC
# MAGIC **Decisão adotada: atribuição de último toque (last-touch).** Para cada jogador,
# MAGIC atribuímos a oferta da campanha do seu touchpoint mais recente (anterior à data de
# MAGIC referência — nunca usamos os 2 touchpoints com data futura identificados na Prata, para
# MAGIC não vazar informação). É a mesma lógica de atribuição mais comum em Martech quando não
# MAGIC há um modelo multi-touch mais sofisticado implementado. Jogadores sem nenhum touchpoint
# MAGIC ficam em `no_campaign_exposure` — não inventamos uma oferta para quem nunca foi
# MAGIC impactado por nenhuma campanha.
# MAGIC
# MAGIC **Limitação explícita (documentada, não escondida):** last-touch simplifica demais para
# MAGIC decisão de orçamento de mídia em produção — um jogador pode ter sido influenciado por 3
# MAGIC campanhas antes de depositar, e o crédito 100% pro último toque superestima aquele
# MAGIC canal. Com mais tempo, um modelo de atribuição multi-touch (ex.: linear ou baseado em
# MAGIC posição) seria mais correto. Ver README, "o que faria diferente com mais tempo".
# MAGIC
# MAGIC ## Por que produto só quebra as métricas de apostas, não de depósito
# MAGIC
# MAGIC `deposits.csv` não tem uma coluna de produto na origem — só `bets.csv` tem
# MAGIC (`sports`/`casino`). Por isso a quebra por produto existe para turnover/GGR (que vêm de
# MAGIC apostas), mas não para depósito confirmado (que fica quebrado só por canal × oferta).
# MAGIC Inventar um produto para depósito seria dado fabricado — preferimos deixar a lacuna
# MAGIC visível.

# COMMAND ----------

# MAGIC %run ../00_config/00_config

# COMMAND ----------

# MAGIC %run ../01_setup/01_setup

# COMMAND ----------

df_players = read_table("silver", "players")
df_bets = read_table("silver", "bets")
df_deposits = read_table("silver", "deposits")
df_touchpoints = read_table("silver", "campaign_touchpoints")
df_campaigns = read_table("silver", "campaigns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Atribuição de oferta por último toque

# COMMAND ----------

w_last_touch = Window.partitionBy("player_id").orderBy(F.desc("event_ts"))

df_last_touch = (
    df_touchpoints
    .filter("NOT is_future_dated")  # nunca atribuir com base em evento que ainda não aconteceu
    .withColumn("rn", F.row_number().over(w_last_touch))
    .filter("rn = 1")
    .join(df_campaigns.select("campaign_id", "offer"), "campaign_id", "left")
    .select("player_id", F.coalesce(F.col("offer"), F.lit("oferta_nao_identificada")).alias("attributed_offer"))
)

df_players_attributed = (
    df_players
    .join(df_last_touch, "player_id", "left")
    .withColumn(
        "attributed_offer",
        F.coalesce(F.col("attributed_offer"), F.lit("no_campaign_exposure")),
    )
    .select("player_id", "acquisition_channel", "attributed_offer")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## LTV por canal × oferta × produto (a partir de apostas)

# COMMAND ----------

df_bets_ltv = (
    df_bets
    .join(df_players_attributed, "player_id", "inner")
    .groupBy("acquisition_channel", "attributed_offer", "product")
    .agg(
        F.round(F.sum("stake_brl"), 2).alias("turnover_brl"),
        F.round(F.sum("ggr_brl"), 2).alias("ggr_brl"),
        F.countDistinct("player_id").alias("n_jogadores"),
        F.count("*").alias("n_apostas"),
    )
)

write_table(df_bets_ltv, layer="gold", table_name="channel_offer_product_ltv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Valor depositado por canal × oferta (sem quebra de produto — ver nota acima)

# COMMAND ----------

df_deposits_ltv = (
    df_deposits
    .filter("is_confirmed")
    .join(df_players_attributed, "player_id", "inner")
    .groupBy("acquisition_channel", "attributed_offer")
    .agg(
        F.round(F.sum("amount_brl"), 2).alias("net_deposits_confirmed_brl"),
        F.countDistinct("player_id").alias("n_jogadores"),
    )
)

write_table(df_deposits_ltv, layer="gold", table_name="channel_offer_deposits")

# COMMAND ----------

if RUNNING_ON_DATABRICKS:
    display(df_bets_ltv.orderBy(F.desc("turnover_brl")))
else:
    df_bets_ltv.orderBy(F.desc("turnover_brl")).show(20, truncate=False)

log_step("gold_channel_offer_product_ltv",
          f"{df_bets_ltv.count()} combinações canal×oferta×produto | "
          f"{df_deposits_ltv.count()} combinações canal×oferta (depósitos)")

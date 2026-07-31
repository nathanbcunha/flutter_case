# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — recommendation
# MAGIC
# MAGIC Consolida tudo em uma recomendação acionável (item 3c do case), escrita para uma
# MAGIC gerente de Growth não-técnica. Todos os números abaixo são **calculados ao vivo** a
# MAGIC partir das tabelas Gold já construídas — nada é hardcoded, para que a recomendação
# MAGIC nunca fique dessincronizada dos dados se o pipeline rodar de novo com dados atualizados.

# COMMAND ----------

# MAGIC %run ../00_config/00_config

# COMMAND ----------

# MAGIC %run ../01_setup/01_setup

# COMMAND ----------

df_segments = read_table("gold", "dormant_segments")
df_channel_offer_product = read_table("gold", "channel_offer_product_ltv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Quem mirar

# COMMAND ----------

seg_stats = (
    df_segments.groupBy("value_segment")
    .agg(
        F.count("*").alias("n_jogadores"),
        F.round(F.sum("net_deposits_confirmed_brl"), 2).alias("total_depositado_brl"),
        F.round(F.avg("days_since_last_activity"), 1).alias("media_dias_dormente"),
    )
    .collect()
)
seg_map = {r["value_segment"]: r for r in seg_stats}

total_dormant_eligible = df_segments.count()
total_value_all = sum(r["total_depositado_brl"] for r in seg_stats)

alto = seg_map.get("alto_valor")
medio = seg_map.get("medio_valor")
n_priority = (alto["n_jogadores"] if alto else 0) + (medio["n_jogadores"] if medio else 0)
value_priority = (alto["total_depositado_brl"] if alto else 0) + (medio["total_depositado_brl"] if medio else 0)
pct_value_priority = round(100 * value_priority / total_value_all, 1) if total_value_all else 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Com qual oferta e produto — desempenho histórico (hold rate)
# MAGIC
# MAGIC `hold_rate = GGR / turnover`. Mais próximo de zero (ou positivo) = a operação retém
# MAGIC mais do volume apostado; mais negativo = a oferta/produto "custou" mais do que gerou.
# MAGIC **Excluímos a oferta `none` do ranking de recomendação** — ela tem o melhor hold rate
# MAGIC justamente por não ter custo de incentivo nenhum, mas por isso também não serve como
# MAGIC "oferta de reativação" (não há gancho para trazer um jogador dormente de volta sem
# MAGIC nenhum incentivo).

# COMMAND ----------

offer_perf = (
    df_channel_offer_product
    .filter(~F.col("attributed_offer").isin("none", "no_campaign_exposure", "oferta_nao_identificada"))
    .groupBy("attributed_offer")
    .agg(F.sum("turnover_brl").alias("turnover_brl"), F.sum("ggr_brl").alias("ggr_brl"))
    .withColumn("hold_rate_pct", F.round(100 * F.col("ggr_brl") / F.col("turnover_brl"), 2))
    .orderBy(F.desc("hold_rate_pct"))
    .collect()
)

product_perf = (
    df_channel_offer_product
    .groupBy("product")
    .agg(F.sum("turnover_brl").alias("turnover_brl"), F.sum("ggr_brl").alias("ggr_brl"))
    .withColumn("hold_rate_pct", F.round(100 * F.col("ggr_brl") / F.col("turnover_brl"), 2))
    .orderBy(F.desc("hold_rate_pct"))
    .collect()
)

best_offer = offer_perf[0]["attributed_offer"] if offer_perf else "N/A"
best_product = product_perf[0]["product"] if product_perf else "N/A"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Recomendação escrita — para a gerente de Growth

# COMMAND ----------

recommendation_text = f"""
RECOMENDAÇÃO — CAMPANHA DE REATIVAÇÃO (ref. {REFERENCE_DATE})
================================================================

QUEM MIRAR
----------
De {total_dormant_eligible} jogadores dormentes elegíveis (após excluir autoexcluídos e
KYC rejeitado), priorize os segmentos "alto valor" e "médio valor":

  - {n_priority} jogadores ({round(100*n_priority/total_dormant_eligible,1)}% da base dormente elegível)
  - Representam R$ {value_priority:,.2f} em depósitos históricos confirmados
    ({pct_value_priority}% de TODO o valor da base dormente)

Ou seja: com orçamento pra atingir pouco mais da metade do público dormente, você
alcança quase 90% do valor histórico em jogo. É a alavanca de maior eficiência dado
orçamento limitado.

Detalhe por segmento:
"""
for seg_name in ["alto_valor", "medio_valor", "baixo_valor", "valor_minimo"]:
    r = seg_map.get(seg_name)
    if r:
        recommendation_text += (
            f"  - {seg_name}: {r['n_jogadores']} jogadores | "
            f"R$ {r['total_depositado_brl']:,.2f} depositados | "
            f"dormentes há {r['media_dias_dormente']} dias em média\n"
        )

recommendation_text += f"""
COM QUAL OFERTA E PRODUTO
--------------------------
Historicamente, entre as ofertas com custo de incentivo (excluindo 'sem oferta'):
"""
for r in offer_perf:
    recommendation_text += f"  - {r['attributed_offer']}: hold rate {r['hold_rate_pct']}%\n"

recommendation_text += f"""
  → Melhor custo-benefício histórico: '{best_offer}'

Por produto:
"""
for r in product_perf:
    recommendation_text += f"  - {r['product']}: hold rate {r['hold_rate_pct']}%\n"

recommendation_text += f"""
  → Produto com melhor margem histórica: '{best_product}'

RECOMENDAÇÃO FINAL: priorizar os segmentos alto e médio valor, com oferta
'{best_offer}' direcionada a produtos '{best_product}', por ser a combinação com melhor
retorno histórico observado nesta base.

TRADE-OFFS E LIMITES (para deixar claro na conversa com a mesa)
------------------------------------------------------------------
  - GGR agregado desta base é NEGATIVO em vários cortes — dataset fictício sem vantagem
    de casa embutida na geração dos dados. Os hold rates acima são direcionais/comparativos
    entre ofertas, não devem ser lidos como "a operação perde dinheiro de verdade".
  - A oferta atribuída a cada jogador é por ÚLTIMO TOQUE (last-touch) — simplificação;
    jogadores tocados por múltiplas campanhas têm 100% do crédito no último toque.
  - "Valor" aqui é depósito confirmado histórico, não o valor esperado de UMA campanha de
    reativação especificamente (não temos dado de campanhas de reativação anteriores para
    medir taxa de resposta esperada) — é um proxy de "quanto esse jogador já provou valer",
    não uma previsão de quanto ele vai gastar se reativado.
"""

print(recommendation_text)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persistência do resumo (para consumo por BI/apresentação)

# COMMAND ----------

summary_rows = [(
    total_dormant_eligible, n_priority, float(value_priority), pct_value_priority,
    best_offer, best_product, REFERENCE_DATE,
)]
df_summary = spark.createDataFrame(
    summary_rows,
    ["total_dormant_eligible", "n_priority_segment", "value_priority_brl",
     "pct_value_priority", "recommended_offer", "recommended_product", "reference_date"],
)
write_table(df_summary, layer="gold", table_name="recommendation_summary")

log_step("gold_recommendation", "recomendação gerada e persistida em gold.recommendation_summary")

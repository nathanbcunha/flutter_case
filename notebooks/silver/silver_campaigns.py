# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — campaigns
# MAGIC
# MAGIC Este é o notebook mais denso em regra de negócio do case: parseia `campaign_name` em
# MAGIC colunas estruturadas conforme o padrão oficial `{geo}_{channel}_{objective}_{product}_
# MAGIC {audience}_{period}_{offer}` e produz o **relatório de conformidade da taxonomia**
# MAGIC exigido no guia (item 4a-ii).
# MAGIC
# MAGIC ## Estratégia do parser — duas camadas de rigor
# MAGIC
# MAGIC 1. **Checagem estrita**: o nome bate 100% com o padrão oficial, literalmente — 7
# MAGIC    segmentos, ordem certa, separador `_`, capitalização certa. `taxonomy_compliant=True`.
# MAGIC    Essa é a métrica que respondemos para a liderança quando perguntarem "quantas
# MAGIC    campanhas seguem nosso padrão hoje".
# MAGIC 2. **Recuperação por correspondência de vocabulário (bag-of-tokens)**: se a checagem
# MAGIC    estrita falha, normalizamos (separador, case, erros de digitação conhecidos) e
# MAGIC    procuramos, para cada segmento da taxonomia, um token que bate com o vocabulário
# MAGIC    daquele segmento — **independente da posição**. Isso recupera conteúdo válido de
# MAGIC    nomes com ordem trocada, ruído extra (`_v2_FINAL`), ou separador errado, sem exigir
# MAGIC    que a string inteira bata perfeitamente. `taxonomy_recovered=True`.
# MAGIC
# MAGIC Nomes vazios ou sem nenhum token reconhecível (ex.: texto livre em português, sem
# MAGIC relação com o vocabulário em inglês) ficam com todos os campos nulos e uma nota
# MAGIC explícita pedindo categorização manual — **não inventamos valor nenhum por inferência
# MAGIC estatística**; é melhor um `NULL` auditável do que um palpite errado silencioso.
# MAGIC
# MAGIC ## Decisão de "fonte de verdade": campaign_name vs. campaign_touchpoints.channel
# MAGIC
# MAGIC O profiling encontrou um conflito real: a campanha **C003** tem `channel=email` no
# MAGIC nome, mas 100% dos touchpoints reais dela (84 eventos) foram enviados por `push`.
# MAGIC
# MAGIC **Decisão adotada:** `campaign_touchpoints.channel` é a fonte de verdade para o canal
# MAGIC *efetivamente usado*, porque é um registro operacional gerado pelo sistema de envio no
# MAGIC momento do disparo — não pode estar "errado" no sentido de digitação humana, é o que
# MAGIC de fato aconteceu. `campaign_name` é um rótulo digitado por uma pessoa no momento da
# MAGIC criação da campanha e pode divergir da execução real (cópia de outra campanha,
# MAGIC mudança de canal decidida depois de nomear, erro de digitação).
# MAGIC
# MAGIC Por isso calculamos `effective_channel` = canal dominante observado em
# MAGIC `campaign_touchpoints` para aquela campanha, e mantemos `parsed_channel` (vindo do
# MAGIC nome) como campo separado — nunca sobrescrevemos silenciosamente, sinalizamos o
# MAGIC conflito em `channel_source_conflict` para que fique visível e auditável.

# COMMAND ----------

# MAGIC %run ../00_config/00_config

# COMMAND ----------

# MAGIC %run ../01_setup/01_setup

# COMMAND ----------

import re
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parser de taxonomia
# MAGIC
# MAGIC Implementado como função Python pura (não SQL/regex-only) porque a lógica de
# MAGIC recuperação por bag-of-tokens precisa de controle procedural (busca por segmento,
# MAGIC marcação de tokens já usados) que fica ilegível em uma única expressão regex. A tabela
# MAGIC tem 12 linhas — não há motivo de performance para forçar isso em SQL nativo do Spark.

# COMMAND ----------

SEGMENTS_ORDER = ["geo", "channel", "objective", "product", "audience", "period", "offer"]
PERIOD_PATTERN = re.compile(r"^\d{4}q[1-4]$")


def parse_campaign_taxonomy(raw_name):
    """Retorna um dict com os 7 segmentos da taxonomia + flags de conformidade.
    Ver célula de markdown acima para a estratégia completa."""
    if raw_name is None or raw_name.strip() == "":
        return dict(**{s: None for s in SEGMENTS_ORDER}, taxonomy_compliant=False,
                    taxonomy_recovered=False,
                    parse_notes="nome de campanha vazio - requer categorização manual")

    # --- Passo 1: checagem ESTRITA sobre o texto original, sem normalizar nada ---
    raw_tokens = raw_name.split("_")
    strict_ok = len(raw_tokens) == 7
    strict_result = {}
    if strict_ok:
        for seg, tok in zip(SEGMENTS_ORDER, raw_tokens):
            if seg == "period":
                if PERIOD_PATTERN.match(tok.lower()) and len(tok) == 6 and tok[4] == "Q":
                    strict_result[seg] = tok
                else:
                    strict_ok = False
                    break
            elif seg == "geo":
                if tok in CAMPAIGN_TAXONOMY_VOCAB["geo"]:
                    strict_result[seg] = tok
                else:
                    strict_ok = False
                    break
            else:
                if tok in CAMPAIGN_TAXONOMY_VOCAB[seg]:
                    strict_result[seg] = tok
                else:
                    strict_ok = False
                    break
    if strict_ok:
        return dict(**strict_result, taxonomy_compliant=True, taxonomy_recovered=False,
                    parse_notes="nome 100% conforme ao padrão oficial")

    # --- Passo 2: normalização + recuperação por correspondência de vocabulário ---
    normalized = re.sub(r"[-\s]+", "_", raw_name.strip())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    tokens = [t.lower() for t in normalized.split("_") if t != ""]
    tokens = [CAMPAIGN_TYPO_CORRECTIONS.get("objective", {}).get(t)
              or CAMPAIGN_TYPO_CORRECTIONS.get("audience", {}).get(t)
              or t for t in tokens]

    vocab_lower = {seg: [v.lower() for v in vals] for seg, vals in CAMPAIGN_TAXONOMY_VOCAB.items()}

    recovered = {s: None for s in SEGMENTS_ORDER}
    used_idx = set()
    for seg in SEGMENTS_ORDER:
        if seg == "period":
            for i, tok in enumerate(tokens):
                if i in used_idx:
                    continue
                if PERIOD_PATTERN.match(tok):
                    recovered[seg] = tok.upper()
                    used_idx.add(i)
                    break
        else:
            candidates = [i for i, tok in enumerate(tokens)
                          if i not in used_idx and tok in vocab_lower[seg]]
            if candidates:
                i = candidates[0]
                recovered[seg] = tokens[i].upper() if seg == "geo" else tokens[i]
                used_idx.add(i)

    unmatched = [tokens[i] for i in range(len(tokens)) if i not in used_idx]
    n_recovered = sum(1 for v in recovered.values() if v is not None)

    if n_recovered == 0:
        return dict(**{s: None for s in SEGMENTS_ORDER}, taxonomy_compliant=False,
                    taxonomy_recovered=False,
                    parse_notes="nenhum segmento reconhecível (não segue o padrão nem "
                                "parcialmente) - requer categorização manual")

    missing = [s for s in SEGMENTS_ORDER if recovered[s] is None]
    notes = [f"{n_recovered}/7 segmentos recuperados via correspondência de vocabulário "
             f"(formato/ordem fora do padrão)"]
    if missing:
        notes.append(f"segmentos não encontrados: {missing}")
    if unmatched:
        notes.append(f"tokens ignorados (não reconhecidos): {unmatched}")

    return dict(**recovered, taxonomy_compliant=False, taxonomy_recovered=True,
                parse_notes="; ".join(notes))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aplicação via UDF

# COMMAND ----------

parse_result_schema = StructType([
    StructField("geo", StringType()),
    StructField("channel", StringType()),
    StructField("objective", StringType()),
    StructField("product", StringType()),
    StructField("audience", StringType()),
    StructField("period", StringType()),
    StructField("offer", StringType()),
    StructField("taxonomy_compliant", BooleanType()),
    StructField("taxonomy_recovered", BooleanType()),
    StructField("parse_notes", StringType()),
])

parse_udf = F.udf(lambda name: parse_campaign_taxonomy(name), parse_result_schema)

df_campaigns_bronze = read_table("bronze", "campaigns")

df_parsed = (
    df_campaigns_bronze
    .withColumn("_parsed", parse_udf(F.col("campaign_name")))
    .select(
        "campaign_id",
        "campaign_name",
        F.col("created_date").cast("date").alias("created_date"),
        "status",
        F.col("_parsed.geo").alias("parsed_geo"),
        F.col("_parsed.channel").alias("parsed_channel"),
        F.col("_parsed.objective").alias("objective"),
        F.col("_parsed.product").alias("product"),
        F.col("_parsed.audience").alias("audience"),
        F.col("_parsed.period").alias("period"),
        F.col("_parsed.offer").alias("offer"),
        F.col("_parsed.taxonomy_compliant").alias("taxonomy_compliant"),
        F.col("_parsed.taxonomy_recovered").alias("taxonomy_recovered"),
        F.col("_parsed.parse_notes").alias("parse_notes"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Canal efetivo — resolvendo o conflito campaign_name vs. touchpoints
# MAGIC
# MAGIC Calculamos o canal dominante por campanha a partir dos touchpoints reais e comparamos
# MAGIC com o canal extraído do nome. Ver racional completo na célula de markdown do topo.

# COMMAND ----------

df_touchpoints_bronze = read_table("bronze", "campaign_touchpoints")

df_dominant_channel = (
    df_touchpoints_bronze
    .groupBy("campaign_id", "channel")
    .agg(F.count("*").alias("n_events"))
    .withColumn("rn", F.row_number().over(
        Window.partitionBy("campaign_id").orderBy(F.desc("n_events"))
    ))
    .filter("rn = 1")
    .select("campaign_id", F.col("channel").alias("effective_channel"))
)

df_campaigns_silver = (
    df_parsed
    .join(df_dominant_channel, "campaign_id", "left")
    # Se a campanha não tem nenhum touchpoint ainda (caso não observado nos 12 registros
    # atuais, mas plausível em produção para campanha recém-criada), caímos no canal
    # parseado do nome como melhor estimativa disponível.
    .withColumn("effective_channel", F.coalesce(F.col("effective_channel"), F.col("parsed_channel")))
    .withColumn(
        "channel_source_conflict",
        (F.col("parsed_channel").isNotNull())
        & (F.col("effective_channel").isNotNull())
        & (F.col("parsed_channel") != F.col("effective_channel")),
    )
)

write_table(df_campaigns_silver, layer="silver", table_name="campaigns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Relatório de conformidade da taxonomia
# MAGIC
# MAGIC Saída dedicada exigida pelo guia do case (item 4a-ii) — visão executiva de quantas
# MAGIC campanhas seguem o padrão oficial hoje, quantas foram recuperáveis e quantas exigem
# MAGIC intervenção manual.

# COMMAND ----------

df_compliance_report = (
    df_campaigns_silver
    .withColumn(
        "compliance_category",
        F.when(F.col("taxonomy_compliant"), F.lit("conforme"))
         .when(F.col("taxonomy_recovered"), F.lit("recuperado_automaticamente"))
         .otherwise(F.lit("requer_revisao_manual")),
    )
    .select(
        "campaign_id", "campaign_name", "compliance_category",
        "channel_source_conflict", "parse_notes",
    )
)

write_table(df_compliance_report, layer="silver", table_name="campaign_taxonomy_compliance_report")

summary = (
    df_compliance_report.groupBy("compliance_category").count().orderBy(F.desc("count"))
)
if RUNNING_ON_DATABRICKS:
    display(summary)
else:
    summary.show(truncate=False)

n_conflicts = df_campaigns_silver.filter("channel_source_conflict").count()
log_step("silver_campaigns",
          f"{n_conflicts} campanha(s) com conflito entre canal do nome e canal real de envio")

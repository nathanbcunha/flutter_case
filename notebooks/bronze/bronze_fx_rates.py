# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Cotações de câmbio (Frankfurter API)
# MAGIC
# MAGIC **Grão da tabela:** 1 linha por (data, moeda de origem) → taxa de conversão para BRL.
# MAGIC
# MAGIC **Decisão de design — 1 chamada de time series, não N chamadas por data:**
# MAGIC A API oferece endpoint de intervalo (`/{start}..{end}`), então buscamos o range inteiro
# MAGIC coberto pelas transações em uma única chamada por par de moeda, em vez de uma chamada
# MAGIC por data/transação. Isso é o que "ingestão robusta pensada para produção" significa na
# MAGIC prática: menos chamadas de rede, menor chance de rate limiting, execução determinística.
# MAGIC
# MAGIC **Decisão de design — base EUR, derivando USD→BRL:**
# MAGIC A Frankfurter usa EUR como moeda-base por padrão (é a fonte primária — Banco Central
# MAGIC Europeu). Buscamos EUR→BRL e EUR→USD na mesma chamada e derivamos USD→BRL localmente
# MAGIC (EUR→BRL / EUR→USD), em vez de fazer uma segunda chamada com base=USD. Reduz round-trips
# MAGIC e mantém uma única fonte de verdade cambial (tudo ancorado em EUR/ECB).
# MAGIC
# MAGIC **Decisão de design — fins de semana/feriados (forward-fill):**
# MAGIC O ECB não publica cotação em fins de semana/feriados bancários, então a API simplesmente
# MAGIC não retorna essas datas na série. Como nossas apostas/depósitos acontecem todo santo dia
# MAGIC (inclusive fins de semana — apostador não para no sábado), precisamos de uma cotação para
# MAGIC TODA data de transação. Premissa adotada: usar a **última cotação disponível anterior**
# MAGIC (forward-fill) — é a prática padrão de mercado para converter transações de fim de semana
# MAGIC (o câmbio de sexta-feira "vale" para o fim de semana até a próxima atualização).

# COMMAND ----------

# MAGIC %run ../00_config/00_config

# COMMAND ----------

# MAGIC %run ../01_setup/01_setup

# COMMAND ----------

import time
import urllib.request
import json
# Nota: usamos `import datetime` (módulo) em vez de `from datetime import datetime` de
# propósito. Como os notebooks compartilham o mesmo namespace via %run (igual ao que já
# acontece aqui), um `from datetime import datetime` sobrescreveria o nome `datetime` usado
# por `log_step` em 01_setup.py — um bug sutil de shadowing que só aparece porque os
# notebooks realmente compartilham escopo em produção.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Range de datas a cobrir
# MAGIC
# MAGIC Cobrimos desde a data mínima observada nos dados brutos (com folga de alguns dias para
# MAGIC trás, para garantir forward-fill correto mesmo se a primeira transação cair num
# MAGIC fim de semana) até a data de referência do negócio.

# COMMAND ----------

# Em produção, esse range viria de MIN/MAX real das tabelas de origem (bets/deposits) já
# ingeridas. Para o bronze de FX rodar de forma independente (sem depender de outro bronze
# já ter rodado), fixamos um range levemente mais largo que o observado no profiling
# (~2023-08-01 a 2024-04-01), com folga de 7 dias no início para garantir forward-fill.
FX_START_DATE = "2023-07-25"
FX_END_DATE = REFERENCE_DATE  # 2024-04-01

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chamada HTTP com retry
# MAGIC
# MAGIC "Robusto" para produção significa não quebrar o pipeline inteiro por uma falha
# MAGIC transiente de rede. Retry com backoff exponencial simples; se todas as tentativas
# MAGIC falharem, o notebook falha explicitamente (melhor falhar alto e visível do que
# MAGIC seguir adiante com câmbio ausente/errado silenciosamente).

# COMMAND ----------

def fetch_frankfurter_timeseries(start_date: str, end_date: str, symbols: str,
                                  max_retries: int = 3, backoff_seconds: float = 2.0) -> dict:
    """Busca série histórica de câmbio (base EUR) da Frankfurter API v1.
    Sempre monta a URL a partir de FRANKFURTER_BASE_URL (config centralizada) — nunca por
    concatenação livre, para não correr risco de apontar pra versão errada da API."""
    url = f"{FRANKFURTER_BASE_URL}/{start_date}..{end_date}?symbols={symbols}"

    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            with urllib.request.urlopen(url, timeout=15) as resp:
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}")
                return json.loads(resp.read().decode("utf-8"))
        except Exception as e:
            last_error = e
            log_step("bronze_fx_rates",
                      f"tentativa {attempt}/{max_retries} falhou para {url}: {e}")
            if attempt < max_retries:
                time.sleep(backoff_seconds * attempt)
    raise RuntimeError(f"Falha ao buscar câmbio após {max_retries} tentativas: {last_error}")


# MAGIC %md
# MAGIC ### Fallback: degradação controlada quando a API está inacessível
# MAGIC
# MAGIC Alguns ambientes (ex.: Databricks Free Edition, que restringe egress de rede a uma
# MAGIC lista fechada de domínios confiáveis) bloqueiam chamadas a APIs externas arbitrárias.
# MAGIC Em vez de deixar o pipeline inteiro travar por causa disso, caímos para um seed de
# MAGIC câmbio versionado junto com o repositório (`data/fx_rates_fallback_seed.json`) —
# MAGIC **claramente sinalizado como fallback**, nunca silencioso. Em produção real (rede
# MAGIC liberada, workspace pago), esse fallback nunca é acionado; ele existe para o pipeline
# MAGIC continuar demonstrável mesmo em um ambiente com rede restrita, e para ilustrar um
# MAGIC padrão de resiliência real diante de uma dependência externa instável.

# COMMAND ----------

def load_fx_fallback_seed():
    fallback_path = f"{REPO_ROOT}/data/fx_rates_fallback_seed.json"
    with open(fallback_path) as f:
        return json.load(f)


try:
    raw_fx_response = fetch_frankfurter_timeseries(FX_START_DATE, FX_END_DATE, symbols="BRL,USD")
    used_fallback = False
    log_step("bronze_fx_rates",
              f"{len(raw_fx_response.get('rates', {}))} datas retornadas pela API "
              f"(base={raw_fx_response.get('base')})")
except Exception as e:
    log_step("bronze_fx_rates",
              f"AVISO: API de câmbio inacessível ({e}). Usando fallback local "
              f"(data/fx_rates_fallback_seed.json) — cotações NÃO são as mais recentes; "
              f"isso é degradação controlada, não um erro silencioso.")
    raw_fx_response = load_fx_fallback_seed()
    used_fallback = True

# COMMAND ----------

# MAGIC %md
# MAGIC ## Persistência do payload bruto (auditoria)
# MAGIC
# MAGIC Guardamos a resposta da API tal como veio, com timestamp de captura — é a essência da
# MAGIC camada Bronze: se amanhã suspeitarmos de uma cotação estranha, conseguimos auditar
# MAGIC exatamente o que a fonte externa retornou naquele momento, sem depender da memória da
# MAGIC API (que só serve "latest" e histórico, não o que ela respondeu no passado).

# COMMAND ----------

raw_rows = []
for date_str, rates in raw_fx_response.get("rates", {}).items():
    row = {"rate_date": date_str, "base_currency": raw_fx_response.get("base", "EUR"),
           "used_fallback_source": used_fallback}
    row.update(rates)
    raw_rows.append(row)

df_fx_raw = spark.createDataFrame(raw_rows)
write_table(df_fx_raw, layer="bronze", table_name="fx_rates_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Forward-fill para cobrir todas as datas do calendário (incl. fins de semana)
# MAGIC
# MAGIC Geramos um calendário denso dia-a-dia e propagamos a última cotação conhecida para
# MAGIC frente. Isso ainda é considerado "bronze densificado" (não é limpeza de regra de
# MAGIC negócio, é completude técnica de uma fonte externa) — mantemos como uma tabela bronze
# MAGIC separada e explícita, para deixar claro que a interpolação aconteceu aqui, não
# MAGIC escondida dentro da Prata.

# COMMAND ----------

from pyspark.sql.types import StringType

all_dates = []
d = datetime.datetime.strptime(FX_START_DATE, "%Y-%m-%d")
end = datetime.datetime.strptime(FX_END_DATE, "%Y-%m-%d")
while d <= end:
    all_dates.append(d.strftime("%Y-%m-%d"))
    d += datetime.timedelta(days=1)

df_calendar = spark.createDataFrame([(x,) for x in all_dates], ["calendar_date"])

df_fx_dense = (
    df_calendar.join(df_fx_raw, df_calendar.calendar_date == df_fx_raw.rate_date, "left")
    .withColumn(
        "BRL_filled",
        F.last("BRL", ignorenulls=True).over(
            Window.orderBy("calendar_date").rowsBetween(Window.unboundedPreceding, 0)
        ),
    )
    .withColumn(
        "USD_filled",
        F.last("USD", ignorenulls=True).over(
            Window.orderBy("calendar_date").rowsBetween(Window.unboundedPreceding, 0)
        ),
    )
    .withColumn("was_forward_filled", F.col("rate_date").isNull())
    .select(
        F.col("calendar_date").alias("rate_date"),
        F.col("BRL_filled").alias("eur_to_brl"),
        F.col("USD_filled").alias("eur_to_usd"),
        "was_forward_filled",
    )
)

write_table(df_fx_dense, layer="bronze", table_name="fx_rates_dense_calendar")

display(df_fx_dense.orderBy("rate_date").limit(10)) if RUNNING_ON_DATABRICKS else \
    df_fx_dense.orderBy("rate_date").show(10, truncate=False)

log_step("bronze_fx_rates",
          f"{df_fx_dense.filter('was_forward_filled').count()} de {df_fx_dense.count()} "
          f"datas foram preenchidas por forward-fill (fins de semana/feriados)")

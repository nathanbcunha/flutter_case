# Databricks notebook source
# MAGIC %md
# MAGIC # 02_execution — Orquestração do pipeline completo
# MAGIC
# MAGIC Executa Bronze → Prata → Ouro em ordem, respeitando dependências entre tabelas
# MAGIC (ex.: `silver_bets` precisa de `bronze_fx_rates` já ter rodado; `gold_player_ltv`
# MAGIC precisa de todas as tabelas Prata).
# MAGIC
# MAGIC **Como isso viraria produção de verdade:** este notebook é feito para ser o notebook
# MAGIC apontado por um **Databricks Job/Workflow**, agendado (ex.: diariamente, após a janela
# MAGIC de carga da origem). Cada `dbutils.notebook.run(...)` abaixo viraria uma **task** no
# MAGIC grafo do Workflow, com dependências explícitas entre tasks (o Workflow já paraleliza
# MAGIC automaticamente o que não depende entre si — ex.: as 5 tabelas Bronze de origem podem
# MAGIC rodar em paralelo, só a de FX é sequencial-livre também). Ver `docs/architecture.md`
# MAGIC para o desenho completo de produção (agendamento, alertas, monitoramento de qualidade).
# MAGIC
# MAGIC Rodar este notebook interativamente (como está aqui) é o equivalente a uma execução
# MAGIC manual/ad-hoc — útil para desenvolvimento e para a apresentação ao vivo do case.

# COMMAND ----------

# MAGIC %run ../00_config/00_config

# COMMAND ----------

# MAGIC %run ../01_setup/01_setup

# COMMAND ----------

import time

NOTEBOOK_TIMEOUT_SECONDS = 600

# Ordem reflete as dependências reais entre camadas — não é arbitrária.
BRONZE_NOTEBOOKS = [
    "../bronze/bronze_fx_rates",
    "../bronze/bronze_players",
    "../bronze/bronze_bets",
    "../bronze/bronze_deposits",
    "../bronze/bronze_campaigns",
    "../bronze/bronze_campaign_touchpoints",
]

SILVER_NOTEBOOKS = [
    "../silver/silver_players",
    "../silver/silver_bets",              # depende de bronze_fx_rates
    "../silver/silver_deposits",          # depende de bronze_fx_rates
    "../silver/silver_campaign_touchpoints",
    "../silver/silver_campaigns",         # depende de silver_campaign_touchpoints (canal efetivo)
]

GOLD_NOTEBOOKS = [
    "../gold/gold_player_ltv",                    # depende de silver_players/bets/deposits
    "../gold/gold_dormant_segments",              # depende de gold_player_ltv
    "../gold/gold_channel_offer_product_ltv",      # depende de silver_* + campaigns
    "../gold/gold_recommendation",                # depende de todas as tabelas gold acima
]

# COMMAND ----------

def run_layer(layer_name, notebook_paths):
    log_step("02_execution", f"=== iniciando camada {layer_name} ===")
    for nb in notebook_paths:
        start = time.time()
        if RUNNING_ON_DATABRICKS:
            dbutils.notebook.run(nb, NOTEBOOK_TIMEOUT_SECONDS)
        else:
            # Execução local: %run não é chamável programaticamente fora do Databricks,
            # então o harness de teste (tests/_run_local.py) assume esse papel. Este ramo
            # existe só para deixar claro, para quem ler o código, o que aconteceria aqui
            # em Databricks de verdade.
            log_step("02_execution",
                      f"[modo local] pular execução programática de {nb} — use "
                      f"tests/_run_local.py para validação local completa")
        elapsed = round(time.time() - start, 1)
        log_step("02_execution", f"{nb} concluído em {elapsed}s")
    log_step("02_execution", f"=== camada {layer_name} concluída ===")


run_layer("bronze", BRONZE_NOTEBOOKS)
run_layer("silver", SILVER_NOTEBOOKS)
run_layer("gold", GOLD_NOTEBOOKS)

log_step("02_execution", "pipeline completo executado com sucesso")

# Databricks notebook source
# MAGIC %md
# MAGIC # 00_config — Configuração centralizada do pipeline
# MAGIC
# MAGIC **Por que este notebook existe isolado:** paths, nomes de catálogo/schema e premissas de
# MAGIC negócio (ex.: janela de dormência) são usados em TODAS as camadas (bronze/silver/gold).
# MAGIC Centralizar aqui evita que uma constante de negócio fique divergente entre notebooks
# MAGIC (ex.: um notebook usando 60 dias de dormência e outro usando 90 por esquecimento).
# MAGIC
# MAGIC Todos os demais notebooks começam com `%run ./notebooks/00_config/00_config` (ou path
# MAGIC relativo equivalente) para herdar essas variáveis no seu escopo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ambiente / Catálogo

# COMMAND ----------

# Widget para alternar entre dev/staging/prod sem editar código.
# Em produção real, isso seria injetado pelo orquestrador (Databricks Jobs) via job parameters.
# Guard "dbutils" in dir(): permite rodar este mesmo arquivo fora do Databricks (validação
# local/CI) sem quebrar na primeira linha por falta do objeto dbutils.
if "dbutils" in dir():
    dbutils.widgets.dropdown("env", "dev", ["dev", "staging", "prod"])
    ENV = dbutils.widgets.get("env")
else:
    ENV = "dev"

# Usamos Unity Catalog com 3 níveis: catalog.schema.table.
# Um catalog por ambiente evita que um job de dev escreva acidentalmente em cima de prod
# (mais seguro do que isolar só por schema).
CATALOG = f"flutter_martech_{ENV}"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Flag para permitir rodar este mesmo código fora do Databricks (validação local/CI),
# usando Parquet em vez de Delta e caminhos de arquivo em vez de tabelas de catálogo.
# Delta é nativo no runtime do Databricks (sem download de JAR); localmente, sem acesso
# a um cluster, Parquet é o formato de fallback para testar a lógica de transformação.
RUNNING_ON_DATABRICKS = "dbutils" in dir() and hasattr(dbutils, "widgets") and \
    not globals().get("FORCE_LOCAL_MODE", False)
STORAGE_FORMAT = "delta" if RUNNING_ON_DATABRICKS else "parquet"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raiz do repositório (calculada, não assumida)
# MAGIC
# MAGIC O working directory de um notebook dentro de Databricks Repos varia entre versões de
# MAGIC runtime (às vezes é a pasta do próprio notebook, às vezes a raiz do repo). Em vez de
# MAGIC assumir um dos dois, perguntamos ao próprio contexto do notebook onde ele está e
# MAGIC subimos até a raiz do repo — funciona igual não importa de onde o notebook é chamado.

# COMMAND ----------

if RUNNING_ON_DATABRICKS:
    _notebook_path = (
        dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        .notebookPath().get()
    )
    # _notebook_path é algo como /Repos/<usuario>/flutter_case/notebooks/00_config/00_config
    # A raiz do repo é tudo antes de "/notebooks/".
    REPO_ROOT = "/Workspace" + _notebook_path.split("/notebooks/")[0]
else:
    # Execução local: tests/_run_local.py sempre roda com cwd = raiz do repositório.
    REPO_ROOT = "."

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paths de origem (raw data)
# MAGIC
# MAGIC Em produção, isso seria um volume do Unity Catalog (`/Volumes/...`) alimentado por um
# MAGIC processo de landing (ex.: Fivetran, SFTP drop, ou export do sistema transacional).
# MAGIC Aqui apontamos para o path relativo do repositório para a entrega do case ser
# MAGIC autocontida e reproduzível sem depender de um volume específico de workspace.

# COMMAND ----------

# Path relativo à raiz do repositório (não ao notebook individual). Pressupõe execução com
# working directory = raiz do repo, que é o padrão ao rodar via Databricks Job/Workflow
# apontando para um Databricks Repo, e é o que o notebook 02_execution garante explicitamente.
# Para ESTE case, os dados brutos vivem dentro do próprio repositório (pasta data/),
# então lemos de lá tanto local quanto no Databricks — é o que realmente existe para
# rodar. Em uma implantação de produção real, isso apontaria para um Volume do Unity
# Catalog alimentado por um processo de landing (Fivetran, SFTP drop, export do sistema
# transacional) em vez de um CSV versionado no Git — comentário deixado aqui de propósito
# para a conversa técnica sobre a diferença entre "rodar o case" e "produção real".
RAW_DATA_PATH = f"{REPO_ROOT}/data"

RAW_FILES = {
    "players": f"{RAW_DATA_PATH}/players.csv",
    "bets": f"{RAW_DATA_PATH}/bets.csv",
    "deposits": f"{RAW_DATA_PATH}/deposits.csv",
    "campaigns": f"{RAW_DATA_PATH}/campaigns.csv",
    "campaign_touchpoints": f"{RAW_DATA_PATH}/campaign_touchpoints.csv",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data de referência do negócio
# MAGIC
# MAGIC O case fixa "hoje" = 2024-04-01 para tornar a régua de dormência determinística e
# MAGIC reproduzível (senão o resultado mudaria a cada execução). Em produção, este valor
# MAGIC viria de `current_date()` — deixamos como parâmetro explícito justamente para poder
# MAGIC trocar por `current_date()` em uma linha no dia do go-live.

# COMMAND ----------

REFERENCE_DATE = "2024-04-01"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Câmbio (Frankfurter API)

# COMMAND ----------

# Base URL fixa na v1 conforme exigido pelo case. Nunca montar a URL por concatenação
# livre de string do usuário — sempre a partir desta constante, para evitar quebrar em
# caso de mudança de versão da API.
FRANKFURTER_BASE_URL = "https://api.frankfurter.dev/v1"

# Moeda de destino de toda a normalização financeira do pipeline.
TARGET_CURRENCY = "BRL"

# Moedas esperadas nos dados brutos (vocabulário fechado observado no perfilamento).
# Qualquer moeda fora desta lista deve ser tratada como imperfeição de dado (quarentena),
# não silenciosamente ignorada.
EXPECTED_CURRENCIES = ["BRL", "USD", "EUR"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Taxonomia oficial de campanhas
# MAGIC
# MAGIC Vocabulário controlado fornecido pelo negócio (seção 2b do guia do case) — não é
# MAGIC inferível a partir dos dados, por isso vive como configuração, não como lógica
# MAGIC descoberta via profiling.

# COMMAND ----------

CAMPAIGN_TAXONOMY_PATTERN = "{geo}_{channel}_{objective}_{product}_{audience}_{period}_{offer}"

CAMPAIGN_TAXONOMY_VOCAB = {
    "geo": ["BR", "PT", "AO"],
    "channel": ["email", "push", "sms"],
    "objective": ["acquisition", "reactivation", "retention", "crosssell"],
    "product": ["sports", "casino", "both"],
    "audience": ["new", "active", "dormant", "vip"],
    # period segue padrão YYYYQ# (ex.: 2024Q1) — validado por regex, não por lista fechada.
    "offer": ["bonus50", "bonus100", "freebet", "freespins", "cashback", "none"],
}

# Correções conhecidas de erros de digitação/variação observados no profiling de campaigns.csv.
# Mantidas explícitas (em vez de fuzzy-matching automático/silencioso) para que a decisão de
# "isso é o mesmo valor com erro de digitação" seja auditável e revisável por um humano,
# e não uma inferência estatística que pode errar silenciosamente.
CAMPAIGN_TYPO_CORRECTIONS = {
    "objective": {"reactivaton": "reactivation"},
    "audience": {"dorment": "dormant"},
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Premissas de negócio — Dormência e LTV
# MAGIC
# MAGIC Estas constantes materializam as decisões alinhadas com o time de Growth antes da
# MAGIC implementação (ver README, seção "Decisões e Premissas", para o racional completo).

# COMMAND ----------

# --- Dormência ---
# Definição adotada: um jogador é "dormente" se o tempo desde sua última atividade
# (o mais recente entre depósito confirmado e aposta) excede um limiar de dias.
#
# O limiar é IDEALMENTE pessoal (baseado na cadência histórica do próprio jogador), mas
# testamos essa hipótese no profiling e, com este volume amostral (poucos depósitos por
# jogador, distribuídos por só ~8 meses), a cadência pessoal calculada é estatisticamente
# ruidosa (mediana de intervalo de apenas 6 dias, com muitos intervalos de 0 dias — sinal
# de geração quase aleatória dos timestamps, não de comportamento real).
#
# Por isso: usamos cadência pessoal SOMENTE para quem tem depósitos confirmados
# suficientes para ela ser minimamente confiável; para o restante, caímos em um
# threshold-padrão de mercado. Um piso/teto evita que outliers de amostra pequena gerem
# thresholds absurdos (ex.: 3 dias).
DORMANCY_MIN_DEPOSITS_FOR_PERSONAL_CADENCE = 5
DORMANCY_CADENCE_MULTIPLIER = 2.5
DORMANCY_THRESHOLD_FLOOR_DAYS = 30
DORMANCY_THRESHOLD_CAP_DAYS = 90
DORMANCY_FALLBACK_THRESHOLD_DAYS = 60  # usado quando não há amostra suficiente p/ cadência pessoal

# Jogadores sem NENHUM depósito confirmado e NENHUMA aposta no histórico inteiro não são
# "dormentes" no sentido de reativação — nunca converteram. Tratamos como segmento à parte
# ("never_converted") e os excluímos da campanha de reativação (é caso de aquisição, não
# retenção/reativação — oferta e mensagem seriam completamente diferentes).
EXCLUDE_NEVER_CONVERTED_FROM_REACTIVATION = True

# --- Elegibilidade para campanha (regra de negócio, não de qualidade de dado) ---
# Autoexclusão é regra de jogo responsável: um jogador que se autoexcluiu NUNCA deve ser
# alvo de uma campanha de reativação, independente de quão "valioso" ele seja.
EXCLUDE_SELF_EXCLUDED_FROM_TARGETING = True

# KYC rejeitado normalmente impede o jogador de sacar/operar plenamente na conta — incluir
# esses jogadores numa campanha de reativação com oferta de bônus tende a gerar custo sem
# retorno (ele não consegue de fato jogar/sacar). Tratamos como não-elegível por padrão,
# mas documentamos como premissa revisável junto ao time de Compliance/Risk.
EXCLUDE_KYC_REJECTED_FROM_TARGETING = True

# --- Depósitos ---
# Apenas depósitos com status 'confirmed' representam dinheiro real na operação.
# 'pending' e 'failed' são mantidos como colunas separadas na camada Silver/Gold para
# decisão a jusante (ex.: taxa de falha alta pode indicar fricção de pagamento — insight
# relevante para a recomendação, mesmo não entrando no valor monetário do jogador).
DEPOSIT_VALID_STATUS = "confirmed"

print(f"[00_config] ENV={ENV} | STORAGE_FORMAT={STORAGE_FORMAT} | REFERENCE_DATE={REFERENCE_DATE}")

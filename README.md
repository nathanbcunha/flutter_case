# Flutter Brazil — Case Técnico Martech Specialist

Pipeline de dados em arquitetura medalhão (Bronze/Prata/Ouro) no Databricks, para responder:
**quais jogadores dormentes vale a pena reativar, com qual oferta, e quanto eles valem?**

---

## Como rodar

### No Databricks (produção real)

1. Importe este repositório via **Databricks Repos** (Git integration).
2. Rode os notebooks na ordem: `notebooks/00_config` → `notebooks/01_setup` → cada
   notebook de `notebooks/bronze/` → cada notebook de `notebooks/silver/` → cada notebook
   de `notebooks/gold/`. Ou, mais simples: abra `notebooks/02_execution/02_execution` e
   rode ele — ele orquestra tudo na ordem certa via `dbutils.notebook.run`.
3. **Pré-requisito:** working directory do cluster = raiz do repositório (padrão ao rodar
   via Databricks Repos/Workflow). Os paths de dado (`data/*.csv`) e de output local são
   relativos à raiz do repo, não ao notebook individual.
4. Sem variáveis de ambiente/segredo necessárias — a API de câmbio (Frankfurter) é
   pública, sem autenticação.
5. Tabelas ficam em `{catalog}.bronze/silver/gold.*` no Unity Catalog, onde
   `catalog = flutter_martech_{env}` (`env` é um widget no `00_config`, default `dev`).

### Localmente (validação/revisão sem workspace Databricks)

Este é o modo usado para gerar os arquivos em `outputs/` deste repositório, já que não
havia um workspace Databricks disponível durante o desenvolvimento.

```bash
pip install pyspark==3.5.1

# roda o pipeline inteiro (bronze -> silver -> gold) e escreve outputs/ como Parquet + CSV
MOCK_FRANKFURTER=1 python3 tests/_run_local.py \
  notebooks/00_config/00_config.py \
  notebooks/01_setup/01_setup.py \
  notebooks/bronze/bronze_fx_rates.py \
  notebooks/bronze/bronze_players.py \
  notebooks/bronze/bronze_bets.py \
  notebooks/bronze/bronze_deposits.py \
  notebooks/bronze/bronze_campaigns.py \
  notebooks/bronze/bronze_campaign_touchpoints.py \
  notebooks/silver/silver_players.py \
  notebooks/silver/silver_bets.py \
  notebooks/silver/silver_deposits.py \
  notebooks/silver/silver_campaign_touchpoints.py \
  notebooks/silver/silver_campaigns.py \
  notebooks/gold/gold_player_ltv.py \
  notebooks/gold/gold_dormant_segments.py \
  notebooks/gold/gold_channel_offer_product_ltv.py \
  notebooks/gold/gold_recommendation.py
```

`tests/_run_local.py` emula o `%run` do Databricks (mesmo namespace Python entre
notebooks). A flag `MOCK_FRANKFURTER=1` troca a chamada real à API de câmbio por um
fixture local (`tests/fixtures/frankfurter_timeseries_MOCK.json`) — **só necessário
porque o sandbox onde este case foi desenvolvido não tinha saída de rede liberada para
`api.frankfurter.dev`**. O código de `bronze_fx_rates.py` em si chama a API real via
`urllib`; em Databricks (que tem internet liberada) ou em qualquer outro ambiente com
rede aberta, ele funciona sem essa flag e sem nenhuma mudança de código. Ver
`tests/fixtures/README.md` para detalhe.

---

## O que cada notebook faz, e onde os resultados ficam

| Notebook | Faz | Output |
|---|---|---|
| `00_config/00_config` | Paths, moedas, vocabulário da taxonomia, premissas de dormência/LTV | — (variáveis compartilhadas) |
| `01_setup/01_setup` | Cria catálogo/schema, funções `write_table`/`read_table`/`convert_to_brl`/`log_step` | — |
| `bronze/bronze_fx_rates` | Busca câmbio histórico na Frankfurter API, forward-fill de fins de semana | `bronze.fx_rates_raw`, `bronze.fx_rates_dense_calendar` |
| `bronze/bronze_players` | Ingestão bruta de `players.csv` | `bronze.players` |
| `bronze/bronze_bets` | Ingestão bruta de `bets.csv` | `bronze.bets` |
| `bronze/bronze_deposits` | Ingestão bruta de `deposits.csv` (duplicatas preservadas) | `bronze.deposits` |
| `bronze/bronze_campaigns` | Ingestão bruta de `campaigns.csv` | `bronze.campaigns` |
| `bronze/bronze_campaign_touchpoints` | Ingestão bruta de `campaign_touchpoints.csv` | `bronze.campaign_touchpoints` |
| `silver/silver_players` | Trata nulos de canal, casts, flag de elegibilidade para targeting | `silver.players` |
| `silver/silver_bets` | Casts, conversão para BRL, cálculo de GGR por aposta | `silver.bets` |
| `silver/silver_deposits` | Dedup de duplicatas exatas, conversão para BRL, flag de confirmado | `silver.deposits` |
| `silver/silver_campaign_touchpoints` | Casts, flag de evento futuro | `silver.campaign_touchpoints` |
| `silver/silver_campaigns` | **Parser da taxonomia** + resolução de conflito de canal | `silver.campaigns`, `silver.campaign_taxonomy_compliance_report` |
| `gold/gold_player_ltv` | LTV consolidado por jogador + status de dormência | `gold.player_ltv` |
| `gold/gold_dormant_segments` | Segmentação por valor dos dormentes elegíveis | `gold.dormant_segments` |
| `gold/gold_channel_offer_product_ltv` | LTV por canal de aquisição × oferta × produto | `gold.channel_offer_product_ltv`, `gold.channel_offer_deposits` |
| `gold/gold_recommendation` | Recomendação de negócio final, com números calculados ao vivo | `gold.recommendation_summary` (+ texto impresso) |

Localmente, cada tabela vira `outputs/{camada}/{tabela}` (Parquet) e
`outputs/{camada}/{tabela}_csv/` (CSV, um único arquivo, para revisão fora do Databricks).

---

## Decisões e premissas

### 1. Definição de "jogador dormente"

Um jogador é dormente se o tempo desde sua última atividade (o mais recente entre
depósito **confirmado** e aposta) excede um limiar de dias.

**A ideia inicial era um limiar 100% pessoal** (baseado na cadência histórica de cada
jogador) — testamos isso no profiling: a mediana de intervalo entre depósitos
consecutivos de um mesmo jogador é de **6 dias**, com muitos intervalos de **0 dias**.
Isso é sinal de que, neste dataset sintético (~8 meses, poucos depósitos por jogador), os
timestamps não carregam um padrão comportamental real — aplicar cadência pessoal
literalmente classificaria quase toda a base como dormente em ~15 dias, o que não passa
no teste de sanidade.

**Solução adotada — híbrida:**
- Cadência pessoal só é usada para jogadores com **≥ 5 depósitos confirmados**
  (`DORMANCY_MIN_DEPOSITS_FOR_PERSONAL_CADENCE`), com **piso de 30 e teto de 90 dias**
  para não deixar um outlier gerar um threshold absurdo.
- Para os demais (maioria da base neste volume amostral), usamos um threshold-padrão de
  mercado de iGaming: **60 dias**.
- Jogadores sem NENHUM depósito confirmado e NENHUMA aposta (**7 jogadores**) são
  `never_converted` — não entram no público de reativação (nunca converteram; é caso de
  aquisição, não de retenção).

Com mais dados (12+ meses, volume real de produção), a cadência pessoal dominaria e o
fallback seria raramente acionado.

### 2. Métrica de "valor" (LTV)

Combinamos três métricas em BRL, todas calculadas por jogador:

| Métrica | O que mede |
|---|---|
| `turnover_brl` (Σ stake) | Volume de atividade |
| `ggr_brl` (Σ stake − Σ payout) | Receita real gerada |
| `net_deposits_confirmed_brl` | Dinheiro que entrou |
| `hold_rate` = ggr/turnover | Eficiência de monetização |
| `wallet_burn_rate` = ggr/depósitos | % do que depositou virou receita |
| `theoretical_balance_brl` = depósitos − ggr | Saldo teórico remanescente (**proxy**, não saldo real — ver Limitações) |

**Achado de sanidade importante:** o GGR agregado deste dataset é **negativo** em vários
cortes (jogadores, no total, ganharam mais do que apostaram). Isso foge do padrão real de
operação de apostas — a casa normalmente tem vantagem estatística positiva. É esperado em
dados fictícios gerados sem impor essa vantagem, não um bug do pipeline. **Por isso, para
ranquear valor na segmentação usamos `net_deposits_confirmed_brl`** (mais estável), e os
hold rates de GGR são usados de forma **comparativa/direcional** entre ofertas na
recomendação, não como P&L real.

### 3. Depósitos por status

`confirmed` é o único status que conta como valor real (LTV). `pending` e `failed` ficam
visíveis como colunas separadas em toda a cadeia (Silver → Gold) para quem quiser
investigar fricção de pagamento — não descartamos esse dado, só não o tratamos como
receita.

### 4. Duplicatas em `deposits.csv`

25 `deposit_id` duplicados — confirmamos no profiling que são duplicatas **exatas**
(todas as colunas idênticas), consistente com reenvio de evento na origem.
`dropDuplicates()` sobre a linha inteira resolve com segurança. A Bronze preserva as
duplicatas (fiel à origem); o dedup acontece na Prata, documentado e contável.

### 5. Nulos em `acquisition_channel`

22 jogadores (8.8%) sem canal de aquisição. Substituímos por `'unknown'` explícito em vez
de descartar a linha — esses jogadores continuam tendo depósitos/apostas e não podem
desaparecer do LTV por canal.

### 6. Taxonomia de campanhas

Parser em duas camadas:
1. **Checagem estrita**: nome bate 100% com o padrão oficial literal (separador `_`,
   ordem, capitalização certos) → `taxonomy_compliant = True`.
2. **Recuperação por correspondência de vocabulário**: se falhar, normalizamos
   (separador `-`→`_`, case, erros de digitação conhecidos como `reactivaton`→
   `reactivation` e `dorment`→`dormant`) e procuramos, **para cada segmento**, um token
   que bate com o vocabulário daquele segmento — independente de posição. Recupera nomes
   com ordem trocada ou ruído (`C006`: `..._v2_FINAL`) sem exigir match perfeito da
   string inteira. → `taxonomy_recovered = True`.

Resultado sobre as 12 campanhas reais: **4 conformes, 6 recuperadas automaticamente, 2
exigem revisão manual** (`C007` nome vazio; `C008` texto livre em português, sem relação
com o vocabulário — nenhum token reconhecível).

**Fonte de verdade em conflito — achado real, não hipotético:** `C003` tem
`channel=email` no nome, mas **100% dos touchpoints reais dela (84 eventos) foram
enviados por `push`**. Decidimos que `campaign_touchpoints.channel` é a fonte de verdade
para o canal *efetivamente usado* — é um log operacional gerado no momento do disparo,
não pode estar "errado" no sentido de digitação humana. `campaign_name` é um rótulo
digitado por uma pessoa e pode divergir da execução real. Nunca sobrescrevemos
silenciosamente: `parsed_channel` (do nome) e `effective_channel` (dos touchpoints) ficam
lado a lado, com uma flag `channel_source_conflict` explícita.

### 7. Câmbio (Frankfurter API)

- Base EUR (padrão da API/ECB); buscamos EUR→BRL e EUR→USD numa única chamada de time
  series e derivamos USD→BRL localmente (`eur_to_brl / eur_to_usd`) — menos round-trips,
  uma única fonte cambial.
- **Forward-fill para fins de semana/feriados**: o ECB não publica cotação nesses dias,
  mas jogadores apostam todo santo dia. Usamos a última cotação disponível anterior — é
  a prática padrão de mercado.
- Conversão aplicada por **data da transação** (não uma taxa fixa única), para refletir o
  câmbio real do dia de cada aposta/depósito.

### 8. Eventos futuros em `campaign_touchpoints`

2 touchpoints com `event_ts` posterior a 2024-04-01 (data de referência). Não removidos —
são um evento real — mas flagados (`is_future_dated`) e **excluídos de qualquer lógica
que dependa de "o que já aconteceu até hoje"** (ex.: atribuição de oferta por último
toque), para não vazar informação futura na análise.

### 9. Atribuição de oferta (LTV por canal × oferta × produto)

`acquisition_channel` é atributo direto do jogador; `offer` não é — vive em `campaigns`,
e um jogador pode ter tido touchpoints de várias campanhas. Adotamos **atribuição de
último toque (last-touch)**: a oferta da campanha do touchpoint mais recente de cada
jogador (antes da data de referência). É a simplificação mais comum quando não há um
modelo multi-touch implementado — ver Limitações.

`deposits.csv` não tem coluna de produto na origem (só `bets.csv` tem) — por isso a
quebra por produto existe para turnover/GGR (de apostas), não para depósito confirmado.

### 10. Elegibilidade para targeting (não é limpeza de dado, é regra de negócio)

Jogadores **autoexcluídos** (`self_excluded=true`, 14 jogadores) nunca são alvo de
campanha — é regra de jogo responsável, não opcional. Jogadores com **KYC rejeitado**
também ficam de fora por padrão (não conseguem operar/sacar plenamente) — essa é revisável
com o time de Compliance/Risk, mas adotamos como padrão conservador.

### 11. Resiliência a rede restrita (ex.: Databricks Free Edition)

Alguns workspaces (notadamente o **Databricks Free Edition**, gratuito) restringem saída
de rede a uma lista fechada de domínios confiáveis — `api.frankfurter.dev` não está nela,
então a chamada de câmbio falha por lá. Em vez de deixar o pipeline inteiro travar por
causa de uma dependência externa fora do nosso controle, `bronze_fx_rates.py` cai para um
**seed de câmbio versionado no repositório** (`data/fx_rates_fallback_seed.json`) quando
todas as tentativas de chamar a API real falham — **sempre com aviso explícito no log e
uma coluna `used_fallback_source`** rastreando a origem, nunca silenciosamente. Em
qualquer ambiente com rede liberada (Databricks pago, ou local com internet), o fallback
nunca é acionado. Testado localmente forçando o cenário de falha (ver
`tests/_run_local.py` sem a flag `MOCK_FRANKFURTER`) — o pipeline completa normalmente.

---

## Recomendação de negócio (resumo — números completos no notebook `gold_recommendation`)

- **Quem mirar:** os segmentos "alto valor" e "médio valor" da base dormente elegível —
  **~52% dos jogadores dormentes concentram ~88% do valor histórico depositado**. É a
  alavanca de maior eficiência dado orçamento limitado.
- **Com qual oferta:** entre as ofertas com custo de incentivo, `bonus50` tem o melhor
  hold rate histórico (menos negativo) frente a `freebet`, `freespins`, `cashback` e
  `bonus100`.
- **Com qual produto:** `sports` tem hold rate historicamente melhor que `casino` nesta
  base.
- **Trade-off explícito:** os hold rates são direcionais (ver achado de sanidade sobre
  GGR negativo, item 2 acima) — servem para comparar ofertas entre si, não como previsão
  de lucro absoluto da campanha.

---

## O que ficou de fora / o que faria diferente com mais tempo

- **Testes de dado automatizados** (Great Expectations / Delta Live Tables expectations)
  entre camadas — hoje a validação é o profiling manual documentado aqui + os `log_step`
  de contagem em cada notebook, mas não há um "gate" automático que barre o pipeline se
  uma regra de qualidade quebrar.
- **Atribuição multi-touch** em vez de last-touch — daria um crédito mais justo entre
  canais/ofertas quando um jogador foi tocado por várias campanhas antes de agir.
- **Taxa de resposta histórica de campanhas de reativação anteriores** — não existe no
  dataset fornecido. Sem isso, a recomendação usa valor histórico do jogador como proxy
  de prioridade, não uma previsão de ROI esperado da campanha específica (seria o próximo
  passo natural: um modelo de propensão a responder).
- **Dados de saque (withdrawals)** — não existem na base. `theoretical_balance_brl` é uma
  aproximação (depósitos − GGR) que superestima o saldo real de qualquer jogador que já
  sacou. Com dados de saque, essa métrica ficaria muito mais confiável para decidir
  "quanto esse jogador ainda tem em jogo".
- **CI**: rodar `tests/_run_local.py` num GitHub Actions a cada PR, travando merge se o
  pipeline quebrar — não implementado por escopo/tempo.
- **Incrementalidade real** (Auto Loader + MERGE INTO) em vez de overwrite total — ver
  `docs/architecture.md`, seção Incrementalidade, para o desenho completo.

---

## Estrutura do repositório

```
notebooks/
  00_config/          parâmetros centralizados
  01_setup/            funções utilitárias compartilhadas (write_table, convert_to_brl...)
  bronze/               ingestão bruta (1 notebook por tabela + FX)
  silver/               limpeza, tipagem, normalização de câmbio, parser de taxonomia
  gold/                  LTV, segmentação, recomendação
  02_execution/       orquestração completa
data/                    os 5 CSVs originais fornecidos no case
outputs/                 tabelas geradas (Parquet + CSV), já rodadas localmente
docs/architecture.md     desenho de produção (agendamento, incrementalidade, governança)
tests/
  _run_local.py          emulador de %run para validação local
  fixtures/               fixture de câmbio para teste local (ver README ali)
```

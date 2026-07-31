# Arquitetura de produção

Este documento descreve como o pipeline rodaria em produção de verdade — não só uma vez
num notebook — conforme pedido no item 3a.4 do guia do case.

## Visão geral

```mermaid
flowchart TD
    subgraph Origem
        SRC1[Sistema transacional\nplayers / bets / deposits]
        SRC2[CRM / Martech\ncampaigns / campaign_touchpoints]
        SRC3[Frankfurter API\ncâmbio ECB]
    end

    subgraph Landing
        VOL[Unity Catalog Volume\nlanding/raw_exports]
    end

    subgraph Databricks Workflow diário
        B1[Bronze: fx_rates]
        B2[Bronze: players/bets/deposits/\ncampaigns/touchpoints]
        S1[Silver: players]
        S2[Silver: bets + FX]
        S3[Silver: deposits + FX]
        S4[Silver: touchpoints]
        S5[Silver: campaigns\nparser de taxonomia]
        G1[Gold: player_ltv]
        G2[Gold: dormant_segments]
        G3[Gold: channel_offer_product_ltv]
        G4[Gold: recommendation]
    end

    subgraph Consumo
        BI[Dashboards / BI]
        CRM_OUT[Ativação de campanha\nCRM/Martech]
    end

    SRC1 --> VOL
    SRC2 --> VOL
    SRC3 --> B1
    VOL --> B2
    B1 --> S2
    B1 --> S3
    B2 --> S1
    B2 --> S2
    B2 --> S3
    B2 --> S4
    S4 --> S5
    S1 --> G1
    S2 --> G1
    S3 --> G1
    G1 --> G2
    S1 --> G3
    S2 --> G3
    S3 --> G3
    S4 --> G3
    S5 --> G3
    G2 --> G4
    G3 --> G4
    G4 --> BI
    G2 --> CRM_OUT
```

## Orquestração

- **Databricks Workflows** (Jobs) aponta para o repositório Git (Databricks Repos), não
  para uma cópia manual de notebooks — todo deploy é via merge na branch principal
  (CI/CD com GitHub Actions/Azure DevOps rodando `databricks bundle deploy` é o próximo
  passo natural, fora do escopo deste case por tempo).
- Cada notebook do repo vira uma **task** no grafo do Workflow, com `depends_on`
  explícito replicando a ordem já documentada em `02_execution`.
- **Agendamento:** diário, após a janela de corte do sistema de origem (ex.: 03:00 UTC).
  A tabela Gold fica pronta antes do horário comercial do time de Growth.
- **Cluster:** job cluster efêmero (não all-purpose) — sobe só para a execução, evita
  custo de cluster ocioso. Dado o volume atual (KBs), um cluster single-node já basta;
  o desenho em Spark/Delta garante que escalar para milhões de linhas não exige reescrever
  nada, só ajustar o cluster.

## Incrementalidade

O pipeline atual roda em modo `overwrite` (adequado ao volume do case). Em produção, com
volume real crescendo todo dia, a evolução natural seria:

- **Bronze:** Auto Loader (`cloudFiles`) para ingestão incremental dos exports do sistema
  de origem, com checkpoint de arquivo já processado — nunca reprocessa o mesmo arquivo
  duas vezes.
- **Silver/Gold:** `MERGE INTO` (upsert) por chave (`player_id`, `bet_id`, etc.) em vez de
  overwrite total, processando só a partição de data mais recente. Isso também abre
  caminho para Structured Streaming se a latência de horas deixar de ser suficiente.
- **FX rates:** cache incremental — só buscar da API as datas que ainda não existem na
  tabela Bronze, não o range inteiro toda vez (o notebook atual já isola essa lógica em
  uma função dedicada, então essa mudança é local a `bronze_fx_rates`).

## Qualidade e observabilidade

- **Testes de dado** (não implementados neste case por tempo, mas seriam o próximo passo):
  Delta Live Tables expectations ou Great Expectations rodando entre camadas — ex.:
  "nenhum `player_id` em `bets` sem correspondência em `players`", "`stake_brl` nunca
  nulo para moeda válida".
- **Alertas:** o Workflow notifica Slack/e-mail em falha de task. Uma falha na busca de
  câmbio (rede instável) não deveria travar Bronze de players/bets/deposits, que são
  independentes — por isso essas tasks não dependem uma da outra no grafo.
- **Auditoria:** colunas `_ingested_at` e `_source_file` em toda tabela Bronze/Silver já
  presentes no código atual são a base disso — qualquer linha é rastreável até a carga que
  a gerou.

## Governança de dado

- **Unity Catalog** com 3 catálogos (`dev`/`staging`/`prod`), já refletido em `00_config`.
- Permissões por schema: Bronze restrito a engenharia de dados (contém PII de jogador
  bruta); Gold liberado para o time de Growth/BI consumir via dashboard, sem acesso direto
  às camadas anteriores.

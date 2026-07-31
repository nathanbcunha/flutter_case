# Fixture de câmbio para testes locais

`frankfurter_timeseries_MOCK.json` é um câmbio **sintético** (random walk em torno de
faixas realistas: EUR/BRL ~5.2-5.5, EUR/USD ~1.05-1.10), gerado localmente, cobrindo
2023-07-25 a 2024-04-01 em dias úteis (fins de semana ausentes, de propósito — é isso que
permite testar a lógica de forward-fill em `bronze_fx_rates.py`).

**Por que existe:** o sandbox onde este case foi desenvolvido não tinha saída de rede
liberada para `api.frankfurter.dev`, então não foi possível chamar a API real a partir
dali para gerar os outputs de exemplo deste repositório. `bronze_fx_rates.py` é escrito
para chamar a API real via `urllib` — em Databricks, ou qualquer ambiente com internet
liberada, ele funciona sem nenhuma mudança de código e sem este fixture.

**Como é usado:** `tests/_run_local.py`, quando chamado com `MOCK_FRANKFURTER=1`,
intercepta `urllib.request.urlopen` e devolve o conteúdo deste arquivo em vez de bater na
rede. Fora desse harness de teste, o fixture não é referenciado em lugar nenhum do
pipeline.

**O que isso significa para os números em `outputs/`:** os valores absolutos em BRL nos
outputs deste repositório usam essa cotação sintética, não a cotação real do período — as
proporções, agregações e lógica de negócio (dedup, dormência, taxonomia, segmentação) são
válidas; os valores monetários exatos mudariam ao rodar com a API real.

# Fixture de câmbio para testes locais

`frankfurter_timeseries_MOCK.json` é um câmbio **sintético** (random walk em torno de
faixas realistas: EUR/BRL ~5.2-5.5, EUR/USD ~1.05-1.10), gerado localmente, cobrindo
2023-07-25 a 2024-04-01 em dias úteis (fins de semana ausentes, de propósito — é isso que
permite testar a lógica de forward-fill em `bronze_fx_rates.py`).

**Por que existe:** o teste local (`tests/_run_local.py`) roda fora do Databricks, sem
depender de conexão de rede real com `api.frankfurter.dev` — útil tanto para
desenvolvimento offline quanto para uma futura esteira de CI, que idealmente não deveria
depender de uma API externa estar no ar para validar a lógica de transformação.
`bronze_fx_rates.py` é escrito para chamar a API real via `urllib` sempre em primeiro
lugar — no Databricks, ou em qualquer ambiente com internet liberada, ele funciona sem
nenhuma mudança de código e sem este fixture. O fixture só entra em cena quando a flag de
teste `MOCK_FRANKFURTER=1` é usada explicitamente.

**Como é usado:** `tests/_run_local.py`, quando chamado com `MOCK_FRANKFURTER=1`,
intercepta `urllib.request.urlopen` e devolve o conteúdo deste arquivo em vez de bater na
rede. Fora desse harness de teste, o fixture não é referenciado em lugar nenhum do
pipeline.

**O que isso significa para os números em `outputs/`:** os valores absolutos em BRL nos
outputs locais deste repositório usam essa cotação sintética, não a cotação real do
período — as proporções, agregações e lógica de negócio (dedup, dormência, taxonomia,
segmentação) são válidas. **Os números oficiais desta entrega são os da execução real no
Databricks**, em `gold.recommendation_summary` e demais tabelas Gold, que usam a
Frankfurter API de verdade (ou o fallback documentado na seção 11 do README, quando a
rede do workspace está restrita).


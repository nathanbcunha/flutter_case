# Roteiro de apresentação — Case Martech Specialist


---

## 1. Abertura (1-2 min)

> "O time de Growth tinha duas perguntas sem resposta confiável: quanto vale cada jogador
> (porque os valores estão em 3 moedas diferentes) e qual oferta funciona melhor (porque a
> taxonomia de campanhas virou uma bagunça). Construí um pipeline no Databricks, arquitetura
> medalhão, que resolve os dois problemas e termina numa recomendação de quem reativar."

Não entrar em código ainda — abrir com o problema de negócio, não com a arquitetura.

---

## 2. Arquitetura (3-5 min)

- Bronze → Prata → Ouro, versionado no GitHub, com `00_config` (parâmetros/premissas
  centralizadas), `01_setup` (funções compartilhadas), 1 notebook por tabela por camada,
  `02_execution` orquestrando tudo.
- **Mostrar rodando de verdade** (se der tempo): abrir `02_execution` ou navegar pelo
  Catalog Explorer mostrando `flutter_martech_dev.bronze/silver/gold`.
- Frase-chave pra deixar clara a intenção de produção: *"isso não foi pensado pra rodar
  uma vez no meu notebook — dá pra ver o desenho completo de produção em
  `docs/architecture.md`: Databricks Workflows, incrementalidade via Auto Loader/MERGE,
  alertas, Unity Catalog governando acesso por camada."*

---

## 3. Achados de qualidade de dados — "detalhismo" (5-7 min)

Isso é o que mais pesa na avaliação, segundo o guia. Não recite a lista — escolha 3-4
achados fortes e conte como história, não como checklist:

1. **Duplicatas em deposits**: 25 `deposit_id` duplicados, confirmados como duplicatas
   *exatas* (não conflito de dado) — dedup seguro, documentado, contável.
2. **A taxonomia de campanhas**: mostre 2-3 exemplos do parser recuperando conteúdo de
   nomes bagunçados (ex.: `C006` — ordem trocada + ruído `_v2_FINAL`, ainda assim 7/7
   segmentos recuperados). Resultado: 4 conformes / 6 recuperadas / 2 revisão manual.
3. **O conflito de fonte de verdade — este é o achado mais forte pra citar**: `C003` diz
   `channel=email` no nome, mas 100% dos touchpoints reais (84 eventos) foram por `push`.
   Decisão: touchpoints é a fonte de verdade (log operacional, não digitação humana).
4. **GGR agregado negativo**: teste de sanidade que *falhou* na primeira leitura ingênua —
   jogadores ganharam mais do que apostaram, no total. Não é bug, é característica de dado
   fictício sem vantagem de casa embutida. Por isso a métrica de ranking virou depósito
   confirmado, e os hold rates viraram comparativos, não P&L real.

> Frase de fechamento desse bloco: *"prefiro mostrar onde os dados me surpreenderam e como
> reagi, a fingir que tudo bateu de primeira."*

---

## 4. Análise (5 min)

**Dormência**: comece pela hipótese errada, não pela resposta certa — mostra raciocínio.
> "Minha primeira ideia era um threshold 100% pessoal, baseado na cadência de cada
> jogador. Testei: mediana de intervalo entre depósitos de 6 dias, com muitos de 0 dias —
> sinal de que os timestamps não têm padrão comportamental real nesse volume amostral.
> Ajustei para híbrido: cadência pessoal só com ≥5 depósitos confirmados, com piso/teto;
> fallback de 60 dias pra maioria."

**Segmentação**: 250 jogadores → 130 dormentes → 116 elegíveis (14 fora por autoexclusão/KYC)
→ 4 segmentos por quartil de valor depositado.

---

## 5. RECOMENDAÇÃO DE NEGÓCIO (3c) — a parte pra fechar, escrita pro gerente de Growth

*(Esta seção é a resposta literal ao item 3c do guia — pode ser lida quase como está.)*

### Quem mirar

Dos **250 jogadores da base**, 130 estão dormentes. Depois de tirar quem não pode ser
contatado por regra de jogo responsável (autoexcluídos) ou compliance (KYC rejeitado),
sobram **116 jogadores dormentes elegíveis para a campanha**.

Desses 116, **não recomendo mirar todo mundo com o mesmo orçamento.** Os dados mostram uma
concentração de valor muito clara:

| Segmento | Jogadores | Valor histórico depositado | Dormente há (média) |
|---|---|---|---|
| Alto valor | 31 | R$ 300.802,25 | 83 dias |
| Médio valor | 29 | R$ 101.638,97 | 114 dias |
| Baixo valor | 28 | R$ 46.572,54 | 119 dias |
| Valor mínimo | 28 | R$ 7.551,72 | 124 dias |

**Recomendação:** priorizar Alto + Médio valor — **60 jogadores (52% da base dormente
elegível) que concentram R$ 402.441,22, ou 88% de todo o valor histórico da base
dormente.** Com um orçamento que só dê pra alcançar metade do público, essa metade
capta quase 9 em cada 10 reais de valor em jogo. É a alavanca de maior eficiência.

Bônus estatístico que ajuda a decisão: o segmento de alto valor está dormente há **menos
tempo** (83 dias) que o de valor mínimo (124 dias) — ou seja, os jogadores mais valiosos
também são os "menos frios", plausivelmente mais fáceis de trazer de volta.

### Com qual oferta e produto

Olhando o histórico de hold rate (o quanto cada oferta/produto reteve de receita sobre o
volume apostado):

| Oferta | Hold rate |
|---|---|
| **bonus50** | **-1,02%** ← melhor custo-benefício |
| freebet | -2,05% |
| freespins | -4,10% |
| cashback | -19,08% |
| bonus100 | -28,44% |

| Produto | Hold rate |
|---|---|
| **sports** | **-0,94%** ← melhor margem |
| casino | -5,05% |

**Recomendação:** oferta `bonus50`, direcionada a `sports`, é a combinação com melhor
retorno histórico observado entre as opções com custo de incentivo real.

### Valor esperado

R$ 402.441,22 é o valor histórico já comprovado desses 60 jogadores — é o tamanho da
oportunidade, não uma previsão de quanto a campanha específica vai recuperar (não temos
histórico de taxa de resposta de campanhas de reativação anteriores nesta base para
projetar isso com confiança — ver trade-offs).

### Trade-offs (para deixar claro, com transparência, se perguntarem)

- Os hold rates são **direcionais** — comparam ofertas entre si, não representam lucro
  real da operação (ver achado do GGR negativo).
- A oferta atribuída a cada jogador é por **último toque** — se ele foi impactado por
  várias campanhas, 100% do crédito vai pro último touchpoint. Simplificação necessária
  sem um modelo de atribuição multi-touch implementado.
- "Valor" é depósito histórico comprovado, **não uma previsão de resposta à campanha**.
  O próximo passo natural (fora de escopo aqui) seria um modelo de propensão a responder,
  usando resultado de campanhas de reativação anteriores — que não existem neste dataset.

---

## 6. Limitações de ambiente/tempo e o que eu implementaria numa produção real

*(Seção importante para deixar claro que as escolhas foram conscientes, dado o escopo do
case — não desconhecimento das alternativas.)*

### Limitações que moldaram as decisões desta entrega

- **Tempo**: o guia pede 45min-1h de implementação; priorizei profundidade nas decisões
  de negócio (dormência, taxonomia, atribuição) e testar o pipeline de ponta a ponta de
  verdade, em vez de cobrir mais ferramentas superficialmente.
- **Ambiente**: rodei o pipeline de ponta a ponta na **Free Edition** do Databricks (a
  versão gratuita), que tem restrições reais que um workspace pago não tem — rede de
  saída limitada a domínios confiáveis (por isso o fallback de câmbio) e cota apertada de
  compute serverless. Essas restrições geraram decisões de engenharia genuínas (o
  fallback, o cálculo dinâmico da raiz do repo), mas também limitaram o que dava pra
  configurar de verdade nesta janela de tempo (ex.: Databricks Workflows/Jobs eu descrevi
  em `docs/architecture.md`, mas não cheguei a configurar um de verdade).

### dbt — a peça que mais mudaria a arquitetura com mais tempo

O pedido original incluía um notebook de dbt por tabela. Na prática, optei por
**PySpark puro em notebooks Databricks**, não dbt, por uma troca consciente: dado o
tempo disponível, escrever a lógica direto em PySpark (com testes locais reais, via
`tests/_run_local.py`) me deu mais confiança de que tudo *rodava de verdade* do que
configurar o adapter `dbt-databricks`, perfis de conexão, e a estrutura de projeto dbt do
zero dentro da mesma janela.

**Numa implementação de produção real, migraria a Prata e a Ouro para dbt** (a Bronze
tende a continuar em Python/Auto Loader, que é onde dbt não brilha tanto — ingestão bruta
não é o forte dele). O que isso ganharia sobre o que existe hoje:

| Hoje (PySpark + `%run`) | Com dbt |
|---|---|
| Dependência entre notebooks via `%run` manual, ordem decidida por mim no README/`02_execution` | `ref()` declarativo — o dbt monta o grafo de dependência sozinho a partir do SQL |
| Sem teste de dado automatizado — só os `log_step` de contagem que escrevi | Testes nativos (`unique`, `not_null`, `accepted_values`, `relationships`) rodando a cada execução, falhando o pipeline se uma regra quebrar |
| Documentação das decisões só no README, separada do código | `dbt docs generate` — documentação e **lineage visual** gerados automaticamente a partir do próprio projeto |
| Comentários de premissa como texto em `%md` | `schema.yml` com descrições por coluna, testáveis e versionadas junto com o modelo |
| Reprocessamento incremental exigiria eu implementar `MERGE INTO` manualmente | Materialização `incremental` built-in, com estratégia de merge configurável por poucas linhas de config |

Trade-off honesto: dbt teria me obrigado a reescrever o parser de taxonomia (que é lógica
procedural em Python, bag-of-tokens) como uma UDF externa chamada de dentro do SQL do
dbt, ou mantê-lo como um step Python separado antes do dbt — não é 100% natural encaixar
ali, mas ainda compensaria pelo ganho em teste/documentação/lineage do resto do pipeline.

### Outros itens da lista "faria diferente com mais tempo" (ver também README)

- **CI/CD real**: GitHub Actions rodando `tests/_run_local.py` (ou os testes dbt) a cada
  PR, e Databricks Asset Bundles para deploy versionado do Workflow — hoje o deploy é
  manual via Databricks Repos.
- **Orquestração de produção de verdade**: configurar o Databricks Workflow descrito em
  `docs/architecture.md`, com agendamento, alertas de falha e retries — hoje só existe o
  desenho, não a implementação.
- **Atribuição multi-touch** em vez de last-touch.
- **Dados de saque (withdrawals)** para saldo real, não a aproximação atual.
- **Modelo de propensão a responder**, usando histórico de campanhas de reativação
  anteriores (que não existe neste dataset) — passaria de "quem já provou valer" para
  "quem tem maior chance de responder a esta campanha específica".

---

## 7. Perguntas prováveis da banca (ensaiar resposta curta pra cada uma)

| Pergunta provável | Resposta-chave |
|---|---|
| "Por que não usar cadência 100% pessoal pra dormência?" | Testei, dado esparso demais nesse volume amostral (mediana 6 dias, muitos 0 dias) — não é sinal comportamental real |
| "Por que confiar em touchpoints e não no nome da campanha pro canal?" | Touchpoint é log operacional do disparo; nome é digitação humana, mais sujeito a erro |
| "O GGR negativo não invalida a análise?" | Não — é característica do dataset sintético sem vantagem de casa; por isso usei depósito confirmado pra ranquear valor, não GGR |
| "Por que last-touch e não multi-touch?" | Simplificação documentada; multi-touch é o próximo passo natural com mais tempo |
| "Isso escala pra produção de verdade?" | Sim — `docs/architecture.md`: Databricks Workflows, Auto Loader incremental, MERGE INTO, alertas, Unity Catalog por camada |
| "Por que não usaram dbt, já que foi mencionado no início?" | Troca consciente de tempo: PySpark puro + testes locais reais deram mais confiança dentro da janela do case do que configurar o adapter dbt-databricks do zero; ver seção 6 para o plano de migração |
| "O que você faria diferente com mais tempo?" | dbt para Prata/Ouro, testes de dado automatizados, atribuição multi-touch, dados de saque pra saldo real, modelo de propensão a responder |

---

## 8. Como fechar

> "A resposta pra pergunta original da liderança: mirem os 60 jogadores de alto e médio
> valor — 52% da base dormente, 88% do valor — com oferta bonus50 em sports. É a alavanca
> de maior eficiência dado o orçamento limitado, com os trade-offs que acabei de mostrar."

Termina exatamente com a recomendação de negócio, como o guia pede — não com "mais alguma
pergunta?" antes disso.

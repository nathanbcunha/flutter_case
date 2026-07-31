"""
Harness de validação LOCAL do pipeline (fora do Databricks).
Não faz parte da entrega de produção — existe só para provar que a lógica dos notebooks
roda de ponta a ponta e gerar os artefatos reais em outputs/ para revisão no repositório
(item 4a-ii do guia do case), já que não há um workspace Databricks disponível aqui.

Emula o comportamento do "%run" do Databricks executando cada notebook, em ordem, no
mesmo namespace Python -- exatamente como aconteceria de fato dentro do Databricks.
"""
import sys, os, runpy, json, io

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NS = {"__name__": "__main__"}

# --------------------------------------------------------------------------------------
# Mock de rede SOMENTE para este harness de teste local: o sandbox de validação não tem
# saída de internet para api.frankfurter.dev (whitelist de rede do ambiente). O notebook
# bronze_fx_rates.py em si é escrito para chamar a API real via urllib -- em Databricks ou
# qualquer ambiente com internet liberada, ele bate na API de verdade sem nenhuma mudança
# de código. Isso aqui só intercepta a chamada para não travar a validação local.
# --------------------------------------------------------------------------------------
MOCK_FX = os.environ.get("MOCK_FRANKFURTER") == "1"
if MOCK_FX:
    import urllib.request

    FIXTURE_PATH = os.path.join(BASE, "tests", "fixtures", "frankfurter_timeseries_MOCK.json")

    class _MockResponse:
        def __init__(self, payload):
            self._payload = json.dumps(payload).encode("utf-8")
            self.status = 200
        def read(self):
            return self._payload
        def __enter__(self):
            return self
        def __exit__(self, *a):
            return False

    def _mock_urlopen(url, timeout=15):
        print(f"[MOCK urlopen] {url}  (fixture local, ver tests/fixtures/README)")
        with open(FIXTURE_PATH) as f:
            payload = json.load(f)
        return _MockResponse(payload)

    urllib.request.urlopen = _mock_urlopen

def run_nb(rel_path):
    full = os.path.join(BASE, rel_path)
    print(f"\n{'='*70}\n>>> RUNNING {rel_path}\n{'='*70}")
    with open(full) as f:
        code = f.read()
    exec(compile(code, full, "exec"), NS)

if __name__ == "__main__":
    notebooks = sys.argv[1:]
    for nb in notebooks:
        run_nb(nb)

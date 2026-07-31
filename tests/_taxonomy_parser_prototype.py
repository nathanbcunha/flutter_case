import re

CAMPAIGN_TAXONOMY_VOCAB = {
    "geo": ["BR", "PT", "AO"],
    "channel": ["email", "push", "sms"],
    "objective": ["acquisition", "reactivation", "retention", "crosssell"],
    "product": ["sports", "casino", "both"],
    "audience": ["new", "active", "dormant", "vip"],
    "offer": ["bonus50", "bonus100", "freebet", "freespins", "cashback", "none"],
}
CAMPAIGN_TYPO_CORRECTIONS = {"reactivaton": "reactivation", "dorment": "dormant"}
SEGMENTS_ORDER = ["geo", "channel", "objective", "product", "audience", "period", "offer"]
PERIOD_PATTERN = re.compile(r"^\d{4}q[1-4]$")


def parse_campaign_taxonomy(raw_name):
    if raw_name is None or raw_name.strip() == "":
        return dict(**{s: None for s in SEGMENTS_ORDER}, taxonomy_compliant=False,
                    taxonomy_recovered=False,
                    parse_notes="nome de campanha vazio - requer categorização manual")

    # --- Passo 1: checagem ESTRITA sobre o texto original (sem normalizar nada) ---
    # É isso que responde "quantas campanhas seguem o padrão oficial de verdade hoje".
    raw_tokens = raw_name.split("_")
    strict_ok = len(raw_tokens) == 7
    if strict_ok:
        strict_result = {}
        for seg, tok in zip(SEGMENTS_ORDER, raw_tokens):
            if seg == "period":
                if PERIOD_PATTERN.match(tok.lower()) and tok[-2] == "Q":
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

    # --- Passo 2: normalização + recuperação por correspondência de vocabulário (bag-of-tokens) ---
    # Trata: separador '-' em vez de '_', case inconsistente, ordem trocada, tokens extras
    # de ruído, e erros de digitação conhecidos (CAMPAIGN_TYPO_CORRECTIONS).
    normalized = re.sub(r"[-\s]+", "_", raw_name.strip())
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    tokens = [t.lower() for t in normalized.split("_") if t != ""]
    tokens = [CAMPAIGN_TYPO_CORRECTIONS.get(t, t) for t in tokens]

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
            candidates = [i for i, tok in enumerate(tokens) if i not in used_idx and tok in vocab_lower[seg]]
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
             f"(formato/ordem não padrão)"]
    if missing:
        notes.append(f"segmentos não encontrados: {missing}")
    if unmatched:
        notes.append(f"tokens ignorados (não reconhecidos): {unmatched}")

    return dict(**recovered, taxonomy_compliant=False, taxonomy_recovered=True,
                parse_notes="; ".join(notes))


TEST_CASES = [
    ("C001", "BR_email_reactivation_casino_dormant_2024Q1_bonus50"),
    ("C002", "PT-push-acquisition-sports-new-2024Q1-freebet"),
    ("C003", "br_EMAIL_Reactivation_Casino_Dormant_2024Q1_BONUS50"),
    ("C004", "BR_email_reactivation_2024Q1_bonus50"),
    ("C005", "BR_push_reactivaton_casino_dorment_2024Q1_bonus_50"),
    ("C006", "BR_email_2024Q1__both_crosssell___vip_freespins_v2_FINAL_"),
    ("C007", ""),
    ("C008", "Reativacao Janeiro"),
    ("C009", "BR_sms_retention_sports_active_2024Q1_cashback"),
    ("C010", "PT_email_acquisition_casino_new_2024Q1_bonus100"),
    ("C011", "BR_push_reactivation_sports_dormant_2024Q1_freebet"),
    ("C012", "_email_AO__vip_crosssell_both_2024Q1_none_"),
]

for cid, name in TEST_CASES:
    r = parse_campaign_taxonomy(name)
    print(f"{cid:5s} | compliant={r['taxonomy_compliant']!s:5s} recovered={r['taxonomy_recovered']!s:5s} "
          f"geo={r['geo']} channel={r['channel']} obj={r['objective']} prod={r['product']} "
          f"aud={r['audience']} period={r['period']} offer={r['offer']}")
    print(f"       notes: {r['parse_notes']}")

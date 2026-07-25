# MockSQL Accuracy Benchmarks

Harness: `examples/eval/` (LLM-as-judge) + `mocksql test --frozen` (deterministic replay) · Latest runs 2026-06 → 2026-07

---

## What this benchmark measures (and why it isn't all `1.00`)

MockSQL is **not** a static analyzer with a deterministic ground truth. It generates
synthetic test data with an LLM, runs it locally on DuckDB, and assigns an argued
verdict. So its accuracy can't be reduced to a single F1 table where the ground truth
*is* the parser — a benchmark like that would be near-tautological, and a wall of
perfect scores would tell you nothing about real behavior.

Instead we report **three distinct measurements**, stratified by corpus **and by the
model that generated the data** (the number moves a lot with the model — that's the
point of showing it):

1. **Generation quality** — is the synthetic data coherent, does it exercise the
   scenario, is the test readable? *Judged by an LLM, so it carries judge noise.*
2. **Replay fidelity** — deterministic, zero-LLM: does a frozen test still reproduce
   the exact rows it was confirmed against? *This is the regression gate.*
3. **Parity** — do the local DuckDB results match the real warehouse? *This is what
   backs the "0 € on BigQuery" promise.*

The honest, uneven numbers below are the feature. A perfect score would be the bug.

---

## 1. Generation quality (LLM-as-judge)

Each generated test is scored on three axes (1–5) by an independent judge with a blank
context, plus a binary `is_valid` and the DuckDB `exec_status`. Averages are over the
**whole corpus** (including invalid tests), latest run per corpus.

| Corpus | Source | Generation model | n | valid | pass rate | data coherence | test coherence | readability | exec complete |
|--------|--------|------------------|---|-------|-----------|----------------|----------------|-------------|---------------|
| spider | Spider · BigQuery | `gpt-5-mini` | 26 | 25 | **96.2%** | 4.73 | 4.65 | 4.38 | 26/26 |
| fdp | private set | `gemini-3.1-flash-lite-preview` | 11 | 11 | **100%** | 4.73 | 4.82 | 4.36 | 11/11 |
| thelook | thelook_ecommerce · BigQuery | `gemini-3.1-flash-lite-preview` | 14 | 11 | **78.6%** | 4.79 | 4.79 | 4.79 | 13/14 |
| spider2-snow | Spider 2.0 · Snowflake | `gemini-2.5-flash-lite` | 109 | 74 | **67.9%** | 3.75 | 3.77 | 4.39 | 88/109 |

Dogfood set (`_mocksql_eval`, n=5, `flash-lite-preview`): 5/5 valid, axes 4.2 / 4.4 / 4.4.

### The three axes

| Axis | Key | Question |
|------|-----|----------|
| Data coherence | `cohérence_données` | Do the injected rows actually isolate the described scenario? |
| Test coherence | `cohérence_test` | Do the assertions validate the business logic without ambiguity? |
| Readability | `lisibilité_métier` | Would a data engineer understand the scenario at a glance? |

`is_valid` = the judge's overall go/no-go. `exec_status` = `complete` (DuckDB returned
the expected shape) vs `empty_results` (a CTE returned 0 rows — a generation miss the
executor retry loop couldn't recover). Note these are **orthogonal**: a test can execute
cleanly (`complete`) yet be judged invalid because the data doesn't truly cover the case.

### Reading the spread

The gap between spider (96.2% on `gpt-5-mini`) and spider2-snow (67.9% on
`gemini-2.5-flash-lite`) is driven by **both** corpus difficulty *and* model choice:
spider2-snow is Spider 2.0's hardest real-world Snowflake SQL, run on a deliberately
cheap/weak model. Readability stays high across the board (4.38–4.79) even where data
coherence drops — the failures are typically *wrong data for the scenario*, not
*unreadable tests*.

---

## 2. Replay fidelity (deterministic, zero-LLM)

Once a test is confirmed, its expected output is frozen and replayed with **no LLM
involved** — pure DuckDB. This is the layer that makes MockSQL a regression gate rather
than a one-shot generator.

Corpus: **spider2-snow**, 111 models, `mocksql test --frozen`.

| Measurement | Result |
|-------------|--------|
| Judge ↔ row-comparison agreement (88 cases with both assertions and an `expect` contract) | **86 / 88 (97.7%)** |
| Frozen replay pass (79 confirmed cases) | **78 / 79** |

The two agreement mismatches both argue **for** row comparison over assertions:

- `sf_local003` — same rows, different order: a **real tie** on the sort key
  (`AverageSalesPerOrder = 100.0 ×2`). Row comparison caught a non-determinism the
  assertions were blind to.
- `sf_bq444` — output **byte-identical** to the contract, but a timestamp assertion
  fired anyway: a false positive of the assertion language.

The single frozen "fail" (`sf_bq263`) is a contract frozen *before* a UTC-tz fix — the
replay correctly catches the tz-corrected value. The gate working as designed.

---

## 3. Parity (DuckDB ↔ warehouse)

`mocksql parity` replays each saved test **on the real warehouse** (BigQuery /
Snowflake) with the *same synthetic data*, and compares the two result sets. This
audits the transpilation layer that the "runs locally on DuckDB" promise rests on.

It's an **opt-in, on-demand audit**, not a corpus — there is no aggregate score to
publish (and inventing one would misrepresent it). What is specified and tested is the
comparison contract:

- **Order**: order-insensitive (multiset) unless the query has a terminal `ORDER BY`.
- **Floats**: relative tolerance `1e-9`.
- **Normalized before compare**: decimals → canonical; dates/timestamps → ISO 8601 UTC;
  `NULL ≡ NULL` regardless of carrier type; JSON/VARIANT → canonical.
- **Strings compared as-is**: a case/trim difference **is** a diff (it may be the
  collation, and you must see it).
- **Attestation**: a matching test gets a committed `sha256` fingerprint over
  (normalized SQL + data + dialect + transpiler version). Change any input → the
  attestation goes stale and the test becomes replay-eligible again.

Full contract and exit codes → **[docs/parity.md](parity.md)**.

---

## Methodology

### Corpora

All queries are drawn from public benchmark suites (Spider, Spider 2.0, thelook) or
private model sets — no hand-picked cherries. Ground truth for generation quality is an
LLM judge with a **blank context** (it never sees MockSQL's own reasoning); ground truth
for replay fidelity is the frozen, human-or-legacy-confirmed output.

### Judge

`examples/eval/generate_tests.py` drives generation via the HTTP/SSE API; the judge
(the `eval-mocksql` skill) scores each test on the three axes above. The judge is
instructed to grade **coherence** (narrative ↔ data ↔ SQL), not realism.

### Reproducibility

```bash
# 1. Generate tests for a corpus (backend must be running on :8100)
cd examples/eval
python generate_tests.py --models sf001 sf002 ...

# 2. Judge them (writes results/<date>_<project>.json)
#    → via the eval-mocksql skill

# 3. Compare two runs for regressions
python compare.py results/<before>.json results/<after>.json --threshold 0.5

# 4. Deterministic replay fidelity (zero LLM)
mocksql test --frozen
```

Raw per-model results (SQL, injected data, judge reasoning, verdict) are committed as
timestamped JSON under **`examples/eval/results/`**.

---

## Caveats

The reasons *not* to read these numbers as a guarantee — stated plainly, because a
benchmark that hides them isn't a benchmark:

1. **LLM-judged, non-deterministic.** The generation-quality scores carry judge noise;
   re-running the judge can shift a borderline verdict. Known recurring false negatives
   are tracked — always check the raw `reasoning` before trusting a single score.
2. **Mixed generation models.** Corpora were run on different models across dates
   (`gpt-5-mini`, `gemini-2.5-flash-lite`, and the now-deprecated
   `gemini-3.1-flash-lite-preview`). The scores are **not** directly comparable across
   corpora — the model column is not decoration.
3. **Small corpora.** fdp (n=11), thelook (n=14) and the dogfood set (n=5) are too small
   for tight confidence intervals; treat them as directional.
4. **Deliberately weak model on the hard corpus.** spider2-snow (67.9%) ran on
   `flash-lite`, a cheap/weak tier. It is a floor, not a ceiling.
5. **No production SQL yet.** Everything here is public benchmark or private-project SQL,
   not anonymized customer production queries — the next validation step.
6. **Parity has no aggregate.** It's an on-demand audit; the "score" is per-run, in your
   terminal, against your warehouse.

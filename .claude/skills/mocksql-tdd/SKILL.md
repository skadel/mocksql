---
name: mocksql-tdd
description: Fix a SQL bug test-first with MockSQL — reproduce the bug as a failing (red) test replayed locally on DuckDB, fix the SQL, verify green, freeze the test as a regression gate. Use whenever asked to fix incorrect behavior in a SQL model (wrong values, unexpected NULLs, duplicated or missing rows, broken joins or casts).
---

# MockSQL TDD — reproduce, fix, freeze

You are fixing a bug in a SQL model. Do NOT patch the SQL first. Reproduce the bug
as a red test, then fix the SQL, then prove green. MockSQL replays tests locally on
DuckDB — deterministic, no LLM calls, nothing billed on the warehouse — so the
red/green loop is fast and free.

Run every command from the directory containing `mocksql.yml`.
`<model>` is the model name as listed under `models_path` (e.g. `orders`,
`demo/payment_summary`) or a path to the `.sql` file.

## 0. State the bug as a premise

Write the bug as one sentence: *"given INPUT SCENARIO, the output must DESIRED
BEHAVIOR"*. Example: "a customer with no orders must still appear, with
signup_date not NULL". Every later step checks against this premise.

## 1. Generate the scenario (data only — the test is born green)

```bash
mocksql test -m <model> --json                    # does a suite exist already?
mocksql generate <model> -i "repro: <premise>"    # additive: adds ONE test
```

Open `.mocksql/tests/<model>.json`, find the new case in `test_cases` (match its
description against your instruction), note its `test_uid`.

**Important:** at this point the test SNAPSHOTS THE BUGGY OUTPUT — its
`expect.rows` were recorded from the current (buggy) SQL, so the test is green by
construction. That is not the repro yet.

## 2. Make the contract prescriptive (this creates the red)

Hand-edit the case's `expect` block in `.mocksql/tests/<model>.json` (the file is
designed to be human-editable): replace the buggy value(s) with the DESIRED
behavior from the premise.

- Change only the column(s) the premise names. Keep every other value exactly as
  observed.
- Keep the observed formatting (same date string format, numbers stay numbers,
  `null` for NULL).
- If the premise column is missing from `expect.columns`, add it to `columns`
  **and** to every row of `expect.rows`.
- Do **not** run `mocksql confirm` now — confirm re-snapshots the CURRENT (buggy)
  output as the contract and would erase your edit.

## 3. Verify RED — and that it is red for the right reason

```bash
mocksql test -m <model> --json
```

Find your case by `test_uid`. A valid repro requires **both**:

1. `status` is `"unconfirmed"` and `expect_check.passed` is `false`. (The exit
   code stays 0 for unconfirmed cases — read the JSON, not the exit code.)
2. The diff differs ON THE PREMISE COLUMN: `expect_check.missing` (rows the
   contract wants but the SQL did not produce) vs `expect_check.unexpected` (rows
   the SQL produced outside the contract) — e.g. `missing` shows
   `"signup_date": "2024-01-15"` while `unexpected` shows `"signup_date": null`.

Rule out the two false reds:

- **Diff on another column, or `actual_count` is 0** → the test bench is wrong,
  not the SQL. Fix the input data first: hand-edit the case's `data` block, or run
  `mocksql update-test <model> -u <test_uid> -i "<what to change>"`.
  **Caution:** `update-test` re-records the draft `expect.rows` from the buggy
  output — redo step 2 afterwards. Then re-check red.
- **`expect_check.passed` is `true`** → the bug does NOT reproduce: the SQL
  already behaves as desired on this scenario. STOP and report this to the user
  instead of patching — either the premise is wrong or the bug lives elsewhere.

## 4. Fix the SQL

Edit the model's `.sql` source file. `mocksql test` reads the SQL fresh from disk
(not from a snapshot), so your edit is picked up on the next run.

## 5. Verify GREEN — and no collateral damage

```bash
mocksql test -m <model> --json    # your case must now be "status": "pass"
mocksql test --json               # full suite: no other case regressed
```

If your case is still red, iterate on step 4. Never edit `expect` again to make it
pass — the contract encodes the desired behavior; only the SQL moves.

## 6. Freeze as a regression gate

```bash
mocksql confirm <model> --test-uid <test_uid>
```

Now that the output is correct, confirm freezes it: `review.status` becomes
`confirmed`. Optionally export the confirmed test as a native dbt unit test:
`mocksql export dbt -t <model>`.

**CI gate — use `mocksql test --require-confirmed`.** Plain `mocksql test` exits 1
only when the SQL snapshot is unchanged but the output diverges. When the `.sql` is
edited (the usual way a regression is introduced), the confirmed contract goes
`stale` / `unconfirmed` and plain `mocksql test` still exits 0 (the diff is
reported, not failed — an SQL edit is presumed intentional and asks for
re-confirmation). To make any drift from a confirmed contract fail the build, gate
CI on `mocksql test --require-confirmed` (exit 1 on `stale` / `unconfirmed` / any
non-confirmed executed case).

## JSON reference (`mocksql test --json`)

Output is an array of `{model, sql_source, cases[]}`. Each case:

| field | meaning |
|---|---|
| `test_uid` | stable id — use it for `update-test` / `confirm` |
| `name`, `description` | test identity |
| `status` | `pass` · `fail` (confirmed contract violated) · `unconfirmed` (draft contract diverges — your red) · `error` · `skip` |
| `review` | `confirmed` · `stale` (SQL drifted since confirm) · `draft` |
| `expect_check` | `{passed, expected_count, actual_count, missing[], unexpected[], ordered, order_only_mismatch}` |

Exit code is 1 only if a CONFIRMED case fails on unchanged SQL (or, with
`--require-confirmed`, if any executed case is not confirmed — this is the gate to
use in CI, since an edited `.sql` turns a confirmed contract `stale`, not `fail`).

`sql_source` is `disk` (read from the live `.sql`, the default), `frozen`
(`--frozen`), or `snapshot-fallback` (the `.sql` was not found or unreadable, so
the frozen snapshot ran instead). **If you edited the `.sql` and see
`snapshot-fallback`, your file was not read** — a green result then reflects the
OLD snapshot, not your edit. Check the path/encoding before trusting the pass.

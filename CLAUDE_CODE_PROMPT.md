# Claude Code Prompt — Refactor & Implement DML Schema Conversion

Copy this prompt into Claude Code (terminal) at the root of the converter repo.

---

## The Prompt

```
You are working on an internal Ab Initio → PySpark converter. I need you to
audit the current DML schema conversion module, refactor it for correctness
and maintainability, and ensure all 25 test cases in DML_TEST_SUITE.md pass.

Work in this exact order. Do not skip steps. Do not jump ahead to coding.

═══════════════════════════════════════════════════════════════════════
PHASE 0 — EXPLORE (read-only, no edits yet)
═══════════════════════════════════════════════════════════════════════
1. Run `find . -type f \( -name "*.py" -o -name "*.md" -o -name "*.toml" \
   -o -name "*.cfg" -o -name "*.txt" \) | head -100` and `ls -la`.
2. Read README, pyproject.toml/setup.py, and any docs/ files.
3. Identify the modules responsible for:
   - DML lexing/tokenization
   - DML parsing (AST construction)
   - Type mapping (DML type → Spark type)
   - PySpark code emission
   - Read-logic generation (csv vs text+substring vs binary)
   - The existing test suite
4. Read every file in those modules end-to-end. Do NOT skim.
5. Open DML_TEST_SUITE.md and map each of the 25 test cases to the
   modules that will need to handle it.

DELIVERABLE for Phase 0: a written summary in your response containing:
  (a) Current architecture diagram (text/ASCII is fine)
  (b) For each TC-### in the suite: which existing module handles it,
      or "NOT IMPLEMENTED"
  (c) Top 5 code-quality issues you found (duplication, missing tests,
      tangled responsibilities, missing types, etc.)
  (d) A proposed target architecture
  (e) A migration plan (which files change, in what order, and why)

STOP after Phase 0 and wait for my "go" before continuing.

═══════════════════════════════════════════════════════════════════════
PHASE 1 — TEST FIXTURES (no production code changes yet)
═══════════════════════════════════════════════════════════════════════
1. Create tests/fixtures/tc_001/ through tests/fixtures/tc_025/.
2. For each test case in DML_TEST_SUITE.md:
   - input.dml          — the DML schema (verbatim from the suite)
   - expected_schema.json — the StructType serialized via .jsonValue()
   - sample_data.csv (or .dat for fixed/binary) — 3-5 sample rows
   - expected_output.json — DataFrame rows as JSON list
3. Build a parametrized pytest harness that loads each fixture and
   runs three assertions:
     (a) generated schema equals expected_schema.json
     (b) generated read code, when executed, produces expected_output.json
     (c) for TC-019/020/024 a warning is emitted with the documented
         "MANUAL_REVIEW" marker
4. Run the harness. EXPECT MANY FAILURES — that is the baseline.
5. Commit the fixtures and harness as a single commit:
   "test: add 25-case DML schema conversion fixture suite"

DELIVERABLE for Phase 1: pytest output showing the failure count broken
down by test case ID. STOP and wait for my "go".

═══════════════════════════════════════════════════════════════════════
PHASE 2 — REFACTOR (separate from feature work)
═══════════════════════════════════════════════════════════════════════
Apply the target architecture from Phase 0. Constraints:

1. Keep each layer single-purpose:
     dml_lexer.py     → tokens only, no semantic logic
     dml_parser.py    → AST only, no Spark knowledge
     dml_ast.py       → dataclasses for every DML construct
     type_mapper.py   → DML type AST → Spark DataType (pure function)
     emitter.py       → AST + type mapping → PySpark code string
     read_strategy.py → picks csv vs text+substring vs binaryFile
     warnings.py      → centralized warning emission for unsupported
                        constructs (unions, packed decimals, EBCDIC)
2. Every public function gets type hints and a docstring.
3. No regex parsing of DML in the emitter. All decisions come from the AST.
4. Replace any `eval`, string concatenation of code, or stringly-typed
   dispatch with proper data structures.
5. Remove dead code. If something has zero call sites after refactor,
   delete it (do not comment out).
6. The 25-case test suite must still run (still failing where features
   are missing — that's fine). No NEW failures introduced.
7. Commit in small, reviewable chunks:
     "refactor: extract DML AST into dataclasses"
     "refactor: separate type mapping from code emission"
     "refactor: centralize unsupported-construct warnings"
     ...

DELIVERABLE for Phase 2: pytest output showing same or fewer failures
than baseline, plus a `git log --oneline` of the refactor commits.
STOP and wait for my "go".

═══════════════════════════════════════════════════════════════════════
PHASE 3 — IMPLEMENT MISSING FEATURES
═══════════════════════════════════════════════════════════════════════
Work through DML_TEST_SUITE.md in order, easiest first:

  TC-001 → TC-005   primitive types, fixed-length, void
  TC-006 → TC-012   nullable, dates, defaults, fixed vectors,
                    mixed delimiters, hybrid string fields
  TC-013 → TC-019   nesting, variable vectors, conditionals, unions
  TC-020 → TC-025   packed decimals, includes, type aliases,
                    comments, EBCDIC, full combo

Rules:
1. One test case per commit. Commit message format:
     "feat(TC-007): map Ab Initio date formats to Spark patterns"
2. Before writing code for a TC, read its expected output from the
   suite TWICE. Confirm the expected behavior is reflected in the
   fixture files.
3. After each commit, run the FULL test suite (not just the new TC).
   If a previously-passing test breaks, fix it before moving on.
4. For TC-019, TC-020, TC-024: implement the documented degraded
   mapping AND emit a warning. Do not silently approximate.
5. For format strings (TC-007), build a single canonical mapping
   table in one place. Do not scatter token replacements.
6. For variable-length vectors (TC-015/016): generate code that
   uses `slice` and `transform` SQL expressions, not Python UDFs,
   unless a UDF is genuinely required.
7. After every 5 test cases, run `ruff check`, `mypy`, and the
   full pytest suite. Fix any new issues immediately.

DELIVERABLE for Phase 3: all 25 tests passing, clean lint/type-check,
and a CHANGELOG.md entry summarizing the new feature coverage.

═══════════════════════════════════════════════════════════════════════
PHASE 4 — DOCUMENTATION & SAFETY NETS
═══════════════════════════════════════════════════════════════════════
1. Update README with:
     - Supported DML constructs (full table from suite)
     - Known limitations and degraded mappings
     - How to add a new test case
2. Add docs/ARCHITECTURE.md describing the pipeline:
     dml file → tokens → AST → type-mapped AST → emitted PySpark
3. Add a CLI smoke test: convert a sample DML file and assert the
   output is valid Python (`ast.parse(generated_code)` succeeds).
4. Add a regression guard: a test that loads ALL fixtures and
   asserts no NEW warnings appear vs a recorded baseline.
5. Final commit: "docs: architecture, supported constructs, regression
   guard"

═══════════════════════════════════════════════════════════════════════
GROUND RULES (apply throughout)
═══════════════════════════════════════════════════════════════════════
- Never edit a file without viewing it first in the same session.
- Never delete a test. If a test is wrong, fix the test in its own
  commit with a clear "test: ..." message explaining why.
- If you discover the suite itself has a bug (wrong expected output),
  STOP, explain the bug, and ask before fixing.
- No silent best-guess type mappings. If a DML construct isn't covered
  by the test suite and isn't obviously a primitive, raise a clear
  error in the converter, do NOT guess.
- Prefer pure functions over classes with mutable state.
- Prefer dataclasses over dicts for structured data.
- Every commit must leave the repo in a runnable state. No WIP commits.

Begin with Phase 0. Output the Phase 0 deliverable, then stop.
```

---

## Why this prompt is structured the way it is

A few notes on the design choices, in case you want to tweak it for your repo's specifics:

**Phased with explicit stops.** Claude Code, like any agent, drifts on long tasks. Forcing a stop after Phase 0 gives you a chance to course-correct on the architecture proposal before any code is written — way cheaper than rewriting after the fact.

**Phase 0 is read-only.** This is the single most important constraint. Without it, agents start editing files before understanding the codebase, which is how you get tangled refactors.

**Refactor and feature work are separate phases.** Mixing them produces commits where you can't tell whether a change is a behavior change or a cleanup. Separating them makes review tractable and makes `git bisect` actually useful when something breaks later.

**Test fixtures before refactor.** The fixture suite acts as a behavioral lock — it pins what the converter is supposed to do, so the refactor can move code around without you having to manually verify each test case still works.

**One test case per commit in Phase 3.** When TC-019 (unions) breaks something downstream three months from now, you want a single commit to revert, not a bundle of five.

**Hard rules at the bottom.** "Never edit without viewing first" and "no silent best-guess type mappings" prevent two specific failure modes: agents editing stale files, and agents inventing type mappings that look plausible but are wrong (very common with mainframe types).

---

## Optional tweaks

- If your converter is small (< 2k LOC), you can compress Phases 1 and 2 into one prompt session — just delete the "STOP" between them.
- If you have a CI pipeline, add a Phase 3.5: "ensure CI passes before final commit."
- If your repo uses something other than pytest (e.g. unittest), swap the test-runner references.
- If you're using Claude Code's plan mode, paste only Phase 0 first, then paste each subsequent phase as a fresh prompt to keep context windows clean.

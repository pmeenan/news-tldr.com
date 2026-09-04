# Editorial quality evaluation

The regression suite separates structural correctness from editorial judgment.
Automated validation cannot establish that an article is true or that two reports
are independent. The synthetic fixture expectations are initial review guidance;
they have not received independent human sign-off.

## Run

```bash
./.venv/bin/python scripts/evaluate-editorial.py --dry-run
./.venv/bin/python scripts/evaluate-editorial.py --verbose
```

The first command lists cases with no network calls. The second uses the configured
full-Flash fallback chain and writes a private report to ignored
`data/evaluations/editorial.json`; `--output PATH` overrides that location.
Incremental status goes to stderr and the final result is JSON on stdout. This
command does not mutate pipeline events, checkpoints, stories or production.
Keep generated reports private: they include input text and quoted evidence.

## Human review

For each generated story, compare every assertion to the supplied fixture text.
Complete its `human_review` fields in a copy of the report:

- **Unsupported claims**: count invented facts, wrong numbers/units/dates, unsupported causal explanations, and allegations or vendor claims presented as independently established.
- **Missing qualifications**: count material caveats missing from the two-bullet briefing, especially preliminary data, rumor attribution, small observational samples, and unavailable independent testing.
- **Misleading headline**: record whether the headline alone overstates the evidence or loses necessary attribution.
- **Notes**: describe the error and corrected wording; record reviewer identity/date outside public site output.

Report unsupported claims per total reviewed factual assertions, missing
qualifications per case, and misleading headlines per story. Do not treat
`automatic_validation: passed` as a zero-error human assessment. Keep numerator,
denominator, model/prompt metadata and the reviewed output together when comparing
revisions. The five initial story fixtures are a smoke evaluation, not a
representative estimate of production accuracy.

The two event-boundary fixtures cover a contaminated Apple/product/leadership
cluster and a duplicated jobs release mixed with fuel prices. Compare partitions
as sets of article-index sets, ignoring group order. Report false merges and
false splits separately: reducing one by indiscriminately increasing the other
is not an improvement. Production spot checks should additionally include
long-lived clusters, multiple publishers carrying one wire report, single-outlet
original reporting, and late corrections.

## Current rollout

New editorial generation uses passage-backed evidence and verification. Existing
stories stay readable until updated or explicitly regenerated; presentation-only
builds do not retroactively verify them. Coherence repair is bounded per run.
Use normal incremental runs for ongoing migration and narrowly targeted
`editorial --force --event-id ID --verbose` for reviewed correction work. A failed
verification retains the previous artifact/checkpoint and records the error;
it must not be counted as a successfully improved story.

# Guam On Book Implementation Plan

> Execute inline in the isolated `codex/guam-onbook-import` checkout. User approved the data flow and requested implementation.

**Goal:** Build the local reservation-import workflow and an additive USD report for the existing Guam dashboard.

**Architecture:** A Python import reads full reservation snapshots and optional target inputs; atomic JSON outputs feed a static report. A folder watcher reruns only when input contents change and keeps dated snapshots for comparisons.

**Tech Stack:** Python 3, openpyxl, standard-library unittest, plain HTML/CSS/JS.

## Global Constraints
- Do not modify synced sources or existing work in other chats.
- No raw records or live business outputs committed.
- No automatic deployment before the existing dashboard is ready.
- Coverage dates and snapshot dates are explicit.

### Task 1: Import and reconciliation
- [ ] Write tests using hand-calculated fixtures for cancellation, dates, exclusive category routing, duplicate detection, wrong source rejection and malformed amounts.
- [ ] Run `python -m unittest discover -s scripts/guam_onbook/tests` to observe missing implementation failure.
- [ ] Implement `load_bookings(path)` and `aggregate(rows, as_of, start, end)` in `scripts/guam_onbook/import_onbook.py`.
- [ ] Verify monthly venue totals against the existing report, including detailed row mappings.

### Task 2: Targets and snapshots
- [ ] Test coverage mismatch suppresses deltas, zero targets avoid division by zero, missing targets remain null, duplicate target keys are rejected, and valid updates produce dated history plus atomic current output.
- [ ] Implement `load_targets(path)`, `compare(current, previous)` and command-line build/watch.
- [ ] Provide editable target workbook, configuration and double-click Mac runner under ignored `local/guam-onbook/`.

### Task 3: Report integration
- [ ] Create `docs/guam-onbook.html` with month/venue selection, KPI cards and category table using `docs/data/guam_onbook.json`.
- [ ] Show unavailable data and comparisons clearly; show unclassified amounts and source counts.
- [ ] Add only a report link inside existing Guam tab.
- [ ] Check JavaScript syntax, browser rendering, source total reconciliation and repository diff.
- [ ] Prepare a draft PR for later integration; attach it to the chat.

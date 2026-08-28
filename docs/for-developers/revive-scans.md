# Revive Scans

## Run Locally

```bash
make revive
```

This runs [revive](https://github.com/mgechev/revive) against the entire codebase, converts the JSON output to SARIF, and generates a human-friendly HTML report.

Reports are written to `revive-reports/`:

- `revive.json` — raw tool output in JSON format
- `revive.sarif` — machine-readable SARIF report for tool integrations
- `revive.html` — human-friendly HTML report, open in a browser for an easy-to-read view

To open the HTML report after the scan:

```bash
xdg-open revive-reports/revive.html   # Linux
open revive-reports/revive.html       # macOS
```

### Run all static analysis tools at once

To run revive together with staticcheck and errcheck in a single command:

```bash
make go-static-analysis
```

### Direct invocation

`make revive` installs the binary (if needed) and runs the full report. To invoke revive directly instead:

```bash
# Run and capture JSON output
bin/revive -formatter json ./... > revive-reports/revive.json

# Convert JSON to SARIF
python3 scripts/ci/go_tool_report_to_sarif.py revive \
    revive-reports/revive.json \
    revive-reports/revive.sarif

# Convert SARIF to HTML
python3 scripts/ci/sarif_to_html.py \
    revive-reports/revive.sarif \
    revive-reports/revive.html
```

## GitHub Code Scanning

`.github/workflows/go-static-analysis.yml` runs revive (alongside staticcheck and errcheck) on every push and pull request to `main`, on a weekly schedule (Tuesdays at `05:12 UTC`), and can be triggered manually via `workflow_dispatch`.

The workflow uploads `revive-reports/revive.sarif` using `github/codeql-action/upload-sarif` under the category `revive`.

Findings appear in GitHub `Security` > `Code scanning alerts`. GitHub tracks alerts by tool, rule, category, and location, so the same finding is not duplicated on repeated runs. New revive findings create new alerts; existing alerts remain open until revive no longer reports them.

Full scan output is always available from the workflow artifact named `revive-reports-<run_id>`.

## Why Revive May Not Appear

Revive findings appear in GitHub `Security` > `Code scanning alerts` only when all of these are true:

- The workflow actually runs the revive scan.
- SARIF is uploaded with `github/codeql-action/upload-sarif`.
- The workflow has `security-events: write` permission.
- For private repositories, GitHub code scanning is enabled and the repository plan supports third-party SARIF upload.


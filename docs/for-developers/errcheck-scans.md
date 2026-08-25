# Errcheck Scans

## Run Locally

```bash
make errcheck
```

This runs [errcheck](https://github.com/kisielk/errcheck) against the entire codebase, converts the text output to SARIF, and generates a human-friendly HTML report.

Reports are written to `errcheck-reports/`:

- `errcheck.txt` — raw tool output in plain text format
- `errcheck.sarif` — machine-readable SARIF report for tool integrations
- `errcheck.html` — human-friendly HTML report, open in a browser for an easy-to-read view

To open the HTML report after the scan:

```bash
xdg-open errcheck-reports/errcheck.html   # Linux
open errcheck-reports/errcheck.html       # macOS
```

### Run all static analysis tools at once

To run errcheck together with staticcheck and revive in a single command:

```bash
make go-static-analysis
```

### Direct invocation

`make errcheck` installs the binary (if needed) and runs the full report. To invoke errcheck directly instead:

```bash
# Run and capture text output
bin/errcheck -ignoregenerated ./... > errcheck-reports/errcheck.txt

# Convert text output to SARIF
python3 scripts/ci/go_tool_report_to_sarif.py errcheck \
    errcheck-reports/errcheck.txt \
    errcheck-reports/errcheck.sarif

# Convert SARIF to HTML
python3 scripts/ci/sarif_to_html.py \
    errcheck-reports/errcheck.sarif \
    errcheck-reports/errcheck.html
```

## GitHub Code Scanning

`.github/workflows/go-static-analysis.yml` runs errcheck (alongside staticcheck and revive) on every push and pull request to `main`, on a weekly schedule (Tuesdays at `05:12 UTC`), and can be triggered manually via `workflow_dispatch`.

The workflow uploads `errcheck-reports/errcheck.sarif` using `github/codeql-action/upload-sarif` under the category `errcheck`.

Findings appear in GitHub `Security` > `Code scanning alerts`. GitHub tracks alerts by tool, rule, category, and location, so the same finding is not duplicated on repeated runs. New errcheck findings create new alerts; existing alerts remain open until errcheck no longer reports them.

Full scan output is always available from the workflow artifact named `errcheck-reports-<run_id>`.

## Why Errcheck May Not Appear

Errcheck findings appear in GitHub `Security` > `Code scanning alerts` only when all of these are true:

- The workflow actually runs the errcheck scan.
- SARIF is uploaded with `github/codeql-action/upload-sarif`.
- The workflow has `security-events: write` permission.
- For private repositories, GitHub code scanning is enabled and the repository plan supports third-party SARIF upload.


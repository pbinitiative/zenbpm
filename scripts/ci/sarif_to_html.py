#!/usr/bin/env python3
"""Convert a SARIF file to a self-contained HTML report.
Usage: python3 sarif_to_html.py <input.sarif> <output.html>
Requires only Python 3 stdlib (json, sys, html, pathlib).
"""
import html
import json
import sys
from pathlib import Path


def severity_badge(level: str) -> str:
    level = str(level or "none")
    colours = {
        "error": "#d73a49",
        "warning": "#e36209",
        "note": "#0366d6",
        "none": "#6a737d",
    }
    colour = colours.get(level.lower(), "#6a737d")
    return (
        f'<span style="background:{colour};color:#fff;padding:2px 7px;'
        f'border-radius:3px;font-size:0.8em;font-weight:bold;">'
        f"{html.escape(level.upper())}</span>"
    )


def report_title(data: dict) -> str:
    tool_names = []
    for run in data.get("runs", []):
        tool_name = run.get("tool", {}).get("driver", {}).get("name", "")
        if tool_name and tool_name not in tool_names:
            tool_names.append(tool_name)

    if len(tool_names) == 1:
        return f"{tool_names[0]} Report"
    if len(tool_names) > 1:
        return "SARIF Report: " + ", ".join(tool_names)
    return "SARIF Report"


def render(sarif_path: str, html_path: str) -> None:
    with open(sarif_path, encoding="utf-8") as f:
        data = json.load(f)

    title = report_title(data)
    rows: list[str] = []
    for run in data.get("runs", []):
        tool_name = (
            run.get("tool", {}).get("driver", {}).get("name", "Unknown Tool")
        )
        rules: dict[str, dict] = {
            r["id"]: r
            for r in run.get("tool", {}).get("driver", {}).get("rules", [])
        }
        for result in run.get("results", []):
            rule_id = result.get("ruleId", "")
            rule = rules.get(rule_id, {})
            rule_name = rule.get("name", rule_id)
            message = result.get("message", {}).get("text", "")
            level = result.get("level", "warning")

            locations = result.get("locations", [])
            if locations:
                loc = locations[0].get("physicalLocation", {})
                uri = loc.get("artifactLocation", {}).get("uri", "")
                region = loc.get("region", {})
                line = region.get("startLine", "")
                location_str = f"{uri}:{line}" if line else uri
            else:
                location_str = ""

            help_uri = rule.get("helpUri", "")
            rule_link = (
                f'<a href="{html.escape(help_uri)}" target="_blank">'
                f"{html.escape(rule_id)}</a>"
                if help_uri
                else html.escape(rule_id)
            )

            rows.append(
                f"<tr>"
                f"<td>{severity_badge(level)}</td>"
                f"<td>{rule_link}<br/><small>{html.escape(rule_name)}</small></td>"
                f"<td><code>{html.escape(location_str)}</code></td>"
                f"<td>{html.escape(message)}</td>"
                f"<td>{html.escape(tool_name)}</td>"
                f"</tr>"
            )

    total = len(rows)
    rows_html = "\n".join(rows) if rows else (
        "<tr><td colspan='5' style='text-align:center;color:#6a737d;'>"
        "No findings 🎉</td></tr>"
    )

    page = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>{html.escape(title)}</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
         margin: 0; padding: 24px; background: #f6f8fa; color: #24292e; }}
  h1   {{ font-size: 1.6em; border-bottom: 1px solid #e1e4e8; padding-bottom: 8px; }}
  .summary {{ margin-bottom: 16px; color: #586069; }}
  table {{ width: 100%; border-collapse: collapse; background: #fff;
           border: 1px solid #e1e4e8; border-radius: 6px; overflow: hidden; }}
  th    {{ background: #f1f3f5; padding: 10px 14px; text-align: left;
           font-size: 0.85em; text-transform: uppercase; letter-spacing: .05em;
           border-bottom: 1px solid #e1e4e8; }}
  td    {{ padding: 10px 14px; border-bottom: 1px solid #f0f0f0; vertical-align: top;
           font-size: 0.9em; }}
  tr:last-child td {{ border-bottom: none; }}
  tr:hover td {{ background: #f6f8fa; }}
  code  {{ font-family: "SFMono-Regular", Consolas, monospace; font-size: 0.85em;
           background: #f1f3f5; padding: 1px 4px; border-radius: 3px; word-break: break-all; }}
</style>
</head>
<body>
<h1>🔍 {html.escape(title)}</h1>
<p class="summary">Source: <code>{html.escape(sarif_path)}</code> &nbsp;|&nbsp;
   <strong>{total}</strong> finding(s)</p>
<table>
<thead><tr>
  <th>Severity</th><th>Rule</th><th>Location</th><th>Message</th><th>Tool</th>
</tr></thead>
<tbody>
{rows_html}
</tbody>
</table>
</body>
</html>
"""
    output_path = Path(html_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(page, encoding="utf-8")
    print(f"HTML report written to: {html_path}  ({total} finding(s))")


def main() -> int:
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <input.sarif> <output.html>", file=sys.stderr)
        return 1
    try:
        render(sys.argv[1], sys.argv[2])
    except Exception as exc:
        print(f"error: failed to convert {sys.argv[1]} to HTML: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())


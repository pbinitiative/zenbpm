#!/usr/bin/env python3
"""Convert Go analyzer report output to SARIF.
Usage: python3 go_tool_report_to_sarif.py <staticcheck|errcheck|revive> <input-report> <output.sarif>
Requires only Python 3 stdlib.
"""
import json
import re
import sys
from pathlib import Path
from typing import Any


TOOL_METADATA = {
    "staticcheck": {
        "name": "Staticcheck",
        "informationUri": "https://staticcheck.dev/",
        "defaultRuleId": "staticcheck",
        "defaultRuleName": "Staticcheck diagnostic",
    },
    "errcheck": {
        "name": "errcheck",
        "informationUri": "https://github.com/kisielk/errcheck",
        "defaultRuleId": "unchecked-error",
        "defaultRuleName": "Unchecked error",
    },
    "revive": {
        "name": "revive",
        "informationUri": "https://revive.run/",
        "defaultRuleId": "revive",
        "defaultRuleName": "revive rule violation",
    },
}


LEVELS = {
    "error": "error",
    "warning": "warning",
    "warn": "warning",
    "info": "note",
    "information": "note",
    "notice": "note",
    "note": "note",
}

ERRCHECK_LINE_RE = re.compile(r"^(?P<file>.*?):(?P<line>\d+):(?P<column>\d+):\s*(?P<message>.*)$")


def load_json_or_ndjson(path: Path) -> Any:
    if not path.exists():
        return []

    text = path.read_text(encoding="utf-8").strip()
    if not text:
        return []

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        items: list[Any] = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                items.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSON line in {path}: {exc}") from exc
        return items


def case_get(mapping: dict[str, Any], *keys: str, default: Any = None) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    lower_keys = {k.lower(): k for k in mapping}
    for key in keys:
        actual = lower_keys.get(key.lower())
        if actual is not None:
            return mapping[actual]
    return default


def sarif_level(level: Any, default: str = "warning") -> str:
    if level is None:
        return default
    return LEVELS.get(str(level).lower(), default)


def normalize_uri(uri: Any) -> str:
    if not uri:
        return ""
    value = str(uri)
    try:
        return str(Path(value).resolve().relative_to(Path.cwd().resolve()))
    except ValueError:
        return value


def location(uri: Any, line: Any = 1, column: Any = 1) -> dict[str, Any]:
    try:
        line_no = int(line or 1)
    except (TypeError, ValueError):
        line_no = 1
    try:
        column_no = int(column or 1)
    except (TypeError, ValueError):
        column_no = 1

    region: dict[str, Any] = {"startLine": max(line_no, 1)}
    if column_no > 0:
        region["startColumn"] = column_no

    return {
        "physicalLocation": {
            "artifactLocation": {"uri": normalize_uri(uri)},
            "region": region,
        }
    }


def flatten_findings(data: Any) -> list[Any]:
    if data is None:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("issues", "findings", "results", "diagnostics", "failures"):
            value = case_get(data, key)
            if isinstance(value, list):
                return value
        return [data]
    return []


def convert_staticcheck(data: Any) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for diagnostic in flatten_findings(data):
        if not isinstance(diagnostic, dict):
            continue
        loc = case_get(diagnostic, "location", default={})
        if not isinstance(loc, dict):
            loc = {}
        rule_id = str(case_get(diagnostic, "code", "check", default="staticcheck"))
        message = str(case_get(diagnostic, "message", "text", default="Staticcheck diagnostic"))
        results.append(
            {
                "ruleId": rule_id,
                "level": sarif_level(case_get(diagnostic, "severity"), "warning"),
                "message": {"text": message},
                "locations": [location(case_get(loc, "file", "filename"), case_get(loc, "line"), case_get(loc, "column"))],
            }
        )
    return results


def convert_revive(data: Any) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for failure in flatten_findings(data):
        if not isinstance(failure, dict):
            continue
        pos = case_get(failure, "position", default={})
        if not isinstance(pos, dict):
            pos = {}
        start = case_get(pos, "start", default={})
        if not isinstance(start, dict):
            start = {}
        rule_id = str(case_get(failure, "ruleName", "rule", default="revive"))
        message = str(case_get(failure, "failure", "message", default="revive rule violation"))
        results.append(
            {
                "ruleId": rule_id,
                "level": sarif_level(case_get(failure, "severity"), "warning"),
                "message": {"text": message},
                "locations": [
                    location(
                        case_get(start, "filename", "file"),
                        case_get(start, "line"),
                        case_get(start, "column"),
                    )
                ],
            }
        )
    return results


def first_int(mapping: dict[str, Any], *keys: str, default: int = 1) -> int:
    for key in keys:
        value = case_get(mapping, key)
        try:
            return int(value)
        except (TypeError, ValueError):
            continue
    return default


def errcheck_message(finding: dict[str, Any]) -> str:
    function = case_get(finding, "function", "func", "func_name", default="")
    if function:
        return f"unchecked error from {function}"

    line_text = case_get(finding, "line", "line_text", "text", default="")
    if line_text:
        return f"unchecked error: {line_text}"

    return "unchecked error"


def errcheck_result(file_name: Any, finding: dict[str, Any]) -> dict[str, Any]:
    return {
        "ruleId": "unchecked-error",
        "level": "warning",
        "message": {"text": errcheck_message(finding)},
        "locations": [
            location(
                file_name,
                first_int(finding, "line_number", "lineno", "lineNo", "line"),
                first_int(finding, "column", "col"),
            )
        ],
    }


def errcheck_map_results(data: dict[str, Any]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for file_name, findings in data.items():
        if not isinstance(findings, list):
            continue
        for finding in findings:
            if isinstance(finding, dict):
                results.append(errcheck_result(file_name, finding))
    return results


def convert_errcheck_text(text: str) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for raw_line in text.splitlines():
        match = ERRCHECK_LINE_RE.match(raw_line.strip())
        if not match:
            continue
        finding = {
            "line_number": match.group("line"),
            "column": match.group("column"),
            "line_text": match.group("message"),
        }
        results.append(errcheck_result(match.group("file"), finding))
    return results


def convert_errcheck(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, str):
        return convert_errcheck_text(data)

    if isinstance(data, dict):
        mapped_results = errcheck_map_results(data)
        if mapped_results:
            return mapped_results

    results: list[dict[str, Any]] = []
    for finding in flatten_findings(data):
        if not isinstance(finding, dict):
            continue
        file_name = case_get(finding, "file", "filename", "path")
        results.append(errcheck_result(file_name, finding))
    return results


def build_rules(results: list[dict[str, Any]], tool: str) -> list[dict[str, Any]]:
    metadata = TOOL_METADATA[tool]
    rules: dict[str, dict[str, Any]] = {}
    for result in results:
        rule_id = result.get("ruleId") or metadata["defaultRuleId"]
        if rule_id not in rules:
            rules[rule_id] = {
                "id": rule_id,
                "name": rule_id if rule_id != metadata["defaultRuleId"] else metadata["defaultRuleName"],
                "shortDescription": {"text": rule_id},
            }
    if not rules:
        rules[metadata["defaultRuleId"]] = {
            "id": metadata["defaultRuleId"],
            "name": metadata["defaultRuleName"],
            "shortDescription": {"text": metadata["defaultRuleName"]},
        }
    return list(rules.values())


def render(tool: str, input_path: Path, output_path: Path) -> None:
    converters = {
        "staticcheck": convert_staticcheck,
        "errcheck": convert_errcheck,
        "revive": convert_revive,
    }
    if tool not in converters:
        raise ValueError(f"unsupported tool: {tool}")

    if tool == "errcheck":
        data = input_path.read_text(encoding="utf-8") if input_path.exists() else ""
    else:
        data = load_json_or_ndjson(input_path)
    results = converters[tool](data)
    metadata = TOOL_METADATA[tool]
    sarif = {
        "$schema": "https://json.schemastore.org/sarif-2.1.0.json",
        "version": "2.1.0",
        "runs": [
            {
                "tool": {
                    "driver": {
                        "name": metadata["name"],
                        "informationUri": metadata["informationUri"],
                        "rules": build_rules(results, tool),
                    }
                },
                "results": results,
            }
        ],
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(sarif, indent=2), encoding="utf-8")
    print(f"SARIF report written to: {output_path}  ({len(results)} finding(s))")


def main() -> int:
    if len(sys.argv) != 4:
        print(
            f"Usage: {sys.argv[0]} <staticcheck|errcheck|revive> <input-report> <output.sarif>",
            file=sys.stderr,
        )
        return 1
    try:
        render(sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3]))
    except Exception as exc:
        print(f"error: failed to convert {sys.argv[2]} to SARIF: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())



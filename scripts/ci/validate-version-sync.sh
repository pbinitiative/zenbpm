#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
version_file="$repo_root/VERSION"
openapi_file="$repo_root/openapi/api.yaml"

if [ ! -f "$version_file" ]; then
  echo "VERSION file is required" >&2
  exit 1
fi

if [ ! -f "$openapi_file" ]; then
  echo "openapi/api.yaml is required" >&2
  exit 1
fi

version=$(tr -d '\r' < "$version_file")
if [[ ! "$version" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echo "VERSION must contain a v-prefixed release version without a prerelease suffix, for example v1.5.0" >&2
  exit 1
fi
plain_version=${version#v}

if ! openapi_version=$(awk '
  /^info:[[:space:]]*$/ { in_info = 1; next }
  in_info && /^[^[:space:]]/ { in_info = 0 }
  in_info && /^[[:space:]]+version:[[:space:]]*/ {
    value = $0
    sub(/^[[:space:]]+version:[[:space:]]*/, "", value)
    gsub(/^['"'"']|['"'"']$/, "", value)
    print value
    found = 1
    exit
  }
  END { if (!found) exit 1 }
' "$openapi_file"); then
  echo "openapi/api.yaml info.version was not found" >&2
  exit 1
fi

if [ "$openapi_version" != "$plain_version" ]; then
  echo "Version mismatch: VERSION contains $version (OpenAPI comparison value $plain_version), openapi/api.yaml contains $openapi_version" >&2
  exit 1
fi

checked="VERSION and openapi/api.yaml"
release_tag=${RELEASE_TAG:-}
if [ -n "$release_tag" ] && [ "$release_tag" != "dev" ]; then
  if [[ "$release_tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    release_base=$release_tag
  elif [[ "$release_tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+-rc(\.)?[1-9][0-9]*$ ]]; then
    release_base=${release_tag%%-rc*}
  else
    echo "Release tag must be vX.Y.Z, vX.Y.Z-rcN, or vX.Y.Z-rc.N: $release_tag" >&2
    exit 1
  fi
  if [ "$release_base" != "$version" ]; then
    echo "Version mismatch: release tag $release_tag does not match VERSION $version" >&2
    exit 1
  fi
  checked="$checked and release tag $release_tag"
fi

echo "Version $version matches $checked."

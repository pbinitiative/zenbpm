#!/usr/bin/env bash
set -euo pipefail

require_env() {
  local name=$1
  if ! [[ -v $name ]] || [ -z "${!name}" ]; then
    echo "$name is required" >&2
    exit 1
  fi
}

configure_git() {
  git config user.name "zenbpm-release-bot"
  git config user.email "zenbpm-release-bot@users.noreply.github.com"
}

export_release_vars() {
  validate_version_format
  local branch
  branch="release/$(plain_version)"
  RELEASE_BRANCH=$branch
  if [ -n "${GITHUB_ENV:-}" ]; then
    echo "RELEASE_BRANCH=$branch" >> "$GITHUB_ENV"
  fi
  if [ -n "${GITHUB_OUTPUT:-}" ]; then
    echo "release-tag=$VERSION" >> "$GITHUB_OUTPUT"
    echo "release-branch=$branch" >> "$GITHUB_OUTPUT"
  fi
}

plain_version() {
  require_env VERSION
  printf '%s' "${VERSION#v}"
}

github_remote() {
  require_env GH_TOKEN
  require_env ORG
  local repo=$1
  printf 'https://x-access-token:%s@github.com/%s/%s.git' "$GH_TOKEN" "$ORG" "$repo"
}

openapi_version() {
  awk '
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
  ' openapi/api.yaml
}

validate_openapi_version() {
  require_env VERSION
  local actual
  local expected
  expected=$(plain_version)
  if ! actual=$(openapi_version); then
    echo "openapi/api.yaml info.version was not found" >&2
    exit 1
  fi
  if [ "$actual" != "$expected" ]; then
    echo "OpenAPI version mismatch: expected $expected, got $actual" >&2
    exit 1
  fi
}

bump_backend_openapi_version() {
  require_env VERSION
  require_env BACKEND_REPO
  require_env RELEASE_BRANCH
  local version
  version=$(plain_version)

  awk -v new_version="$version" '
    /^info:[[:space:]]*$/ { in_info = 1; print; next }
    in_info && /^[^[:space:]]/ { in_info = 0 }
    in_info && /^[[:space:]]+version:[[:space:]]*/ {
      sub(/version:[[:space:]]*.*/, "version: " new_version)
      updated = 1
    }
    { print }
    END { if (!updated) exit 1 }
  ' openapi/api.yaml > openapi/api.yaml.tmp
  mv openapi/api.yaml.tmp openapi/api.yaml

  configure_git
  if git diff --quiet -- openapi/api.yaml; then
    echo "OpenAPI version is already $version."
    return 0
  fi
  git add openapi/api.yaml
  git commit -m "chore: bump OpenAPI version to $version"
  git push "$(github_remote "$BACKEND_REPO")" HEAD:"$RELEASE_BRANCH"
}

ensure_release_pr() {
  require_env ORG
  require_env BACKEND_REPO
  require_env RELEASE_BRANCH
  require_env VERSION
  local existing_pr

  existing_pr=$(gh pr list \
    --repo "$ORG/$BACKEND_REPO" \
    --head "$RELEASE_BRANCH" \
    --base main \
    --state open \
    --json number \
    --jq '.[0].number // empty')

  if [ -n "$existing_pr" ]; then
    echo "Release PR already exists: #$existing_pr"
    return 0
  fi

  gh pr create \
    --repo "$ORG/$BACKEND_REPO" \
    --head "$RELEASE_BRANCH" \
    --base main \
    --title "chore: release $VERSION" \
    --body "Merge $RELEASE_BRANCH back into main after the release is complete."
}

validate_version_format() {
  require_env VERSION
  if [[ ! "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?$ ]]; then
    echo "Invalid release version '$VERSION'. Expected format vX.Y.Z, for example v2.0.0 or v2.0.0-rc.1" >&2
    exit 1
  fi
}

validate_tags() {
  require_env ORG
  require_env VERSION
  require_env BACKEND_REPO
  require_env FRONTEND_REPO
  require_env JAVA_CLIENT_REPO
  require_env DOCS_REPO
  for repo in "$BACKEND_REPO" "$FRONTEND_REPO" "$JAVA_CLIENT_REPO" "$DOCS_REPO"; do
    if gh api -H "Accept: application/vnd.github+json" "/repos/$ORG/$repo/git/ref/tags/$VERSION" >/dev/null 2>&1; then
      echo "Tag $VERSION already exists in $ORG/$repo" >&2
      exit 1
    fi
  done
}

prepare_branch() {
  local repo=${1:?repo is required}
  require_env RELEASE_BRANCH
  configure_git
  if git ls-remote --exit-code --heads origin "$RELEASE_BRANCH" >/dev/null 2>&1; then
    git checkout "$RELEASE_BRANCH"
    git pull --ff-only origin "$RELEASE_BRANCH"
  else
    git checkout -B "$RELEASE_BRANCH"
  fi
  git push "$(github_remote "$repo")" "$RELEASE_BRANCH"
}

commit_frontend_release_branch() {
  require_env VERSION
  require_env FRONTEND_REPO
  require_env RELEASE_BRANCH
  configure_git
  if git diff --quiet; then
    echo "No frontend OpenAPI/generated changes to commit."
  else
    git add openapi/api.yaml src/base/openapi
    git commit -m "chore: prepare release $VERSION"
  fi
  git push "$(github_remote "$FRONTEND_REPO")" "$RELEASE_BRANCH"
}

create_release_tag() {
  local repo=${1:?repo is required}
  require_env ORG
  require_env VERSION
  require_env RELEASE_BRANCH
  local sha
  sha=$(gh api "/repos/$ORG/$repo/git/ref/heads/${RELEASE_BRANCH}" --jq .object.sha)
  gh api "/repos/$ORG/$repo/git/refs" \
    -f ref="refs/tags/$VERSION" \
    -f sha="$sha"
}

dispatch_frontend_release() {
  require_env ORG
  require_env FRONTEND_REPO
  require_env RELEASE_BRANCH
  require_env VERSION
  gh workflow run release.yaml \
    --repo "$ORG/$FRONTEND_REPO" \
    --ref "$RELEASE_BRANCH" \
    -f version="$VERSION" \
    -f checkout_ref="$VERSION"
}

wait_frontend_release() {
  require_env ORG
  require_env FRONTEND_REPO
  require_env RELEASE_BRANCH
  wait_workflow_run "$FRONTEND_REPO" release.yaml "$RELEASE_BRANCH"
}

dispatch_java_client_release() {
  require_env ORG
  require_env JAVA_CLIENT_REPO
  require_env VERSION
  gh workflow run release.yaml \
    --repo "$ORG/$JAVA_CLIENT_REPO" \
    --ref main \
    -f version="$VERSION"
}

wait_java_client_release() {
  require_env JAVA_CLIENT_REPO
  wait_workflow_run "$JAVA_CLIENT_REPO" release.yaml main
}

dispatch_docs_release() {
  require_env ORG
  require_env DOCS_REPO
  require_env VERSION
  gh workflow run version-docs.yaml \
    --repo "$ORG/$DOCS_REPO" \
    --ref main \
    -f version="$VERSION" \
    -f backend_tag="$VERSION"
}

wait_docs_release() {
  require_env DOCS_REPO
  wait_workflow_run "$DOCS_REPO" version-docs.yaml main
}

wait_workflow_run() {
  require_env ORG
  local repo=${1:?repo is required}
  local workflow=${2:?workflow is required}
  local branch=${3:?branch is required}
  local event=${4:-workflow_dispatch}
  local max_attempts=6
  local attempt=1
  local run_id

  while true; do
    sleep 10
    if run_id=$(gh run list \
      --repo "$ORG/$repo" \
      --workflow "$workflow" \
      --branch "$branch" \
      --event "$event" \
      --json databaseId \
      --jq '.[0].databaseId // empty' 2>/dev/null); then
      if [ -n "$run_id" ]; then
        break
      fi
    fi

    if [ "$attempt" -ge "$max_attempts" ]; then
      echo "Could not find dispatched $workflow workflow run in $ORG/$repo on $branch after $((max_attempts * 10)) seconds" >&2
      exit 1
    fi

    echo "Workflow run not found yet, retrying ($attempt/$max_attempts)..." >&2
    attempt=$((attempt + 1))
  done

  gh run watch "$run_id" --repo "$ORG/$repo" --exit-status
}

notify_discord() {
  if [ -z "${DISCORD_WEBHOOK_URL:-}" ]; then
    echo "DISCORD_WEBHOOK_URL is not configured; skipping notification."
    return 0
  fi

  local content payload
  content="ZenBPM release ${VERSION:-unknown} ${RELEASE_RESULT:-unknown}. Workflow: ${WORKFLOW_URL:-unknown}"
  payload=$(printf '{"content":%s}' "$(printf '%s' "$content" | jq -R .)")
  curl -fsS -H 'Content-Type: application/json' -d "$payload" "$DISCORD_WEBHOOK_URL"
}

case "${1:-}" in
  export-release-vars) export_release_vars ;;
  validate-openapi-version) validate_openapi_version ;;
  bump-backend-openapi-version) bump_backend_openapi_version ;;
  ensure-release-pr) ensure_release_pr ;;
  validate-version-format) validate_version_format ;;
  validate-tags) validate_tags ;;
  prepare-backend-branch) require_env BACKEND_REPO; prepare_branch "$BACKEND_REPO" ;;
  prepare-frontend-branch) require_env FRONTEND_REPO; prepare_branch "$FRONTEND_REPO" ;;
  commit-frontend-release-branch) commit_frontend_release_branch ;;
  tag-backend) require_env BACKEND_REPO; create_release_tag "$BACKEND_REPO" ;;
  tag-frontend) require_env FRONTEND_REPO; create_release_tag "$FRONTEND_REPO" ;;
  dispatch-frontend-release) dispatch_frontend_release ;;
  wait-frontend-release) wait_frontend_release ;;
  dispatch-java-client-release) dispatch_java_client_release ;;
  wait-java-client-release) wait_java_client_release ;;
  dispatch-docs-release) dispatch_docs_release ;;
  wait-docs-release) wait_docs_release ;;
  notify-discord) notify_discord ;;
  *)
    echo "Usage: $0 <command>" >&2
    exit 2
    ;;
esac

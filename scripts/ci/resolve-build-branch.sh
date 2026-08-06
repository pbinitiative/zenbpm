#!/usr/bin/env bash
set -euo pipefail

repo_root=${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}

current_branch=$(git -C "$repo_root" branch --show-current)
if [ -n "$current_branch" ]; then
  printf '%s\n' "$current_branch"
  exit 0
fi

exact_tag=$(git -C "$repo_root" describe --tags --exact-match HEAD 2>/dev/null || true)
if [ -n "$exact_tag" ]; then
  release_branch="release/${exact_tag#v}"
  if git -C "$repo_root" show-ref --verify --quiet "refs/remotes/origin/$release_branch" &&
    git -C "$repo_root" merge-base --is-ancestor HEAD "refs/remotes/origin/$release_branch"; then
    printf '%s\n' "$release_branch"
    exit 0
  fi
fi

default_branch=${REPOSITORY_DEFAULT_BRANCH:-}
if [ -z "$default_branch" ]; then
  origin_head=$(git -C "$repo_root" symbolic-ref --quiet --short refs/remotes/origin/HEAD 2>/dev/null || true)
  default_branch=${origin_head#origin/}
fi
if [ -n "$default_branch" ] &&
  git -C "$repo_root" show-ref --verify --quiet "refs/remotes/origin/$default_branch" &&
  git -C "$repo_root" merge-base --is-ancestor HEAD "refs/remotes/origin/$default_branch"; then
  printf '%s\n' "$default_branch"
  exit 0
fi

head_commit=$(git -C "$repo_root" rev-parse HEAD)
echo "Unable to determine build branch for commit $head_commit: neither a matching release branch nor the repository default branch contains it" >&2
exit 1

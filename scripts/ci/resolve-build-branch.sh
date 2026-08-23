#!/usr/bin/env bash
set -euo pipefail

repo_root=${REPO_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}

current_branch=$(git -C "$repo_root" branch --show-current)
if [ -n "$current_branch" ]; then
  printf '%s\n' "$current_branch"
  exit 0
fi

release_branch=
release_branch_count=0
while IFS= read -r candidate; do
  release_branch=$candidate
  release_branch_count=$((release_branch_count + 1))
done < <(git -C "$repo_root" for-each-ref \
  --contains HEAD \
  --format='%(refname:strip=3)' \
  'refs/remotes/origin/release/*')

if [ "$release_branch_count" -eq 1 ]; then
  printf '%s\n' "$release_branch"
  exit 0
fi
if [ "$release_branch_count" -gt 1 ]; then
  echo "Unable to determine build branch: commit is contained in multiple release branches" >&2
  exit 1
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
echo "Unable to determine build branch for commit $head_commit: neither a release branch nor the repository default branch contains it" >&2
exit 1

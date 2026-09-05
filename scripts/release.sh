#!/usr/bin/env bash
# Lockstep releases: the core module and every contrib module share one
# version, cut from one commit, one tag per module. VERSION at the repo
# root is the single source of truth; every in-tree module's require of
# an in-tree module must equal it (see verify).
#
#   scripts/release.sh verify                  the invariant, as CI runs it
#   scripts/release.sh prepare vX.Y.Z [--dry-run]
#                                              bump VERSION and every in-tree
#                                              require, tidy, verify, then
#                                              branch + commit + push + PR
#   scripts/release.sh tag vX.Y.Z [--dry-run]  at HEAD: check, tag every
#                                              module, push, GitHub release,
#                                              consumer-view verification
#
# Why two phases: the require lines must name the version before the tag
# exists, and go.sum stays free of in-tree hashes only because every
# in-tree dependency is replaced by a local path. So a prepare commit can
# say "require durable v0.4.0" before v0.4.0 is tagged, and every module's
# tag lands on that one commit.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
[[ -f VERSION && -d contrib ]] || { echo "release: not at the repository root: $PWD" >&2; exit 1; }

CORE=github.com/dangra/durable
SEMVER='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?$'

die() { echo "release: $*" >&2; exit 1; }
note() { echo "release: $*" >&2; }

# In-tree modules besides the root, and the subset that gets tagged.
modules() { find contrib examples -name go.mod | sort | xargs -n1 dirname; }
tagged_modules() { find contrib -name go.mod | sort | xargs -n1 dirname; }
module_path() { awk '$1 == "module" { print $2; exit }' "$1/go.mod"; }

# require_version DIR MODPATH: the version DIR's go.mod requires MODPATH
# at, or empty. Handles both block and single-line require forms.
require_version() {
	awk -v m="$2" '
		$1 == "require" && $2 == m { print $3; exit }
		$1 == m && $2 ~ /^v/     { print $2; exit }' "$1/go.mod"
}

# set_require DIR MODPATH VERSION rewrites the require line, in both the
# block and single-line forms, keeping indentation and any trailing
# comment. Portable: no sed -i.
set_require() {
	local tmp; tmp=$(mktemp)
	awk -v m="$2" -v v="$3" '
		($1 == "require" && $2 == m) || ($1 == m && $2 ~ /^v/) { sub(/ v[^ \t]+/, " " v) }
		{ print }' "$1/go.mod" >"$tmp" && mv "$tmp" "$1/go.mod"
}

has_replace() { grep -Eq "^replace[[:space:]]+$2[[:space:]]+=>" "$1/go.mod"; }

# semver_lt A B: A sorts before B under semver (a pre-release sorts before
# its release; pre-release identifiers compare with sort -V).
semver_lt() {
	local a=${1#v} b=${2#v} ac bc ap bp
	ac=${a%%-*}; bc=${b%%-*}
	ap=""; bp=""
	[[ $a == *-* ]] && ap=${a#*-}
	[[ $b == *-* ]] && bp=${b#*-}
	local a1 a2 a3 b1 b2 b3
	IFS=. read -r a1 a2 a3 <<<"$ac"
	IFS=. read -r b1 b2 b3 <<<"$bc"
	if ((a1 != b1)); then ((a1 < b1)); return; fi
	if ((a2 != b2)); then ((a2 < b2)); return; fi
	if ((a3 != b3)); then ((a3 < b3)); return; fi
	[[ $ap == "$bp" ]] && return 1
	[[ -z $ap ]] && return 1
	[[ -z $bp ]] && return 0
	[[ $(printf '%s\n%s\n' "$ap" "$bp" | sort -V | head -1) == "$ap" ]]
}

cmd_verify() {
	[[ -f VERSION ]] || die "VERSION file missing"
	local v; v=$(tr -d '[:space:]' <VERSION)
	[[ $v =~ $SEMVER ]] || die "VERSION $v is not a semver tag (vX.Y.Z[-pre])"
	local ok=1
	if grep -Eq "^[[:space:]]*$CORE/" go.mod; then
		note "root go.mod must not require an in-tree module"; ok=0
	fi
	local dir mod path got
	for dir in $(modules); do
		got=$(require_version "$dir" "$CORE")
		if [[ $got != "$v" ]]; then
			note "$dir requires $CORE $got, VERSION is $v"; ok=0
		fi
		if ! has_replace "$dir" "$CORE"; then
			note "$dir lacks 'replace $CORE => <local path>': in-tree builds would use the released core"; ok=0
		fi
		for mod in $(tagged_modules); do
			path=$(module_path "$mod")
			[[ $path == "$(module_path "$dir")" ]] && continue
			got=$(require_version "$dir" "$path")
			[[ -z $got ]] && continue
			if [[ $got != "$v" ]]; then
				note "$dir requires $path $got, VERSION is $v"; ok=0
			fi
			if ! has_replace "$dir" "$path"; then
				note "$dir lacks 'replace $path => <local path>'"; ok=0
			fi
		done
	done
	((ok)) || die "verify failed"
	note "verify ok: VERSION $v; tagged modules: $CORE $(for m in $(tagged_modules); do module_path "$m"; done | tr '\n' ' ')"
}

cmd_prepare() {
	local v=${1:-}; shift || true
	local dry=0; [[ ${1:-} == --dry-run ]] && dry=1
	[[ $v =~ $SEMVER ]] || die "usage: release.sh prepare vX.Y.Z[-pre] [--dry-run]"
	local cur; cur=$(tr -d '[:space:]' <VERSION)
	semver_lt "$cur" "$v" || die "$v is not after the current VERSION $cur"
	if git rev-parse -q --verify "refs/tags/$v" >/dev/null || git ls-remote --tags origin "refs/tags/$v" | grep -q .; then
		die "tag $v already exists"
	fi
	((dry)) || [[ -z $(git status --porcelain) ]] || die "working tree not clean"

	printf '%s\n' "$v" >VERSION
	local dir mod
	for dir in $(modules); do
		set_require "$dir" "$CORE" "$v"
		for mod in $(tagged_modules); do
			[[ -n $(require_version "$dir" "$(module_path "$mod")") ]] && set_require "$dir" "$(module_path "$mod")" "$v"
		done
		(cd "$dir" && go mod tidy && go build ./... && go vet ./...)
	done
	cmd_verify
	if ((dry)); then
		note "dry run: files updated, nothing committed"
		git --no-pager diff --stat
		return
	fi
	local branch="release/$v"
	git checkout -q -b "$branch"
	local files=(VERSION)
	for dir in $(modules); do
		files+=("$dir/go.mod")
		[[ -f $dir/go.sum ]] && files+=("$dir/go.sum")
	done
	git add "${files[@]}"
	git commit -q -m "Release $v" -m "Lockstep bump of VERSION and every in-tree require. Tags are cut from the merge commit by the tag-release workflow."
	git push -q -u origin "$branch"
	gh pr create --title "Release $v" --body "$(printf 'Bumps VERSION and every in-tree require to %s.\n\nAfter merge, run the **tag-release** workflow with version `%s` to tag every module at the merge commit, publish the GitHub release, and verify the modules resolve for consumers.' "$v" "$v")"
}

# ci_green SHA: every completed GitHub Actions check run on SHA succeeded
# (or was skipped), excluding this workflow's own run, and at least one
# exists.
ci_green() {
	local repo; repo=${GITHUB_REPOSITORY:-$(gh repo view --json nameWithOwner --jq .nameWithOwner)}
	local runs
	runs=$(gh api --paginate "repos/$repo/commits/$1/check-runs?per_page=100" \
		--jq '.check_runs[] | select(.name != "Tag release") | "\(.status)\t\(.conclusion)\t\(.name)"')
	[[ -n $runs ]] || { note "no check runs found for $1"; return 1; }
	local bad
	bad=$(printf '%s\n' "$runs" | awk -F'\t' '$1 != "completed" || ($2 != "success" && $2 != "skipped" && $2 != "neutral")')
	if [[ -n $bad ]]; then
		note "check runs not green for $1:"; printf '%s\n' "$bad" >&2
		return 1
	fi
	note "ci green for $1: $(printf '%s\n' "$runs" | wc -l) check runs"
}

# consumer_check VERSION: from a scratch module with no replace directives,
# fetch every tagged module at VERSION straight from git and build an
# importer — the only proof that the require lines resolve for consumers.
consumer_check() {
	local v=$1 d; d=$(mktemp -d)
	local paths; paths="$CORE $(for m in $(tagged_modules); do module_path "$m"; done)"
	# The subshell's status is captured explicitly: a caller testing this
	# function in an if-condition suspends errexit, so a failure inside
	# would otherwise fall through to success. Cleanup runs either way.
	# Modules are pinned with go mod edit, not go get: go get treats the
	# path as a package pattern and, when the version is missing for the
	# nested module, quietly resolves it at its own latest instead.
	# Tidy fails on an unknown revision, and the final go list asserts
	# that every module resolved to exactly the version.
	local rc=0
	(
		set -e
		cd "$d"
		export GOPROXY=direct GONOSUMDB="$CORE" GOFLAGS=-mod=mod
		go mod init probe.invalid/probe >/dev/null 2>&1
		for p in $paths; do go mod edit -require="$p@$v"; done
		{
			echo 'package main'
			for p in $paths; do echo "import _ \"$p\""; done
			echo 'func main() {}'
		} >main.go
		go mod tidy
		go build ./...
		for p in $paths; do
			got=$(go list -m -f '{{.Version}}' "$p")
			[[ $got == "$v" ]] || { echo "release: $p resolved to $got, not $v" >&2; exit 1; }
		done
	) || rc=$?
	rm -rf "$d"
	((rc == 0)) || return "$rc"
	note "consumer check ok: $paths at $v"
}

cmd_tag() {
	local v=${1:-}; shift || true
	local dry=0; [[ ${1:-} == --dry-run ]] && dry=1
	[[ $v =~ $SEMVER ]] || die "usage: release.sh tag vX.Y.Z[-pre] [--dry-run]"
	local sha; sha=$(git rev-parse HEAD)
	local cur; cur=$(tr -d '[:space:]' <VERSION)
	[[ $cur == "$v" ]] || die "HEAD declares VERSION $cur, not $v: merge the release PR first (release.sh prepare)"
	cmd_verify
	local tags=("$v")
	local mod
	for mod in $(tagged_modules); do tags+=("$mod/$v"); done
	local t
	for t in "${tags[@]}"; do
		git rev-parse -q --verify "refs/tags/$t" >/dev/null && die "tag $t already exists locally"
		git ls-remote --tags origin "refs/tags/$t" | grep -q . && die "tag $t already exists on origin"
	done
	ci_green "$sha" || die "refusing to tag a commit whose CI is not green"
	if ((dry)); then
		note "dry run: would tag $sha as ${tags[*]}"
		return
	fi
	for t in "${tags[@]}"; do
		git tag -a "$t" -m "$t" "$sha"
	done
	git push --atomic origin "${tags[@]/#/refs/tags/}"
	note "pushed tags: ${tags[*]}"

	local repo; repo=${GITHUB_REPOSITORY:-$(gh repo view --json nameWithOwner --jq .nameWithOwner)}
	local notes; notes=$(mktemp)
	{
		echo "## Modules"
		echo
		echo "- \`$CORE@$v\`"
		for mod in $(tagged_modules); do echo "- \`$(module_path "$mod")@$v\`"; done
		echo
		gh api "repos/$repo/releases/generate-notes" -f tag_name="$v" -f target_commitish="$sha" --jq .body
	} >"$notes"
	local pre=(); [[ $v == *-* ]] && pre=(--prerelease)
	gh release create "$v" --title "$v" --target "$sha" --notes-file "$notes" "${pre[@]}"
	rm -f "$notes"
	note "created release $v"

	if ! consumer_check "$v"; then
		cat >&2 <<EOF
release: consumers cannot build the modules at $v. The tags and release
exist; decide whether to withdraw them:
  gh release delete $v --yes
  git push --delete origin ${tags[*]}
EOF
		exit 1
	fi
}

# Sourcing the script loads the functions without running a command, for
# tests.
[[ ${BASH_SOURCE[0]} == "$0" ]] || return 0

case ${1:-} in
verify) cmd_verify ;;
prepare) shift; cmd_prepare "$@" ;;
tag) shift; cmd_tag "$@" ;;
*) sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//' >&2; exit 2 ;;
esac

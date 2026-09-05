# Releasing

durable is one repository holding several Go modules: the core
`github.com/dangra/durable`, the contrib modules under `contrib/`, and
the examples. Releases are **lockstep**: the core and every contrib
module share one version, cut from one commit, one tag per module
(`v0.4.0`, `contrib/durableotel/v0.4.0`). The examples are bumped but
never tagged.

The shape follows opentelemetry-go's: a *prepare* commit that names the
version, reviewed and tested as a normal PR, then tags at the merged
commit. It works because every in-tree dependency is `replace`d by a
local path, so `go.sum` never holds a hash for an in-tree module and the
prepare commit can require a version that is not tagged yet. Keep the
`replace` lines; do not switch the tree to `go.work` — `go mod tidy`
ignores workspaces and would demand the hash of a tag that does not
exist yet.

`VERSION` at the root is the single source of truth. CI's "Release
versions" job runs `scripts/release.sh verify` on every push: every
in-tree module's `require` of an in-tree module must equal `VERSION`,
and must be accompanied by a `replace`. The root module must not require
an in-tree module.

## Cutting a release

1. **Prepare**, locally, from a clean checkout of `master`:

   ```sh
   scripts/release.sh prepare v0.4.0
   ```

   This bumps `VERSION` and every in-tree `require` to `v0.4.0`, tidies
   and builds each module, runs `verify`, and opens the PR
   "Release v0.4.0" from branch `release/v0.4.0`. Add `--dry-run` to
   stop after editing the files. It runs under your identity so the PR
   gets full CI — a PR opened by a workflow token would not.

2. **Merge** the PR like any other.

3. **Tag**: run the *Tag release* workflow (Actions → Tag release → Run
   workflow) with `version = v0.4.0`. At `master` HEAD it checks that
   `VERSION` and the require lines equal the version, that no tag exists
   yet, and that every CI check on the commit is green — waiting, up to
   twenty minutes, for the run a squash merge starts on the merge
   commit, so you can press the button right after merging; then it creates
   annotated tags for the core and every contrib module, pushes them
   atomically, publishes the GitHub release with generated notes and a
   module list, and finally fetches every tagged module at the version
   from a scratch module with no `replace` directives and builds an
   importer — the proof that consumers can build. `dry_run` runs every
   check and creates nothing.

Pre-releases (`v0.4.0-rc.1`) work the same way and are marked as such
on GitHub. `v0.4.0-rc.1` was the first cut made with this procedure,
as a rehearsal: prepare, merge, a `dry_run`, then the real run, whose
consumer check passed.

## When the consumer check fails

The tags and the release already exist. The workflow prints the two
commands that withdraw them (`gh release delete`, `git push --delete
origin <tags>`); withdraw, fix on `master`, and cut the next patch
version — do not retag a version that was ever visible, module proxies
may have cached it.

## Adding a contrib module

Give it a `go.mod` under `contrib/<name>/` with
`replace github.com/dangra/durable => ../..` and a `require` of the core
at `VERSION`. `verify`, `prepare`, and `tag` discover contrib modules by
walking `contrib/`; nothing else to register.

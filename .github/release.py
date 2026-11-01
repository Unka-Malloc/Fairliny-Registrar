#!/usr/bin/env python3
"""Generated release train helper; regenerate it instead of editing this copy.
.github/release.lock records the source inputs that produced it.

Every subcommand runs inside the repository checkout, writes step outputs to
$GITHUB_OUTPUT and a readable report to $GITHUB_STEP_SUMMARY. RELEASE_KIND
says what started the run: `train` (the weekly schedule) or `manual` (Run
workflow).
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import time
import tomllib
import urllib.error
import urllib.request

WEEKDAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")
LEVELS = ("patch", "minor", "major")
SAFE_REF = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/-]*")
VERSION = re.compile(r"(\d+)\.(\d+)\.(\d+)")
DEP_KEYS = ("dependencies", "dev-dependencies", "build-dependencies")
POLL_SECONDS = int(os.environ.get("TRAIN_POLL_SECONDS", "300"))
# The sparse-index base. CARGO_REGISTRY_INDEX overrides it (a mirror or a
# fixture); publish.yml's shell probe shares the same default — keep them
# in sync.
INDEX = (os.environ.get("CARGO_REGISTRY_INDEX") or "https://index.crates.io").rstrip("/")


class TrainError(Exception):
    pass


def git(*args: str, cwd: pathlib.Path | None = None) -> str:
    done = subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True)
    if done.returncode != 0:
        raise TrainError(f"git {' '.join(args)}: {done.stderr.strip()}")
    return done.stdout.strip()


def git_ok(*args: str) -> bool:
    return subprocess.run(["git", *args], capture_output=True).returncode == 0


def emit(**values: str) -> None:
    lines = [f"{key}={value}" for key, value in values.items()]
    target = os.environ.get("GITHUB_OUTPUT")
    if target:
        with open(target, "a", encoding="utf-8") as handle:
            handle.write("\n".join(lines) + "\n")
    else:
        print("\n".join(lines))


def report(markdown: str) -> None:
    target = os.environ.get("GITHUB_STEP_SUMMARY")
    if target:
        with open(target, "a", encoding="utf-8") as handle:
            handle.write(markdown.rstrip() + "\n\n")
    else:
        print(markdown, file=sys.stderr)


def parse_version(text: str) -> tuple[int, int, int] | None:
    match = VERSION.fullmatch(text)
    return tuple(int(part) for part in match.groups()) if match else None


def show(version: tuple[int, int, int] | None) -> str:
    return ".".join(map(str, version)) if version else "none"


def bump(version: tuple[int, int, int], level: str) -> tuple[int, int, int]:
    major, minor, patch = version
    if level == "major":
        return (major + 1, 0, 0)
    if level == "minor":
        return (major, minor + 1, 0)
    return (major, minor, patch + 1)


def parse_offset(text: str) -> dt.timezone:
    match = re.fullmatch(r"([+-])(\d{2}):(\d{2})", text)
    if not match:
        raise TrainError(f"invalid UTC offset {text!r}")
    delta = dt.timedelta(hours=int(match[2]), minutes=int(match[3]))
    return dt.timezone(-delta if match[1] == "-" else delta)


def last_cutoff(now: dt.datetime, weekday: str, hhmm: str, offset: str) -> dt.datetime:
    """Latest cutoff instant at or before `now`; `hhmm` is the last included minute."""
    zone = parse_offset(offset)
    hour, minute = (int(part) for part in hhmm.split(":"))
    local = now.astimezone(zone)
    back = (local.weekday() - WEEKDAYS.index(weekday)) % 7
    day = local.date() - dt.timedelta(days=back)
    cutoff = dt.datetime.combine(day, dt.time(hour, minute), zone) + dt.timedelta(minutes=1)
    if cutoff > now:
        cutoff -= dt.timedelta(days=7)
    return cutoff


def index_path(name: str) -> str:
    name = name.lower()
    if len(name) <= 2:
        return f"{len(name)}/{name}"
    if len(name) == 3:
        return f"3/{name[0]}/{name}"
    return f"{name[:2]}/{name[2:4]}/{name}"


def published_versions(name: str) -> set[tuple[int, int, int]]:
    """Every version the registry holds, yanked included: a yanked number is still taken."""
    request = urllib.request.Request(
        f"{INDEX}/{index_path(name)}", headers={"User-Agent": "release-train"}
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read().decode()
    except urllib.error.HTTPError as error:
        if error.code in (403, 404):
            return set()
        raise
    found = set()
    for line in body.splitlines():
        if line.strip():
            version = parse_version(json.loads(line)["vers"])
            if version:
                found.add(version)
    return found


def dependency_specs(manifest: dict):
    tables = [manifest, manifest.get("workspace", {})]
    tables += list(manifest.get("target", {}).values())
    for table in tables:
        for key in DEP_KEYS:
            yield from table.get(key, {}).items()


def sibling_requirements(repo: pathlib.Path, siblings: set[str]) -> dict[str, set[str]]:
    """Version requirements this repository places on crates of sibling checkouts."""
    parent = repo.resolve().parent
    wanted: dict[str, set[str]] = {}
    for relative in git("ls-files", "*Cargo.toml", cwd=repo).splitlines():
        manifest_path = repo / relative
        if manifest_path.name != "Cargo.toml":
            continue
        manifest = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
        for name, spec in dependency_specs(manifest):
            if not isinstance(spec, dict) or "path" not in spec:
                continue
            target = (manifest_path.parent / spec["path"]).resolve()
            try:
                top = target.relative_to(parent).parts[0]
            except ValueError:
                top = None
            if top == repo.resolve().name:
                continue
            if top not in siblings:
                raise TrainError(
                    f"{relative}: `{name}` points at {spec['path']}, which is not a declared "
                    "sibling; regenerate this workflow"
                )
            wanted.setdefault(spec.get("package", name), set()).add(spec.get("version", "*"))
    return wanted


def resolve_published(wanted: dict[str, set[str]]):
    """Resolve the requirements against crates.io; (versions, None) or (None, reason)."""
    with tempfile.TemporaryDirectory() as scratch:
        root = pathlib.Path(scratch)
        (root / "src").mkdir()
        (root / "src" / "lib.rs").write_text("", encoding="utf-8")
        lines = [
            "[package]",
            'name = "release-train-gate"',
            'version = "0.0.0"',
            'edition = "2021"',
            "publish = false",
            "",
            "[dependencies]",
        ]
        for position, (package, requirements) in enumerate(sorted(wanted.items())):
            requirement = ", ".join(sorted(requirements))
            lines.append(
                f"gate{position} = {{ package = {json.dumps(package)}, "
                f"version = {json.dumps(requirement)} }}"
            )
        (root / "Cargo.toml").write_text("\n".join(lines) + "\n", encoding="utf-8")
        done = subprocess.run(
            ["cargo", "generate-lockfile", "--manifest-path", str(root / "Cargo.toml")],
            capture_output=True,
            text=True,
        )
        if done.returncode != 0:
            return None, done.stderr.strip().splitlines()[-1:] or ["cargo failed"]
        lock = tomllib.loads((root / "Cargo.lock").read_text(encoding="utf-8"))
    resolved: dict[str, tuple[int, int, int]] = {}
    for package in lock.get("package", []):
        version = parse_version(package["version"])
        if version and version > resolved.get(package["name"], (-1, -1, -1)):
            resolved[package["name"]] = version
    return resolved, None


def remote_tags(slug: str) -> dict[tuple[int, int, int], str]:
    listing = git("ls-remote", "--tags", "--refs", f"https://github.com/{slug}.git")
    tags = {}
    for line in listing.splitlines():
        name = line.split("refs/tags/", 1)[-1]
        version = parse_version(name[1:]) if name.startswith("v") else None
        if version:
            tags[version] = name
    return tags


def cmd_candidate(args: argparse.Namespace) -> int:
    mode = os.environ.get("RELEASE_MODE", "dry-run")
    kind = os.environ.get("RELEASE_KIND", "train")
    # The publish gate binds the candidate to the default-branch run head,
    # so selection must live on that same branch: a side line would pick
    # candidates the gate is bound to reject, and the release's back-merge
    # lands on this branch too.
    branch = args.default_branch
    cutoff = ""
    if args.override:
        if not SAFE_REF.fullmatch(args.override):
            raise TrainError(f"refusing candidate {args.override!r}")
        remote = f"refs/remotes/origin/{args.override}"
        ref = remote if git_ok("rev-parse", "--verify", "-q", remote) else args.override
        sha = git("rev-parse", "--verify", f"{ref}^{{commit}}")
        if not git_ok("merge-base", "--is-ancestor", sha, f"refs/remotes/origin/{branch}"):
            raise TrainError(f"{args.override} is not on {branch}")
        source = f"requested candidate {args.override}"
    elif kind == "manual":
        sha = git("rev-parse", "--verify", f"refs/remotes/origin/{branch}^{{commit}}")
        source = f"head of {branch}"
    else:
        moment = last_cutoff(dt.datetime.now(dt.timezone.utc), args.weekday, args.time,
                             args.utc_offset)
        cutoff = moment.isoformat()
        sha = git("rev-list", "-1", "--first-parent", f"--before={cutoff}",
                  f"refs/remotes/origin/{branch}")
        if not sha:
            raise TrainError(f"{branch} has no commit before {cutoff}")
        source = f"last commit on {branch} before {cutoff}"
    git("checkout", "-q", "--detach", sha)
    report(f"### Candidate\n\n`{sha[:12]}`: {source} ({kind}, {mode}).")
    emit(sha=sha, branch=branch, cutoff=cutoff)
    return 0

def cmd_deps(args: argparse.Namespace) -> int:
    mode = os.environ.get("RELEASE_MODE", "dry-run")
    siblings = json.loads(os.environ.get("SIBLINGS") or "[]")
    if not siblings:
        emit(ready="true")
        return 0
    wanted = sibling_requirements(pathlib.Path.cwd(), {s["dir"] for s in siblings})
    deadline = time.monotonic() + args.wait_minutes * 60
    while True:
        resolved, problem = resolve_published(wanted)
        if resolved is not None:
            break
        if time.monotonic() >= deadline:
            lines = "\n".join(f"- `{p}` {', '.join(sorted(r))}" for p, r in sorted(wanted.items()))
            verdict = "would wait for" if mode == "dry-run" else "is still waiting for"
            report(f"### Dependencies\n\nThe train {verdict} unpublished dependencies:\n\n"
                   f"{lines}\n\ncargo: {' '.join(problem)}")
            emit(ready="false")
            return 0 if mode == "dry-run" else 1
        print(f"dependencies not on crates.io yet: {' '.join(problem)}", flush=True)
        time.sleep(POLL_SECONDS)
    rows, refs = [], {}
    # Refs are keyed by sibling directory, not position: a reordered
    # declaration can never pair a checkout with the wrong resolved tag.
    for sibling in siblings:
        tags = remote_tags(sibling["slug"])
        versions = sorted(resolved[c] for c in sibling["crates"] if c in resolved)
        if versions:
            if versions[-1] not in tags:
                raise TrainError(f"{sibling['slug']} published {show(versions[-1])} without a "
                                 f"v{show(versions[-1])} tag")
            ref = tags[versions[-1]]
        elif tags:
            ref = tags[max(tags)]
        else:
            raise TrainError(f"{sibling['slug']} has no release tag to build against")
        refs[f"ref_{sibling['dir']}"] = ref
        rows.append(f"| {sibling['dir']} | `{ref}` |")
    report("### Dependencies\n\n| Sibling | Checked out |\n| --- | --- |\n" + "\n".join(rows))
    emit(ready="true", **refs)
    return 0


def labelled_level(slug: str, branch: str, candidate: str, tag: str) -> tuple[str, list[str]]:
    since = git("log", "-1", "--format=%cs", tag)
    # GitHub search returns at most 1000 rows for any query; there is no
    # pagination past that ceiling. A full page therefore means the merged-PR
    # set may be truncated — and a truncated set can silently drop the
    # release:minor/major labels that decide the bump. Fail closed rather
    # than default to a lower bump: releasing manually with an explicit
    # level is the safe path when the interval overflows.
    listing = subprocess.run(
        ["gh", "pr", "list", "--repo", slug, "--base", branch, "--state", "merged",
         "--search", f"merged:>={since}", "--limit", "1000",
         "--json", "number,labels,mergeCommit"],
        capture_output=True, text=True,
    )
    if listing.returncode != 0:
        raise TrainError(f"listing pull requests failed: {listing.stderr.strip()}")
    pulls = json.loads(listing.stdout)
    if len(pulls) >= 1000:
        raise TrainError(
            f"merged pull requests into {branch} since {since} reached the "
            "1000-row search ceiling; the label set may be truncated. Release "
            "manually with an explicit level — do not guess a lower bump.")
    level, numbers = "patch", []
    for pull in pulls:
        merged = (pull.get("mergeCommit") or {}).get("oid")
        if not merged or not git_ok("merge-base", "--is-ancestor", merged, candidate):
            continue
        if git_ok("merge-base", "--is-ancestor", merged, tag):
            continue
        numbers.append(f"#{pull['number']}")
        for label in pull.get("labels", []):
            name = label.get("name", "")
            if name.startswith("release:") and name[8:] in LEVELS:
                level = max(level, name[8:], key=LEVELS.index)
    return level, numbers


def choose_version(
    manifest: tuple[int, int, int],
    published: dict[str, set[tuple[int, int, int]]],
    tags: set[tuple[int, int, int]],
    level: str,
    requested: tuple[int, int, int] | None = None,
) -> tuple[tuple[int, int, int], bool]:
    """The version to release and whether it resumes an interrupted upload.

    `requested` is a version a person asked for; it must still be a number
    nobody has taken, unless it is the one an interrupted upload left behind.
    """
    on_registry = set().union(*published.values())
    top_published = max(on_registry, default=None)
    top_tag = max(tags, default=None)
    untagged = top_published is not None and (top_tag is None or top_published > top_tag)
    if untagged:
        holders = sum(top_published in versions for versions in published.values())
        if holders < len(published):
            if requested not in (None, top_published):
                raise TrainError(f"{show(top_published)} is only partly on crates.io; finish it "
                                 f"before releasing {show(requested)}")
            return top_published, True
        if top_tag is not None:
            raise TrainError(
                f"{show(top_published)} is on crates.io without a v{show(top_published)} tag; "
                "tag the commit it was published from before the next train"
            )
    base = max([*tags, *on_registry], default=None)
    if requested is not None:
        if base is not None and requested <= base:
            raise TrainError(f"{show(requested)} is not above {show(base)}, the highest number "
                             "already taken")
        return requested, False
    return (manifest if base is None else max(bump(base, level), manifest)), False

def cargo_metadata(repo: pathlib.Path) -> dict:
    """Workspace manifest data only: cargo runs from an empty directory with
    --manifest-path so a checkout's .cargo/config.toml — a credential
    provider is executable — can never load beside the release identity.
    --no-deps suffices: member manifests carry their own dependencies."""
    with tempfile.TemporaryDirectory() as scratch:
        try:
            done = subprocess.run(
                ["cargo", "metadata", "--format-version", "1", "--no-deps",
                 "--manifest-path", str(repo / "Cargo.toml")],
                cwd=scratch, capture_output=True, text=True, check=True,
            )
        except subprocess.CalledProcessError as error:
            tail = (error.stderr or "").strip().splitlines()
            raise TrainError(
                f"cargo metadata failed: {tail[-1] if tail else 'unknown error'}")
    return json.loads(done.stdout)


def cmd_version(args: argparse.Namespace) -> int:
    slug = os.environ.get("GITHUB_REPOSITORY", "")
    kind = os.environ.get("RELEASE_KIND", "train")
    metadata = cargo_metadata(pathlib.Path.cwd())
    members = set(metadata["workspace_members"])
    packages = [p for p in metadata["packages"] if p["id"] in members]
    publishable = [p for p in packages if p["publish"] is None or "crates-io" in p["publish"]]
    if not publishable:
        raise TrainError("the workspace has no publishable package")
    manifest = max(parse_version(p["version"]) or (0, 0, 0) for p in publishable)
    published = {p["name"]: published_versions(p["name"]) for p in publishable}
    on_registry = set().union(*published.values())
    tags = {}
    for name in git("tag", "--list", "v*").splitlines():
        version = parse_version(name[1:])
        if version:
            tags[version] = name
    released = [v for v in tags if v in on_registry]
    last_tag = tags[max(released)] if released else None
    notes, pulls = [], []
    if last_tag:
        changed = not git_ok("diff", "--quiet", last_tag, args.candidate)
    else:
        changed = True
        notes.append("No tag matches a published version, so the whole history counts as new.")
    if tags and max(tags) not in on_registry:
        notes.append(f"`{tags[max(tags)]}` is tagged but not on crates.io; its number is "
                     "skipped.")
    requested = None
    if kind == "manual":
        level = os.environ.get("INPUT_LEVEL") or "patch"
        if level not in LEVELS:
            raise TrainError(f"unknown level {level!r}")
        text = (os.environ.get("INPUT_VERSION") or "").strip().removeprefix("v")
        if text:
            requested = parse_version(text)
            if requested is None:
                raise TrainError(f"version {text!r} is not <major>.<minor>.<patch>")
            level = "exact"
    else:
        level = "patch"
        if changed and last_tag and args.branch:
            level, pulls = labelled_level(slug, args.branch, args.candidate, last_tag)
    upcoming, resume = choose_version(manifest, published, set(tags), level, requested)
    if resume:
        changed = True
        notes.append(f"{show(upcoming)} is only partly on crates.io; this run finishes it.")
    done = sorted(name for name, versions in published.items() if upcoming in versions)
    fresh = sorted(name for name, versions in published.items() if not versions)
    if fresh:
        notes.append("Never published, so Trusted Publishing cannot upload them until a first "
                     "manual upload: " + ", ".join(f"`{n}`" for n in fresh) + ".")
    release = changed and len(done) < len(publishable)
    if changed and not release:
        notes.append(f"Every crate already holds {show(upcoming)} on crates.io.")
    excludes = sorted({p["name"] for p in packages if p not in publishable} | set(done))
    needs_set = any(parse_version(p["version"]) != upcoming for p in publishable)
    source = {"train": "pull request labels", "manual": "Run workflow input"}
    summary = [
        "### Version",
        "",
        "| Item | Value |",
        "| --- | --- |",
        f"| Last released tag | {last_tag or 'none'} |",
        f"| Manifest version | {show(manifest)} |",
        f"| Changed since release | {'yes' if changed else 'no'} |",
        f"| Bump level | {level} from {source.get(kind, kind)} "
        f"({', '.join(pulls) or 'no labelled pull requests' if kind == 'train' else '-'}) |",
        f"| Next version | {show(upcoming) if release else 'no release'} |",
    ]
    report("\n".join(summary + ([""] + [f"- {n}" for n in notes] if notes else [])))
    emit(
        release="true" if release else "false",
        next=show(upcoming),
        needs_set="true" if needs_set else "false",
        excludes=" ".join(excludes),
    )
    return 0

def publishable_members() -> dict[str, dict]:
    metadata = cargo_metadata(pathlib.Path.cwd())
    members = set(metadata["workspace_members"])
    packages = {p["name"]: p for p in metadata["packages"] if p["id"] in members}
    return {n: p for n, p in packages.items()
            if p["publish"] is None or "crates-io" in p["publish"]}


def cmd_order(args: argparse.Namespace) -> int:
    packages = publishable_members()
    excludes = set((os.environ.get("EXCLUDES") or "").split())
    targets = sorted(set(packages) - excludes)
    ordered, pending = [], dict.fromkeys(targets)
    while pending:
        ready = [n for n in pending
                 if not any(d["name"] in pending
                            for d in packages[n]["dependencies"] if d["kind"] != "dev")]
        if not ready:
            raise TrainError(f"dependency cycle among {', '.join(sorted(pending))}")
        ordered.extend(sorted(ready))
        for name in ready:
            del pending[name]
    for name in ordered:
        p = packages[name]
        print(f"{name}\t{p['version']}")
    return 0


def cmd_freshness(args: argparse.Namespace) -> int:
    """Every publishable crate's src/**/*.rs `last_verified: "YYYY-MM-DD"`
    must sit inside the window — a stale native-capability table means the
    card's claims were not re-checked against the shipped binary."""
    window = dt.timedelta(days=args.window_days)
    today = dt.datetime.now(dt.timezone.utc).date()
    checked, stale = 0, []
    for name, package in sorted(publishable_members().items()):
        src = pathlib.Path(package["manifest_path"]).parent / "src"
        dates = [
            dt.date.fromisoformat(match.group(1))
            for path in src.rglob("*.rs") if src.exists()
            for match in [re.search(r'last_verified:\s*"(\d{4}-\d{2}-\d{2})"',
                                    path.read_text())]
            if match
        ]
        if not dates:
            continue
        checked += 1
        oldest = min(dates)
        if today - oldest > window:
            stale.append(f"{name}: last_verified {oldest} "
                         f"({(today - oldest).days}d > {args.window_days}d)")
    if stale:
        report("\n".join(f"- {entry}" for entry in stale))
        raise TrainError(f"{len(stale)} native capability table(s) stale: "
                         + "; ".join(stale))
    emit(checked=str(checked))
    report(f"{checked} native capability table(s) within "
           f"{args.window_days}d.")
    return 0


SECTION = re.compile(r"^\[\[?\s*(?P<name>.+?)\s*\]?\]\s*(?:#.*)?$")
ENTRY = re.compile(
    r"^(?P<key>'[^']+'|\"[^\"]+\"|[A-Za-z0-9_-]+)"
    r"(?:\.(?P<sub>[A-Za-z0-9_-]+))?\s*=\s*(?P<value>.*)$"
)
VERSION_FIELD = re.compile(r'version\s*=\s*"[^"]*"')
WORKSPACE_FIELD = re.compile(r"\bworkspace\s*=\s*true\b")
PACKAGE_FIELD = re.compile(r'package\s*=\s*"([^"]+)"')


def section_of(line: str) -> str | None:
    """The table name a `[a.b]`/`[[a.b]]` header opens, or None."""
    text = line.strip()
    if not text.startswith("["):
        return None
    found = SECTION.match(text)
    if not found:
        return None
    return found.group("name").rstrip("]").strip()


def brace_depth(text: str) -> int:
    """`{` minus `}` outside quoted strings, for multi-line inline tables."""
    depth, quote, escaped = 0, "", False
    for ch in text:
        if quote:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == quote:
                quote = ""
        elif ch in "\"'":
            quote = ch
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
    return depth


def rewrite_manifest(path: pathlib.Path, members: set[str], next_v: str) -> int:
    """Move a manifest to `next_v`: `[package]`/`[workspace.package]` version
    and every dependency-table entry whose package names a workspace member
    — the coordinated bump keeps manifests internally consistent, so the
    lockfile update afterwards resolves against the new member versions."""
    lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
    out, section, edited, index = [], "", 0, 0
    while index < len(lines):
        line = lines[index]
        found = section_of(line)
        if found is not None:
            section = found
        else:
            last = section.rsplit(".", 1)[-1].strip().strip("'\"")
            stripped = line.strip()
            if last == "package" and re.match(r'^version\s*=\s*"', stripped):
                line = VERSION_FIELD.sub(f'version = "{next_v}"', line, count=1)
                edited += 1
            elif last in DEP_KEYS:
                entry = ENTRY.match(stripped)
                if entry and entry.group("sub") == "version" \
                        and entry.group("key").strip("'\"") in members:
                    line = re.sub(r'=\s*"[^"]*"', f'= "{next_v}"', line, count=1)
                    edited += 1
                elif entry and entry.group("sub") is None:
                    block = [line]
                    while brace_depth("".join(block)) > 0 and index + 1 < len(lines):
                        index += 1
                        block.append(lines[index])
                    text = "".join(block)
                    value = entry.group("value")
                    package = PACKAGE_FIELD.search(text)
                    if not WORKSPACE_FIELD.search(text) \
                            and (package.group(1) if package else entry.group("key").strip("'\"")) in members:
                        if VERSION_FIELD.search(text):
                            text = VERSION_FIELD.sub(f'version = "{next_v}"', text, count=1)
                        elif value.lstrip().startswith('"'):
                            text = re.sub(r'=\s*"[^"]*"', f'= "{next_v}"', text, count=1)
                        elif "{" in text:
                            opening = re.search(r"\{(\s*)", text)
                            gap = opening.group(1)
                            closing = text[opening.end(1):].lstrip().startswith("}")
                            comma = "" if closing else ","
                            insert = (f'version = "{next_v}"{comma}' + gap
                                      if "\n" in gap else f'version = "{next_v}"{comma} ')
                            text = text[:opening.end(1)] + insert + text[opening.end(1):]
                        edited += 1
                    line = text
        out.append(line)
        index += 1
    if edited:
        path.write_text("".join(out), encoding="utf-8")
    return edited


def set_version(repo: pathlib.Path, next_v: str) -> int:
    """cargo_metadata-driven rewrite, data only: every member manifest and
    the workspace root — where [workspace.dependencies] pins member
    requirements — so no path dep is left requiring the old numbers."""
    metadata = cargo_metadata(repo)
    member_ids = set(metadata["workspace_members"])
    packages = [p for p in metadata["packages"] if p["id"] in member_ids]
    members = {p["name"] for p in packages}
    files = {pathlib.Path(p["manifest_path"]) for p in packages}
    files.add(pathlib.Path(metadata["workspace_root"]) / "Cargo.toml")
    return sum(rewrite_manifest(path, members, next_v) for path in sorted(files))


def cmd_set_version(args: argparse.Namespace) -> int:
    if parse_version(args.version) is None:
        raise TrainError(f"set-version wants <major>.<minor>.<patch>, got {args.version!r}")
    edited = set_version(pathlib.Path.cwd(), args.version)
    report(f"### Version bump\n\nSet every workspace member and member requirement "
           f"to `{args.version}` ({edited} field(s) rewritten).")
    return 0


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="release.py")
    commands = root.add_subparsers(dest="command", required=True)
    candidate = commands.add_parser("candidate")
    candidate.add_argument("--weekday", choices=WEEKDAYS, required=True)
    candidate.add_argument("--time", required=True)
    candidate.add_argument("--utc-offset", required=True)
    candidate.add_argument("--default-branch", required=True)
    candidate.add_argument("--override", default="")
    candidate.set_defaults(run=cmd_candidate)
    deps = commands.add_parser("deps")
    deps.add_argument("--wait-minutes", type=int, default=0)
    deps.set_defaults(run=cmd_deps)
    version = commands.add_parser("version")
    version.add_argument("--candidate", required=True)
    version.add_argument("--branch", default="")
    version.set_defaults(run=cmd_version)
    order = commands.add_parser("order")
    order.set_defaults(run=cmd_order)
    set_version_cmd = commands.add_parser("set-version")
    set_version_cmd.add_argument("version")
    set_version_cmd.set_defaults(run=cmd_set_version)
    freshness = commands.add_parser("freshness")
    freshness.add_argument("--window-days", type=int, default=14)
    freshness.set_defaults(run=cmd_freshness)
    return root


def main() -> int:
    args = parser().parse_args()
    try:
        return args.run(args)
    except TrainError as error:
        report(f"### Stopped\n\n{error}")
        print(f"::error::{error}", flush=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())

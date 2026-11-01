#!/usr/bin/env python3
"""Generated release train helper tests; regenerate it instead of editing
this copy. .github/release.lock records the source inputs that produced it.

Table-driven coverage of the manifest rewriter and its post-check over
synthetic manifests — plain python3, no cargo, no network: the
cargo_metadata read is stubbed with fabricated workspace data.
"""
from __future__ import annotations

import importlib.util
import pathlib
import tempfile
import unittest

HELPER = pathlib.Path(__file__).resolve().parent / "release.py"
spec = importlib.util.spec_from_file_location("train", HELPER)
train = importlib.util.module_from_spec(spec)
spec.loader.exec_module(train)

NEXT = "0.2.0"


def metadata(packages: dict, workspace_root: str = "") -> dict:
    """A cargo_metadata-shaped workspace: name -> {version, deps, ...}."""
    return {
        "workspace_members": list(packages),
        "workspace_root": workspace_root,
        "packages": [
            {
                "id": name,
                "name": name,
                "version": spec["version"],
                "publish": spec.get("publish"),
                "manifest_path": spec.get("manifest_path", ""),
                "dependencies": [
                    {"name": dep, "req": req}
                    for dep, req in spec.get("deps", {}).items()
                ],
            }
            for name, spec in packages.items()
        ],
    }


# (name, manifest lines, members, expected edit count, expected substrings,
#  absent substrings)
REWRITE_CASES = [
    (
        "bare-string member dep",
        [
            "[package]", 'name = "root"', 'version = "0.1.0"', "",
            "[dependencies]", 'x = "0.1.0"', 'serde = "1"',
        ],
        {"root", "x"},
        2,
        ['version = "0.2.0"', 'x = "0.2.0"', 'serde = "1"'],
        ['"0.1.0"'],
    ),
    (
        "inline table without version gains a pin",
        ["[dependencies]", 'x = { path = "crates/x" }'],
        {"x"},
        1,
        ['x = { version = "0.2.0", path = "crates/x" }'],
        [],
    ),
    (
        "multi-line inline table gains a pin",
        [
            "[dependencies]",
            "x = {",
            '  path = "crates/x",',
            '  features = ["f"],',
            "}",
        ],
        {"x"},
        1,
        ['version = "0.2.0",', 'path = "crates/x"'],
        [],
    ),
    (
        "foo.package + foo.version rename pair",
        [
            "[dependencies]",
            'renamed.package = "x"',
            'renamed.version = "0.1.0"',
        ],
        {"x"},
        1,
        ['renamed.package = "x"', 'renamed.version = "0.2.0"'],
        ['renamed.version = "0.1.0"'],
    ),
    (
        "[dependencies.foo] path-only gains a pin",
        ["[dependencies.x]", 'path = "crates/x"'],
        {"x"},
        1,
        ['[dependencies.x]\nversion = "0.2.0"\npath = "crates/x"'],
        [],
    ),
    (
        "target cfg dev-dependencies sub-table gains a pin",
        ["[target.'cfg(unix)'.dev-dependencies.x]", 'path = "crates/x"'],
        {"x"},
        1,
        ["[target.'cfg(unix)'.dev-dependencies.x]\nversion = \"0.2.0\""],
        [],
    ),
    (
        "a member already at target does not stop the bump",
        [
            "[package]", 'name = "root"', 'version = "0.2.0"', "",
            "[dependencies]", 'x = "0.1.0"',
        ],
        {"root", "x"},
        1,
        ['version = "0.2.0"', 'x = "0.2.0"'],
        ['"0.1.0"'],
    ),
    (
        "a manifest already at target is untouched",
        [
            "[package]", 'name = "root"', 'version = "0.2.0"', "",
            "[dependencies]", 'x = "0.2.0"',
        ],
        {"root", "x"},
        0,
        ['version = "0.2.0"', 'x = "0.2.0"'],
        [],
    ),
]


class HelperTest(unittest.TestCase):
    def stub_metadata(self, data: dict) -> None:
        previous = train.cargo_metadata
        train.cargo_metadata = lambda _repo: data
        self.addCleanup(setattr, train, "cargo_metadata", previous)


class RewriteManifestTest(HelperTest):
    def test_dependency_shapes(self):
        for name, lines, members, edits, present, absent in REWRITE_CASES:
            with self.subTest(name):
                with tempfile.TemporaryDirectory() as tmp:
                    path = pathlib.Path(tmp) / "Cargo.toml"
                    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
                    self.assertEqual(
                        train.rewrite_manifest(path, set(members), NEXT), edits)
                    text = path.read_text(encoding="utf-8")
                for needle in present:
                    self.assertIn(needle, text)
                for needle in absent:
                    self.assertNotIn(needle, text)


class VerifyBumpTest(HelperTest):
    """The post-check is proven against a fresh metadata read: every
    publishable member must sit at the target and every requirement naming
    a member must pin it."""

    def test_flags_a_member_left_at_the_old_version(self):
        self.stub_metadata(metadata({
            "root": {"version": "0.1.0", "deps": {"x": "^0.2.0"}},
            "x": {"version": "0.2.0"},
        }))
        with self.assertRaises(train.TrainError) as error:
            train.verify_bump(pathlib.Path("/workspace"), {"root", "x"}, NEXT)
        self.assertIn("could not confirm the bump", str(error.exception))
        self.assertIn("root is at 0.1.0", str(error.exception))

    def test_flags_a_member_requirement_left_behind(self):
        self.stub_metadata(metadata({
            "root": {"version": "0.2.0", "deps": {"x": "^0.1.0"}},
            "x": {"version": "0.2.0"},
        }))
        with self.assertRaises(train.TrainError) as error:
            train.verify_bump(pathlib.Path("/workspace"), {"root", "x"}, NEXT)
        self.assertIn("root requires x ^0.1.0 instead of 0.2.0",
                      str(error.exception))

    def test_a_workspace_at_target_passes(self):
        self.stub_metadata(metadata({
            "root": {"version": "0.2.0", "deps": {"x": "^0.2.0"}},
            "x": {"version": "0.2.0"},
        }))
        train.verify_bump(pathlib.Path("/workspace"), {"root", "x"}, NEXT)


class SetVersionRerunTest(HelperTest):
    """A rerun on an already-target workspace must succeed as a no-op —
    mixed-version workspaces, publish=false members and hand bumps are
    states the version command itself plans for, not abort conditions."""

    def test_an_already_target_workspace_is_a_clean_noop(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            (root / "crates/x").mkdir(parents=True)
            root_manifest = root / "Cargo.toml"
            member_manifest = root / "crates/x/Cargo.toml"
            root_manifest.write_text("\n".join([
                "[package]", 'name = "root"', 'version = "0.2.0"', "",
                "[workspace]", 'members = ["crates/*"]', "",
                "[dependencies]", 'x = "0.2.0"',
            ]) + "\n", encoding="utf-8")
            member_manifest.write_text(
                '[package]\nname = "x"\nversion = "0.2.0"\n',
                encoding="utf-8")
            before_root = root_manifest.read_text(encoding="utf-8")
            before_member = member_manifest.read_text(encoding="utf-8")
            self.stub_metadata(metadata({
                "root": {"version": "0.2.0", "deps": {"x": "^0.2.0"},
                         "manifest_path": str(root_manifest)},
                "x": {"version": "0.2.0",
                      "manifest_path": str(member_manifest)},
            }, workspace_root=str(root)))
            self.assertEqual(train.set_version(root, NEXT), 0)
            self.assertEqual(root_manifest.read_text(encoding="utf-8"),
                             before_root)
            self.assertEqual(member_manifest.read_text(encoding="utf-8"),
                             before_member)


if __name__ == "__main__":
    unittest.main()

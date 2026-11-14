#!/usr/bin/env python3
"""Enforce the docs ↔ tests contract for the example-based documentation.

Every code example in `docs/*.md` is announced by a marker:

    <!-- example: <section>/<name> -->

and must have an executing counterpart in
`Tests/DocumentationExamplesTests` named

    func test<Section><Name>()     (camel-cased, hyphens removed)

A marker may appear in several locales (docs/OPERATORS.md and
docs/ru/OPERATORS.md describe the same executed example) — one test
covers every locale carrying that marker.

This tool fails (exit 1) when a marker has no test, when a test claims
no marker, or when an intra-documentation markdown link is broken.

Usage:
    python3 Tools/check_doc_examples.py
"""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
LOCALE_DOCS = ROOT / "docs" / "ru"    # translated guides reuse markers
TESTS = ROOT / "Tests" / "DocumentationExamplesTests"

MARKER = re.compile(r"<!--\s*example:\s*([a-z0-9-]+)/([a-z0-9-]+)\s*-->")
FUNC = re.compile(r"^\s*func (test[A-Za-z0-9]+)\(\)", re.MULTILINE)
LINK = re.compile(r"\[[^\]]+\]\(([^)]+\.md)[^)]*\)")


def camel(token: str) -> str:
    return "".join(part[:1].upper() + part[1:] for part in token.split("-"))


def main() -> int:
    errors = []

    # 1. Collect markers from the guides. A marker may legitimately
    #    appear in several locales — one test covers every locale
    #    carrying that marker.
    markers = {}          # expected test name -> set of (file, marker)
    for doc_dir in (DOCS, LOCALE_DOCS):
        for doc in sorted(doc_dir.glob("*.md")):
            text = doc.read_text(encoding="utf-8")
            for section, name in MARKER.findall(text):
                method = f"test{camel(section)}{camel(name)}"
                markers.setdefault(method, set()).add(
                    (doc_dir.name + "/" + doc.name, f"{section}/{name}"))

    # 2. Collect example tests.
    tests = {}
    for test_file in sorted(TESTS.glob("*.swift")):
        text = test_file.read_text(encoding="utf-8")
        for func_name in FUNC.findall(text):
            tests[func_name] = test_file.name

    for method, sites in sorted(markers.items()):
        if method not in tests:
            where = ", ".join(sorted(f"{f} (`{m}`)" for f, m in sites))
            errors.append(f"marker has no executing test: {where} "
                          f"(expected {method}())")
    for func_name, test_file in sorted(tests.items()):
        if func_name.startswith("test") and func_name not in markers:
            errors.append(f"{test_file}: {func_name}() matches no "
                          f"`<!-- example: … -->` marker in docs/")

    # 3. Intra-documentation links (both locales).
    link_docs = list(DOCS.glob("*.md")) + list(LOCALE_DOCS.glob("*.md"))
    for doc in sorted(link_docs):
        for target in LINK.findall(doc.read_text(encoding="utf-8")):
            path = target.split("#")[0]
            if not path:
                continue
            if not (doc.parent / path).resolve().exists():
                errors.append(f"{doc.name}: broken link -> {target}")

    if errors:
        for error in errors:
            print("ERROR:", error)
        print(f"{len(errors)} problem(s)")
        return 1
    n_test_files = len(list(TESTS.glob("*.swift")))
    print(f"doc examples OK: {len(markers)} markers, all with executing "
          f"tests; {n_test_files} test files; links resolve")
    return 0


if __name__ == "__main__":
    sys.exit(main())

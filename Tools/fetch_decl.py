#!/usr/bin/env python3
"""Print the official Apple declaration(s) for a Combine API symbol.

Resolves a query against docs/api-inventory.json and fetches the DocC page
(through the crawl cache), printing every declaration line verbatim.

Usage:
    python3 Tools/fetch_decl.py Publisher
    python3 Tools/fetch_decl.py Publishers/Debounce
    python3 Tools/fetch_decl.py "Publisher/filter"
"""

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from crawl_combine_api import fetch  # noqa: E402

INV = HERE.parent / "docs" / "api-inventory.json"


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(2)
    query = sys.argv[1]
    inv = json.loads(INV.read_text(encoding="utf-8"))

    exact, partial = [], []
    for path, rec in inv["pages"].items():
        if rec.get("kind") == "missing":
            continue
        if path.endswith("/" + query + ".json"):
            exact.append(path)
        elif query.lower() in path.lower():
            partial.append(path)

    matches = exact or partial
    if not matches:
        print(f"no match for {query!r}")
        sys.exit(1)
    if len(matches) > 8:
        print(f"{len(matches)} matches; be more specific:")
        for m in matches[:20]:
            print("  " + m.removesuffix(".json").replace("documentation/", ""))
        sys.exit(1)

    for path in matches:
        doc = fetch(path)
        rec = inv["pages"].get(path, {})
        print(f"== {path.removesuffix('.json').replace('documentation/', '')}")
        if doc is None:
            print("   (page fetch failed)")
            continue
        decls = []
        from crawl_combine_api import extract_decls
        extract_decls(doc.get("primaryContentSections"), decls)
        for d in decls:
            for line in d.splitlines():
                print("   " + line)
        for ts in doc.get("topicSections") or []:
            print(f"   [{ts.get('title', '')}]: "
                  + ", ".join(i.removesuffix(".json").split("/")[-1] for i in ts.get("identifiers", [])))


if __name__ == "__main__":
    main()

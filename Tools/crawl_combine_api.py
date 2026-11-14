#!/usr/bin/env python3
"""Crawl Apple's official DocC JSON for the Combine framework -> API inventory.

Source of truth (official Apple documentation data):
    https://developer.apple.com/tutorials/data/documentation/...

Outputs:
    docs/api-inventory.json   machine-readable inventory (kind, decls, sections)
    docs/API-INVENTORY.md     checkbox checklist for implementation tracking

Raw DocC pages are cached in Tools/.apple-docc-cache/ (git-ignored); re-runs
resume from cache, so an interrupted crawl is cheap to restart.

Usage:
    python3 Tools/crawl_combine_api.py [--max-pages N] [--quiet]
"""

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

BASE = "https://developer.apple.com/tutorials/data/"
ROOT = "documentation/combine.json"
# Combine's schedulers live in other frameworks; crawl only their
# "Scheduling Combine Publishers" topic sections.
# Sections on external seeds whose titles don't contain "Combine" but
# which hold the Combine conformance (OperationQueue's page uses
# "Scheduling Operations" instead of the usual "Scheduling Combine
# Publishers").
EXTERNAL_SECTION_ALLOWLIST = {
    "documentation/Foundation/OperationQueue.json": {
        "Scheduling Operations",
        "Default Implementations",
    },
    # DispatchQueue/RunLoop keep the Scheduler protocol witnesses on a
    # "Default Implementations" section rather than the Combine-titled one.
    "documentation/Dispatch/DispatchQueue.json": {"Default Implementations"},
    "documentation/Foundation/RunLoop.json": {"Default Implementations"},
}

EXTERNAL = [
    "documentation/Dispatch/DispatchQueue.json",
    "documentation/Foundation/RunLoop.json",
    "documentation/Foundation/OperationQueue.json",
    "documentation/Foundation/NotificationCenter.json",
    "documentation/Foundation/Timer.json",
    "documentation/Foundation/URLSession.json",
]

HERE = Path(__file__).resolve().parent
CACHE = HERE / ".apple-docc-cache"
DOCS = HERE.parent / "docs"
WORKERS = 10
BATCH = 128
UA = {"User-Agent": "swift-combine-official-api-crawl/1.0"}


def clean(url: str) -> str:
    """doc://com.apple.Combine/documentation/Combine/X -> documentation/Combine/X.json"""
    if url.startswith("doc:"):
        url = re.sub(r"^doc://[^/]+/", "", url)
    return url if url.endswith(".json") else url + ".json"


def in_scope(path: str) -> bool:
    return path == ROOT or path.startswith("documentation/Combine/")


def cache_file(path: str) -> Path:
    return CACHE / path.replace("/", "__")


def fetch(path: str):
    """Fetch one DocC page via cache; returns parsed JSON or None (not found)."""
    cf = cache_file(path)
    if cf.exists():
        try:
            return json.loads(cf.read_text(encoding="utf-8"))
        except Exception:
            cf.unlink(missing_ok=True)  # corrupt cache entry: refetch
    last_err = None
    for attempt in range(4):
        try:
            req = urllib.request.Request(BASE + path, headers=UA)
            with urllib.request.urlopen(req, timeout=30) as r:
                data = r.read()
            cf.parent.mkdir(parents=True, exist_ok=True)
            cf.write_bytes(data)
            return json.loads(data)
        except urllib.error.HTTPError as e:
            last_err = f"HTTP {e.code}"
            if e.code == 404:
                return None
        except Exception as e:  # noqa: BLE001 - network noise, retry
            last_err = str(e)
        time.sleep(0.5 * (attempt + 1))
    print(f"  !! gave up on {path}: {last_err}", file=sys.stderr)
    return None


def extract_decls(node, out):
    if isinstance(node, dict):
        for d in node.get("declarations") or []:
            tokens = d.get("tokens")
            if tokens:
                text = "".join(t.get("text", "") for t in tokens)
                if text and text not in out:
                    out.append(text)
        for v in node.values():
            extract_decls(v, out)
    elif isinstance(node, list):
        for v in node:
            extract_decls(v, out)


def summarize(doc: dict) -> dict:
    meta = doc.get("metadata") or {}
    title = meta.get("title") or ""
    if not title and meta.get("fragments"):
        title = "".join(f.get("text", "") for f in meta["fragments"])
    decls = []
    extract_decls(doc.get("primaryContentSections"), decls)
    sections = []
    for ts in doc.get("topicSections") or []:
        sections.append({
            "title": ts.get("title", ""),
            "members": [clean(i) for i in ts.get("identifiers", [])],
        })
    relations = []
    for rs in doc.get("relationshipsSections") or []:
        relations.append({
            "title": rs.get("title", ""),
            "members": [clean(i) for i in rs.get("identifiers", [])],
        })
    abstract = "".join(a.get("text", "") for a in doc.get("abstract") or [])
    return {
        "kind": doc.get("kind", "?"),
        "title": title,
        "abstract": abstract,
        "decls": decls,
        "sections": sections,
        "relationships": relations,
    }


def collect_links(doc: dict, path: str):
    """Links to follow from this page.

    Combine-internal pages: every topic/identifier link.
    External scheduler pages: only the Combine-related topic sections,
    so the crawl never wanders into all of Dispatch/Foundation.
    """
    out = set()
    external = path in EXTERNAL
    allowlisted = EXTERNAL_SECTION_ALLOWLIST.get(path, set())
    for ts in doc.get("topicSections") or []:
        if (external
                and "combine" not in ts.get("title", "").lower()
                and ts.get("title") not in allowlisted):
            continue
        for ident in ts.get("identifiers", []):
            out.add(clean(ident))

    def walk(node):
        if isinstance(node, dict):
            ident = node.get("identifier")
            if isinstance(ident, dict) and "url" in ident and "kind" in node:
                url = clean(ident["url"])
                if not external or url.startswith(("documentation/Combine/", "documentation/Dispatch/", "documentation/Foundation/")):
                    out.add(url)
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)

    if not external:
        walk(doc)
    return out


def crawl(max_pages: int, quiet: bool) -> dict:
    seen = {}
    queue = [ROOT] + EXTERNAL
    queued = set(queue)
    missing = []

    while queue and len(seen) < max_pages:
        batch = queue[:BATCH]
        del queue[:BATCH]
        fresh = [p for p in batch if p not in seen]
        if not fresh:
            continue
        with ThreadPoolExecutor(WORKERS) as ex:
            futures = {ex.submit(fetch, p): p for p in fresh}
            for fut in as_completed(futures):
                path = futures[fut]
                doc = fut.result()
                if doc is None:
                    seen[path] = {"kind": "missing"}
                    missing.append(path)
                    continue
                summary = summarize(doc)
                if path in EXTERNAL:
                    keep = EXTERNAL_SECTION_ALLOWLIST.get(path, set())
                    summary["sections"] = [
                        sec for sec in summary["sections"]
                        if "combine" in sec["title"].lower()
                        or sec["title"] in keep
                    ]
                seen[path] = summary
                for link in sorted(collect_links(doc, path)):
                    if not in_scope(link) and link not in EXTERNAL \
                            and not any(link.startswith(ext.removesuffix(".json") + "/")
                                        for ext in EXTERNAL):
                        continue
                    if link not in queued:
                        queued.add(link)
                        queue.append(link)
        if not quiet:
            print(f"  crawled {len(seen)} pages, {len(queue)} queued", flush=True)

    return {
        "source": BASE + "documentation/… (Apple official DocC JSON)",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "pages": {p: seen[p] for p in sorted(seen)},
        "unfetched": sorted(queued - set(seen)),
        "missing": sorted(missing),
    }


def short(path: str) -> str:
    return path.replace("documentation/", "").removesuffix(".json")


def write_markdown(inv: dict) -> str:
    pages = inv["pages"]
    top, publishers, subs, schedulers, articles, other = [], [], [], [], [], []
    members = {}  # top-level name -> list of member rows

    for path, rec in pages.items():
        if rec.get("kind") == "missing":
            continue
        name = short(path)
        if rec["kind"] == "article":
            articles.append((name, rec))
            continue
        if path.startswith(("documentation/Dispatch/", "documentation/Foundation/")):
            schedulers.append((name, rec))
            continue
        parts = name.removeprefix("Combine/").split("/")
        head = parts[0]
        rest = "/".join(parts[1:])
        if len(parts) == 1:
            top.append((head, rec))
        else:
            if head == "Publishers":
                publishers.append((name, rec)) if not rest.count("/") else other.append((name, rec))
            elif head == "Subscribers":
                subs.append((name, rec)) if not rest.count("/") else other.append((name, rec))
            else:
                members.setdefault(head, []).append((name, rec))

    def row(item):
        name, rec = item
        decl = (rec["decls"][0].replace("\n", " ") if rec["decls"] else rec["title"])
        n_members = sum(len(s["members"]) for s in rec["sections"])
        extra = f" · {n_members} members" if n_members else ""
        return f"- [ ] `{name}` — `{decl}`{extra}"

    lines = [
        "# Combine API inventory",
        "",
        f"Generated by `Tools/crawl_combine_api.py` from Apple's official DocC JSON "
        f"({inv['generated_at']}). {len(pages)} pages crawled.",
        "",
        "Checkbox granularity is per type; member lists (protocol requirements,",
        "operator methods) live in `docs/api-inventory.json` under each type's `sections`.",
        "",
    ]

    def section(title, items, sub=None):
        lines.append(f"## {title} ({len(items)})")
        lines.append("")
        for it in sorted(items, key=lambda x: x[0]):
            lines.append(row(it))
            if sub and it[0] in sub:
                for mname, mrec in sorted(sub[it[0]], key=lambda x: x[0]):
                    mdecl = mrec["decls"][0].replace("\n", " ") if mrec["decls"] else mname
                    lines.append(f"  - [ ] `{mname}` — `{mdecl}`")
        lines.append("")

    section("Top-level types", top, members)
    section("Publishers.* (operator types)", publishers)
    section("Subscribers.*", subs)
    section("External: Schedulers (Dispatch/Foundation Combine conformances)", schedulers)
    section("Articles (documentation)", articles)
    if other:
        section("Other nested symbols", other)
    return "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--max-pages", type=int, default=3000)
    ap.add_argument("--quiet", action="store_true")
    args = ap.parse_args()

    t0 = time.time()
    inv = crawl(args.max_pages, args.quiet)
    DOCS.mkdir(parents=True, exist_ok=True)
    (DOCS / "api-inventory.json").write_text(
        json.dumps(inv, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    (DOCS / "API-INVENTORY.md").write_text(write_markdown(inv), encoding="utf-8")

    kinds = {}
    for rec in inv["pages"].values():
        kinds[rec.get("kind", "?")] = kinds.get(rec.get("kind", "?"), 0) + 1
    print(f"OK {len(inv['pages'])} pages {kinds} in {time.time() - t0:.0f}s")
    if inv["missing"]:
        print(f"missing ({len(inv['missing'])}): {', '.join(short(m) for m in inv['missing'])}")
    if inv["unfetched"]:
        print(f"unfetched ({len(inv['unfetched'])}): {', '.join(short(m) for m in inv['unfetched'][:20])}")


if __name__ == "__main__":
    main()

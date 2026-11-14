#!/usr/bin/env python3
"""Member-level declaration verification (M7 deep audit).

For every *implemented* type in docs/api-inventory.json, take the crawled
official member declarations and require a matching declaration shape in
Sources/LinuxCombine:

    official `func name(firstLabel: …)` -> `func name` + `firstLabel:`
    official `init(firstLabel: …)`      -> `init(firstLabel:` (or `init(_`/`init()`)
    official `var|let name`             -> `var|let name`
    official `typealias name`           -> `typealias name`

Presence + signature-shape verification, not full type equality (Swift
source parsing is heuristic): anything ambiguous lands in the TRIAGE list
instead of silently passing. Known exclusions are explicit and appear in
the report with their reasons.

Usage:
    python3 Tools/verify_decls.py      # writes docs/DECL-AUDIT.md
    echo $?                             # 1 when failures exist
"""

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import audit_inventory as audit  # noqa: E402

INVENTORY = audit.INVENTORY
SOURCES = audit.SOURCES
REPORT = audit.ROOT / "docs" / "DECL-AUDIT.md"

EXCLUDED_MEMBERS = {
    # Another framework's extension documented on the Cancellable page.
    "storeWhileEntityActive": "other-framework extension (QUIRKS C5)",
}

# The operator-sugar backends landed (batch 5): every member of
# Just / Publishers.Sequence is now verified, including the
# Optional.Publisher / Result.Publisher sugar.
CORE_ONLY = {}

# (No backend markers any more — Optional.Publisher / Result.Publisher
# are implemented; decls merely *mentioning* them are checked too.)


def normalize(decl: str) -> str:
    return re.sub(r"\s+", " ", decl).strip()


def param_labels(header: str) -> list:
    start = header.find("(")
    if start < 0:
        return []
    depth = 0
    params, current, collecting = [], "", False
    for ch in header[start:]:
        if not collecting and ch == "(":
            collecting = True
            continue
        if collecting:
            if ch in "(<[{":
                depth += 1
            elif ch in ">)]}":
                if depth == 0:
                    params.append(current)
                    break
                depth -= 1
            elif ch == "," and depth == 0:
                params.append(current)
                current = ""
                continue
            if depth >= 0:
                current += ch
    labels = []
    for p in params:
        p = p.strip()
        if not p:
            continue
        # The label is the FIRST token of the parameter:
        #   `in collection: inout C` -> `in`, `_ cancel: ...` -> `_`,
        #   `by n: Stride` -> `by`, `cancel: T` -> `cancel`.
        head = p.split(":", 1)[0].strip().split(" ")
        token = head[0]
        if token.startswith("@"):
            token = head[-1]
        labels.append(token.rstrip(","))
    return labels


def official_keys(decls: list) -> dict:
    """key -> official declaration. Key shapes:
    'func <name>(<firstLabel>)', 'init(<firstLabel>)',
    'var <name>', 'let <name>', 'typealias <name>'.
    """
    keys = {}
    for raw in decls:
        decl = normalize(raw)
        m = re.match(
            r"(?:(?:@\w+(?:\([^)]*\))?|final|static|public|private|internal|"
            r"mutating|nonmutating|override|class|lazy)\s+)*func\s+"
            r"(`[^`]+`|[A-Za-z_+\-*/%=<>!?&|^.]+)\s*(\(|<)",
            decl,
        )
        if m:
            op = m.group(1).strip("`")
            labels = param_labels(decl)
            first = labels[0] if labels else ""
            keys[f"func {op}({first})"] = decl
            continue
        m = re.match(
            r"(?:(?:@\w+(?:\([^)]*\))?|final|static|public|private|internal|"
            r"mutating)\s+)*init\??\s*(\(|<)",
            decl,
        )
        if m:
            labels = param_labels(decl)
            if not labels:
                keys["init()"] = decl
            elif labels[0] == "_":
                keys["init(_)"] = decl
            else:
                keys[f"init({labels[0]}:)"] = decl
            continue
        m = re.match(
            r"(?:(?:@\w+(?:\([^)]*\))?|final|static|public|private|internal|"
            r"lazy|weak|class)\s+)*(var|let)\s+([A-Za-z_]\w*)",
            decl,
        )
        if m:
            keys[f"{m.group(1)} {m.group(2)}"] = decl
            continue
        m = re.match(r"(?:@\w+\s+)*typealias\s+([A-Za-z_]\w*)", decl)
        if m:
            keys[f"typealias {m.group(1)}"] = decl
            continue
        m = re.match(r"case\s+(`[^`]+`|[A-Za-z_]\w*)", decl)
        if m:
            keys[f"case {m.group(1).strip('`')}"] = decl
            continue
        # Nested type declarations and associatedtypes are checked as
        # member keys too (they also appear as their own type pages).
        m = re.match(
            r"(?:(?:@\w+(?:\([^)]*\))?|final|public|indirect)\s+)*"
            r"(struct|enum|class|protocol|actor)\s+(`[^`]+`|[A-Za-z_]\w*)",
            decl,
        )
        if m:
            keys[f"{m.group(1)} {m.group(2).strip('`')}"] = decl
            continue
        m = re.match(r"(?:@\w+\s+)*associatedtype\s+(`[^`]+`|[A-Za-z_]\w*)", decl)
        if m:
            keys[f"associatedtype {m.group(1).strip('`')}"] = decl
            continue
        keys[f"? {decl[:70]}"] = decl
    return keys


def core_name(key: str) -> str:
    if key.startswith(("var ", "let ", "typealias ")):
        return key.split(" ", 1)[1]
    if key.startswith("func "):
        return key[5:].split("(", 1)[0]
    return key  # init(...) incl. parens


def load_sources() -> str:
    chunks = []
    for file in sorted(SOURCES.rglob("*.swift")):
        chunks.append(re.sub(r"//.*", "", file.read_text(encoding="utf-8")))
    return "\n".join(chunks)


def ours_has(key: str, sources: str) -> bool:
    if key.startswith("init"):
        label = key[4:].strip("()")[:-1] if key not in ("init()", "init(_)") else key
        if key == "init()":
            return re.search(r"\binit\s*\(\s*\)", sources) is not None
        if key == "init(_)":
            return re.search(r"\binit\s*\(\s*_\s", sources) is not None
        label = key[5:-1].rstrip(":")  # init(label:) -> label
        return re.search(
            rf"\binit\??\s*(<[^>]*>)?\s*\([^)]*{re.escape(label)}"
            rf"(\s+[A-Za-z_]\w*)?\s*:",
            sources,
        ) is not None
    if key.startswith("func "):
        body = key[5:]
        name, rest = body.rsplit("(", 1)
        label = rest.rstrip(")")
        # The generic part may carry inline constraints (`<S: Sequence>`);
        # match only the base name and let `(<...>)?` cover the rest.
        # Operator names (`<`, `<=`) must not be split on "<", and some
        # declarations are written with backticks (`` `catch` ``).
        if "<" in name and not re.fullmatch(r"[<>=!+\-*/%&|^?~]+", name):
            base = name.split("<", 1)[0]
        else:
            base = name
        pattern = rf"func\s+`?{re.escape(base)}`?\s*(<[^>]*>)?\s*\("
        for m in re.finditer(pattern, sources):
            window = sources[m.start(): m.start() + 500]
            head = window.split("{", 1)[0]
            if label == "":
                if re.search(rf"{re.escape(name)}\s*(<[^>]*>)?\s*\(\s*\)", head):
                    return True
            elif re.search(rf"\b{re.escape(label)}(\s+[A-Za-z_]\w*)?\s*:", head):
                return True
        return False
    kind, name = key.split(" ", 1)
    return re.search(rf"\b{kind}\s+{re.escape(name)}\b", sources) is not None


def main() -> int:
    inv = json.loads(INVENTORY.read_text(encoding="utf-8"))
    declared = audit.load_declared_symbols()
    sources = load_sources()

    checked = passed = 0
    failures, triage, excluded = set(), [], set()

    def walk_members(name: str, rec: dict) -> None:
        nonlocal checked, passed
        for section in rec.get("sections", []):
            for member_path in section.get("members", []):
                member_name = member_path.split("/")[-1]
                base = member_name.split("(")[0]
                if base in EXCLUDED_MEMBERS:
                    excluded.add((name, member_name, EXCLUDED_MEMBERS[base]))
                    continue
                mrec = inv["pages"].get(member_path)
                if not mrec or mrec.get("kind") == "missing":
                    if member_path.startswith(
                        ("documentation/Foundation/", "documentation/Dispatch/")
                    ):
                        continue  # only Combine-facing external members are in scope
                    triage.append((name, member_name, "page not crawled"))
                    continue
                decls = mrec.get("decls") or []
                if not decls:
                    continue
                keys = official_keys(decls)
                for key, decl in keys.items():
                    if key.startswith("? "):
                        triage.append((name, key, decl))
                        continue
                    checked += 1
                    if ours_has(key, sources):
                        passed += 1
                    else:
                        failures.add((name, key, decl))

    # The generic-operator sugar lives on DocC *article* extension pages
    # (kind == "article"), which the type-page filter never reaches —
    # attribute their sections to the base type (batch 5, QUIRKS C10).
    extension_articles = {
        "documentation/Combine/Just-Publisher-Operators.json": "Just",
        "documentation/Combine/Publishers-Sequence-Publisher-Operators.json":
            "Publishers/Sequence",
    }

    for path, rec in sorted(inv["pages"].items()):
        if path in extension_articles:
            walk_members(extension_articles[path], rec)
            continue
        if not audit.is_type_page(path, rec):
            continue
        name = audit.short(path)
        simple = re.sub(r"-swift\.(struct|class|enum|protocol|typealias)$", "",
                        name.split("/")[-1])
        if simple not in declared:
            continue  # backlog type: its members are out of scope
        walk_members(name, rec)

    lines = [
        "# Declaration audit (member level)",
        "",
        f"Generated by `Tools/verify_decls.py` on "
        f"{datetime.now(timezone.utc).isoformat(timespec='seconds')}.",
        "",
        f"- official member keys checked: **{checked}**",
        f"- matched in `Sources/LinuxCombine`: **{passed}**",
        f"- failures: **{len(failures)}**",
        f"- excluded by policy: {len(excluded)}",
        f"- unrecognized/unreachable official shapes (triage): {len(triage)}",
        "",
        "## Failures",
        "",
    ]
    lines += [f"- `{n}` — `{k}` — official: `{d}`" for n, k, d in sorted(failures)] or ["_none_"]
    lines += ["", "## Triage", ""]
    lines += [f"- `{n}` — `{k}` — {d}" for n, k, d in sorted(triage)] or ["_none_"]
    lines += ["", "## Excluded by policy", ""]
    lines += [f"- `{m}` — {r}" for n, m, r in sorted(excluded)] or ["_none_"]
    lines.append("")
    REPORT.write_text("\n".join(lines), encoding="utf-8")

    print(f"checked {checked}, matched {passed}, failures {len(failures)}, "
          f"triage {len(triage)}, excluded {len(excluded)}")
    for n, k, d in sorted(failures)[:30]:
        print(f"  FAIL {n} :: {k}")
    if len(failures) > 30:
        print(f"  ... {len(failures) - 30} more in docs/DECL-AUDIT.md")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

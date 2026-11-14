#!/usr/bin/env python3
"""Render the entire documentation set to a single PDF.

Pipeline: docs/*.md (+ README.md as front matter) -> markdown (tables,
fenced code with Pygments highlighting) -> styled HTML with a linked
table of contents (WeasyPrint renders `target-counter` page numbers)
-> PDF.

Requires the `weasyprint` and `markdown` Python packages (see
Tools/requirements-pdf.txt).

Usage:
    python3 Tools/render_docs_pdf.py             # English volume
    python3 Tools/render_docs_pdf.py --lang ru   # Russian volume
"""

import re
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
OUTPUT = DOCS / "LinuxCombine-Documentation.pdf"

# Reading order: front matter, guides, engineering notes, references.
ORDER = [
    ("../README.md", "Overview & status"),
    ("GETTING-STARTED.md", "Getting started"),
    ("CONCEPTS.md", "Concepts"),
    ("OPERATORS.md", "Operators"),
    ("SCHEDULERS.md", "Schedulers"),
    ("SUBJECTS-AND-STATE.md", "Subjects and state"),
    ("FOUNDATION.md", "Foundation bridges"),
    ("ASYNC-AWAIT.md", "Async/await interop"),
    ("COMPATIBILITY.md", "Compatibility"),
    ("QUIRKS.md", "Behavior quirks register"),
    ("PERF.md", "Performance"),
    ("BACKLOG.md", "Backlog & scope"),
    ("API-REFERENCE.md", "API reference"),
    ("API-INVENTORY.md", "API inventory (crawl)"),
    ("DECL-AUDIT.md", "Declaration audit"),
]

# Russian edition: the translated guides plus the new introductory
# chapter (docs/ru/).
RU_ORDER = [
    ("INTRO.md", "Введение: реактивный подход и модель Combine"),
    ("README.md", "Обзор проекта"),
    ("GETTING-STARTED.md", "Начало работы"),
    ("CONCEPTS.md", "Основные понятия"),
    ("OPERATORS.md", "Операторы"),
    ("SCHEDULERS.md", "Планировщики"),
    ("SUBJECTS-AND-STATE.md", "Субъекты и состояние"),
    ("FOUNDATION.md", "Мосты к Foundation"),
    ("ASYNC-AWAIT.md", "Совместимость с async/await"),
    ("COMPATIBILITY.md", "Совместимость"),
]

DOC_ID = {
    "../README.md": "overview",
    "GETTING-STARTED.md": "getting-started",
    "CONCEPTS.md": "concepts",
    "OPERATORS.md": "operators",
    "SCHEDULERS.md": "schedulers",
    "SUBJECTS-AND-STATE.md": "subjects-and-state",
    "FOUNDATION.md": "foundation",
    "ASYNC-AWAIT.md": "async-await",
    "COMPATIBILITY.md": "compatibility",
    "QUIRKS.md": "quirks",
    "PERF.md": "perf",
    "BACKLOG.md": "backlog",
    "API-REFERENCE.md": "api-reference",
    "API-INVENTORY.md": "api-inventory",
    "DECL-AUDIT.md": "decl-audit",
}

RU_DOC_ID = {
    "INTRO.md": "intro",
    "README.md": "overview",
    "GETTING-STARTED.md": "getting-started",
    "CONCEPTS.md": "concepts",
    "OPERATORS.md": "operators",
    "SCHEDULERS.md": "schedulers",
    "SUBJECTS-AND-STATE.md": "subjects-and-state",
    "FOUNDATION.md": "foundation",
    "ASYNC-AWAIT.md": "async-await",
    "COMPATIBILITY.md": "compatibility",
}

CSS = """
@page {
    size: A4;
    margin: 22mm 18mm 20mm 18mm;
    @top-center { content: "Combine — documentation"; font-size: 8pt;
                  color: #666; }
    @bottom-center { content: "page " counter(page) " of " counter(pages);
                     font-size: 8pt; color: #666; }
}
@page :first { @top-center { content: ""; } @bottom-center { content: ""; } }
body { font-family: sans-serif; font-size: 9.5pt; line-height: 1.45;
       color: #1a1a1a; }
h1 { font-size: 20pt; color: #b03a2e; border-bottom: 2px solid #b03a2e;
     padding-bottom: 4pt; margin-bottom: 10pt; }
h2 { font-size: 14pt; color: #7c2d24; margin-top: 16pt; }
h3 { font-size: 11.5pt; color: #333; margin-top: 12pt; }
h1.doc { page-break-before: always; }
a { color: #1a5276; text-decoration: none; }
code { font-family: monospace; font-size: 8.6pt;
       background: #f4f1ee; padding: 0 2pt; border-radius: 2pt; }
pre { background: #f7f5f3; border: 1px solid #ddd6d0;
      border-left: 3px solid #b03a2e; padding: 7pt 9pt;
      border-radius: 3pt; overflow-wrap: break-word; white-space: pre-wrap;
      font-size: 8.2pt; line-height: 1.35; page-break-inside: avoid; }
pre code { background: none; padding: 0; font-size: 8.2pt; }
table { border-collapse: collapse; margin: 8pt 0; font-size: 8.6pt;
        width: 100%; }
th, td { border: 1px solid #cfc8c2; padding: 3pt 6pt; text-align: left;
         vertical-align: top; }
th { background: #efe9e4; }
blockquote { border-left: 3px solid #c9c2bb; margin: 6pt 0 6pt 8pt;
             padding-left: 8pt; color: #555; }
hr { border: none; border-top: 1px solid #d8d2cc; margin: 14pt 0; }
ul, ol { margin: 4pt 0 4pt 14pt; padding: 0; }
li { margin: 1.5pt 0; }
/* cover */
.cover { text-align: center; margin-top: 90mm; }
.cover .title { font-size: 30pt; color: #b03a2e; font-weight: bold; }
.cover .sub { font-size: 13pt; color: #444; margin-top: 8pt; }
.cover .meta { font-size: 9.5pt; color: #777; margin-top: 30mm; }
/* toc */
.toc ul { list-style: none; margin-left: 0; }
.toc > ul > li { margin: 3.5pt 0; }
.toc a::after { content: target-counter(attr(href), page);
                float: right; color: #777; }
.toc .lvl2 { margin-left: 14pt; font-size: 8.8pt; }
.toc .lvl2 a::after { content: ""; }
"""

TOC_H2 = {"OPERATORS.md", "SCHEDULERS.md", "CONCEPTS.md", "QUIRKS.md",
          "API-REFERENCE.md"}


def main() -> int:
    import markdown

    try:
        import weasyprint  # noqa: F401
    except ImportError:
        print("weasyprint is required: "
              "python3 -m pip install weasyprint markdown pygments")
        return 2

    from pygments.formatters import HtmlFormatter

    russian = "--lang" in sys.argv and \
        sys.argv[sys.argv.index("--lang") + 1] == "ru"
    docs_dir = DOCS / "ru" if russian else DOCS
    order = RU_ORDER if russian else ORDER
    doc_id = RU_DOC_ID if russian else DOC_ID
    output = docs_dir / "LinuxCombine-Documentation-RU.pdf" if russian \
        else OUTPUT
    title_top = "LinuxCombine — документация (русское издание)" if russian \
        else "LinuxCombine — documentation"
    cover_title = "LinuxCombine"
    cover_sub = ("Полная документация — русское издание"
                 if russian else
                 "Pure-Swift Combine reimplementation — complete documentation")
    cover_meta = ("321/321 официальных символа · 1091 проверяемое "
                  "объявление · выполнено 1644 тестов · "
                  f"дата рендера: {date.today().isoformat()}"
                  if russian else
                  "321/321 official symbols · 1091 verified declarations"
                  f" · 1644 tests · rendered {date.today().isoformat()}")
    toc_title = "Содержание" if russian else "Table of contents"
    font_stack = ("'DejaVu Sans', sans-serif" if russian
                  else "sans-serif")
    mono_stack = ("'DejaVu Sans Mono', monospace" if russian
                  else "monospace")

    parts = []
    toc_entries = []          # (level, href, title)

    for rel, title in order:
        path = (docs_dir / rel).resolve()
        text = path.read_text(encoding="utf-8")

        # Rewrite intra-documentation links to document anchors.
        def rewrite(match: re.Match) -> str:
            target = match.group(1)
            base = target.split("#")[0]
            mapped = doc_id.get(base)
            if mapped:
                return f"](#{'doc-' + mapped})"
            return match.group(0)
        text = re.sub(r"\]\(([^)]+\.md[^)]*)\)", rewrite, text)

        this_doc = doc_id[rel]
        extensions = ["tables", "fenced_code", "sane_lists",
                      "codehilite", "toc"]
        md = markdown.Markdown(
            extensions=extensions,
            extension_configs={
                "codehilite": {"guess_lang": False, "noclasses": False},
                "toc": {"permalink": False},
            },
        )
        body = md.convert(text)

        # Collect headings for the table of contents.
        def walk(tokens, level=2, prefix=this_doc):
            for token in tokens:
                if not isinstance(token, dict):
                    continue
                if level <= 2:
                    toc_entries.append(
                        (level, token["name"],
                         f"#{prefix}-{token['id']}"))
                if rel in TOC_H2 and level == 2:
                    walk(token.get("children", []), level + 1, prefix)

        walk(md.toc_tokens)

        # Namespace the anchors: each doc is converted separately and
        # would otherwise reuse ids like "contents".
        body = re.sub(r'id="([^"]+)"',
                      lambda m: f'id="{this_doc}-{m.group(1)}"', body)
        body = re.sub(r'href="#([^"]+)"',
                      lambda m: f'href="#{this_doc}-{m.group(1)}"', body)

        parts.append(
            f'<section class="doc" id="doc-{this_doc}">'
            f'<h1 class="doc">{title}</h1>{body}</section>'
        )
        print(f"  + {rel} ({len(text)//1024} KB)")

    toc_html = ['<div class="toc"><ul>']
    for level, name, href in toc_entries:
        cls = ' class="lvl2"' if level > 2 else ""
        toc_html.append(f'<li{cls}><a href="{href}">{name}</a></li>')
    toc_html.append("</ul></div>")

    css = CSS.replace("sans-serif", font_stack).replace(
        "monospace", mono_stack)
    css = css.replace("Combine — documentation", title_top)

    html = f"""<!doctype html>
<html><head><meta charset="utf-8"><style>
{css}
{HtmlFormatter(style="default").get_style_defs(".codehilite")}
</style></head>
<body>
<div class="cover">
  <div class="title">{cover_title}</div>
  <div class="sub">{cover_sub}</div>
  <div class="meta">{cover_meta}</div>
</div>
<h1 class="doc">{toc_title}</h1>
{''.join(toc_html)}
{''.join(parts)}
</body></html>"""

    from weasyprint import HTML
    document = HTML(string=html, base_url=str(ROOT)).render()
    document.write_pdf(output)
    print(f"wrote {output.relative_to(ROOT)}: {len(document.pages)} pages, "
          f"{output.stat().st_size // 1024} KB")
    return 0


if __name__ == "__main__":
    sys.exit(main())

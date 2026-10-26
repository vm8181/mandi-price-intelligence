"""
Checks the Power BI project files (PBIP) without opening Power BI. Run in CI on every push.

    python tools/check_powerbi.py --schemas <json-schemas checkout> --theme-schema <reportThemeSchema.json>

Four checks, each catching a mistake Power BI Desktop would hide or only show when the report opens:

1. Structure: every report file (report, pages, visuals) is valid against Microsoft's published
   JSON schema for its version. Catches hand edits that break a file.
2. Formatting names: every formatting property a visual sets exists for that visual type, per
   Microsoft's report theme schema. Desktop silently drops unknown names (it dropped a slicer
   "fontSize" that should have been "textSize"), so the setting just never applies.
3. Field references: every table, column and measure a visual uses exists in the semantic model
   (the TMDL files). Catches a visual left pointing at a renamed or deleted measure.
4. Layout: no visual runs off the 1920 x 1080 page, and no two visuals overlap (page headers and
   their navigation buttons are designed to sit together and are allowed to).

Exit code 1 if any check fails, so the CI run turns red.
"""
import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
REPORT = ROOT / "powerbi" / "MandiPriceIntelligence.Report" / "definition"
MODEL = ROOT / "powerbi" / "MandiPriceIntelligence.SemanticModel" / "definition"
SCHEMA_PREFIX = "https://developer.microsoft.com/json-schemas/"
PAGE_W, PAGE_H = 1920, 1080

problems = []


def problem(check, where, what):
    problems.append(f"[{check}] {where}: {what}")


def rel(p):
    return str(p.relative_to(ROOT))


def load(p):
    return json.loads(p.read_text(encoding="utf-8-sig"))


# ---------------------------------------------------------------- 1. structure
def version_key(v):
    return tuple(int(x) for x in v.split("."))


def schema_file(schemas: Path, url: str) -> Path:
    """Local copy of a published schema. If this exact version isn't in the checkout yet
    (Desktop can be ahead of the published repo), use the newest earlier version."""
    path = url.split(SCHEMA_PREFIX, 1)[1].split("#")[0]
    exact = schemas / path
    if exact.exists():
        return exact
    m = re.match(r"(.*)/(\d+\.\d+\.\d+)/(schema\.json)$", path)
    if not m:
        raise FileNotFoundError(path)
    folder, wanted, name = m.groups()
    versions = sorted((d.name for d in (schemas / folder).iterdir()
                       if d.is_dir() and re.fullmatch(r"\d+\.\d+\.\d+", d.name)), key=version_key)
    earlier = [v for v in versions if version_key(v) <= version_key(wanted)]
    if not earlier:
        raise FileNotFoundError(path)
    return schemas / folder / earlier[-1] / name


def check_structure(schemas: Path):
    from jsonschema import Draft7Validator, Draft202012Validator
    from referencing import Registry, Resource

    def retrieve(uri):
        return Resource.from_contents(json.loads(schema_file(schemas, uri).read_text(encoding="utf-8")))

    registry = Registry(retrieve=retrieve)
    checked = 0
    for f in sorted(REPORT.rglob("*.json")):
        doc = load(f)
        url = doc.get("$schema", "")
        if SCHEMA_PREFIX not in url:
            continue
        local = schema_file(schemas, url)
        if local.parent.name not in url:                 # newer than the published schemas:
            doc["$schema"] = SCHEMA_PREFIX + local.relative_to(schemas).as_posix()   # validate against the newest one
        schema = json.loads(local.read_text(encoding="utf-8"))
        cls = Draft7Validator if "draft-07" in schema.get("$schema", "") else Draft202012Validator
        for e in list(cls(schema, registry=registry).iter_errors(doc))[:3]:
            problem("structure", rel(f), f"{list(e.absolute_path)} {e.message[:160]}")
        checked += 1
    return checked


# ---------------------------------------------------------------- 2. formatting names
def check_formatting(theme_schema: Path):
    s = json.loads(theme_schema.read_text(encoding="utf-8"))
    defs = s["definitions"]

    def resolve(x):
        while "$ref" in x:
            x = defs[x["$ref"].split("/")[-1]]
        return x

    def props(x):
        x = resolve(x)
        out = dict(x.get("properties", {}))
        for part in x.get("allOf", []):
            out.update(props(part))
        return out

    checked = 0
    for f in sorted(REPORT.rglob("visual.json")):
        v = load(f)["visual"]
        vtype = v["visualType"]
        if f"visual-{vtype}" not in defs:
            continue                                     # custom visual: no published list
        objects = props(defs[f"visual-{vtype}"])
        for group in ("objects", "visualContainerObjects"):
            for obj, entries in v.get(group, {}).items():
                if obj not in objects:
                    problem("formatting", rel(f), f"{vtype} has no formatting group '{obj}'")
                    continue
                o = resolve(objects[obj])
                allowed = props(o.get("items", o))
                for entry in entries:
                    for name in entry.get("properties", {}):
                        if vtype == "slicer" and obj == "general" and name == "filter":
                            continue                     # a slicer's saved selection: valid in reports
                        if name not in allowed:
                            problem("formatting", rel(f), f"{vtype}.{obj} has no property '{name}'")
        checked += 1
    return checked


# ---------------------------------------------------------------- 3. field references
def model_fields():
    """{table: set(columns and measures)} read from the TMDL files."""
    fields = {}
    for f in sorted((MODEL / "tables").glob("*.tmdl")):
        table = None
        for line in f.read_text(encoding="utf-8").splitlines():
            m = re.match(r"^table\s+('([^']+)'|(\S+))", line)
            if m:
                table = m.group(2) or m.group(3)
                fields.setdefault(table, set())
                continue
            m = re.match(r"^\t(column|measure)\s+('([^']+)'|([^\s=]+))", line)
            if m and table:
                fields[table].add(m.group(3) or m.group(4))
    return fields


def references(node, out):
    """Every (entity, property) a visual's query or formatting points at."""
    if isinstance(node, dict):
        for kind in ("Column", "Measure"):
            ref = node.get(kind)
            if isinstance(ref, dict) and "Property" in ref:
                entity = ref.get("Expression", {}).get("SourceRef", {}).get("Entity")
                if entity:
                    out.add((entity, ref["Property"]))
        for value in node.values():
            references(value, out)
    elif isinstance(node, list):
        for value in node:
            references(value, out)


def check_fields():
    fields = model_fields()
    checked = 0
    for f in sorted(REPORT.rglob("*.json")):
        refs = set()
        references(load(f), refs)
        for entity, prop in sorted(refs):
            if entity not in fields:
                problem("fields", rel(f), f"table '{entity}' is not in the model")
            elif prop not in fields[entity]:
                problem("fields", rel(f), f"'{entity}'[{prop}] is not in the model")
        checked += 1
    return checked, sum(len(v) for v in fields.values())


# ---------------------------------------------------------------- 4. layout
ALLOWED_TOGETHER = {"textbox", "pageNavigator"}          # a page header holds its navigation buttons


def check_layout():
    pages = 0
    for page_dir in sorted((REPORT / "pages").iterdir()):
        if not page_dir.is_dir():
            continue
        name = load(page_dir / "page.json").get("displayName", page_dir.name)
        boxes = []
        for vf in sorted(page_dir.glob("visuals/*/visual.json")):
            v = load(vf)
            p = v["position"]
            vtype = v.get("visual", {}).get("visualType", "group")
            box = (vtype, p["x"], p["y"], p["width"], p["height"])
            if p["x"] < -0.5 or p["y"] < -0.5 or p["x"] + p["width"] > PAGE_W + 0.5 or p["y"] + p["height"] > PAGE_H + 0.5:
                problem("layout", name, f"{vtype} runs off the page")
            for other in boxes:
                if {box[0], other[0]} <= ALLOWED_TOGETHER and box[0] != other[0]:
                    continue
                ox = min(box[1] + box[3], other[1] + other[3]) - max(box[1], other[1])
                oy = min(box[2] + box[4], other[2] + other[4]) - max(box[2], other[2])
                if ox > 1 and oy > 1:
                    problem("layout", name, f"{box[0]} overlaps {other[0]}")
            boxes.append(box)
        pages += 1
    return pages


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--schemas", type=Path, required=True, help="checkout of github.com/microsoft/json-schemas")
    ap.add_argument("--theme-schema", type=Path, required=True, help="reportThemeSchema-*.json")
    a = ap.parse_args()

    n1 = check_structure(a.schemas)
    n2 = check_formatting(a.theme_schema)
    n3, n_fields = check_fields()
    n4 = check_layout()
    print(f"Structure:   {n1} report files checked against Microsoft's schemas")
    print(f"Formatting:  {n2} visuals checked against the theme schema")
    print(f"Fields:      {n3} files checked against {n_fields} model columns and measures")
    print(f"Layout:      {n4} pages checked for overlaps and edges")
    if problems:
        print(f"\n{len(problems)} problem(s):")
        for p in problems:
            print("  " + p)
        sys.exit(1)
    print("\nAll Power BI checks passed.")


if __name__ == "__main__":
    main()

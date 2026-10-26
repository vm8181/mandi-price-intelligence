"""
Checks the Databricks Asset Bundle (databricks.yml + jobs/*.yml) without signing in to Databricks.
Run in CI on every push.

    databricks bundle schema > bundle_schema.json
    python tools/check_bundle.py bundle_schema.json

Why not `databricks bundle validate`? It needs a signed-in workspace, and this public repository
keeps no Databricks token. This check covers what can be proved offline:

1. Every bundle file is valid against the official bundle schema from the Databricks CLI
   (catches misspelt or misplaced settings, wrong value types).
2. Every notebook a task runs exists in the repository.
3. Every task after the first depends on an existing task, so the run order is bronze -> silver -> gold.
4. The job pulls code from this repository's main branch.

Exit code 1 on any problem.
"""
import json
import sys
from pathlib import Path

import regex
import yaml
from jsonschema import Draft7Validator, Draft202012Validator, validators
from jsonschema.exceptions import ValidationError, best_match

ROOT = Path(__file__).resolve().parent.parent
REPO_URL = "https://github.com/vm8181/mandi-price-intelligence"
problems = []


def go_pattern(validator, pattern, instance, schema):
    """The bundle schema checks ${var.x}-style references with regular expressions written for Go
    (for example \\p{L}, any letter), which Python's re module can't compile. The `regex` package
    understands them, so patterns are checked with it instead."""
    if validator.is_type(instance, "string") and not regex.search(pattern, instance):
        yield ValidationError(f"{instance!r} does not match {pattern!r}")


def main():
    schema = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    base = Draft202012Validator if "2020" in str(schema.get("$schema", "")) else Draft7Validator
    validator = validators.extend(base, {"pattern": go_pattern})(schema)

    root_doc = yaml.safe_load((ROOT / "databricks.yml").read_text(encoding="utf-8"))
    files = [ROOT / "databricks.yml"]
    for pattern in root_doc.get("include", []):
        files += sorted(ROOT.glob(pattern))

    jobs = {}
    for f in files:
        doc = yaml.safe_load(f.read_text(encoding="utf-8"))
        errors = list(validator.iter_errors(doc))
        if errors:
            e = best_match(errors)
            problems.append(f"{f.relative_to(ROOT)}: {list(e.absolute_path)} {e.message[:200]}")
        jobs.update((doc.get("resources") or {}).get("jobs") or {})

    if not jobs:
        problems.append("no job defined in the bundle")
    for key, job in jobs.items():
        keys = [t["task_key"] for t in job.get("tasks", [])]
        for i, task in enumerate(job.get("tasks", [])):
            nb = task.get("notebook_task", {}).get("notebook_path")
            if nb and not (ROOT / f"{nb}.py").exists():
                problems.append(f"job {key}, task {task['task_key']}: notebook {nb}.py not in the repository")
            deps = [d["task_key"] for d in task.get("depends_on", [])]
            if i > 0 and not deps:
                problems.append(f"job {key}, task {task['task_key']}: runs without waiting for an earlier task")
            for d in deps:
                if d not in keys[:i]:
                    problems.append(f"job {key}, task {task['task_key']}: depends on unknown or later task {d}")
        git = job.get("git_source", {})
        if git and (git.get("git_url") != REPO_URL or git.get("git_branch") != "main"):
            problems.append(f"job {key}: code should come from {REPO_URL}, branch main")
        print(f"Job {key}: {' -> '.join(keys)}; schedule {job.get('schedule', {}).get('pause_status', 'not set')}")

    print(f"Checked {len(files)} bundle file(s) against the Databricks bundle schema.")
    if problems:
        print(f"\n{len(problems)} problem(s):")
        for p in problems:
            print("  " + p)
        sys.exit(1)
    print("Bundle checks passed.")


if __name__ == "__main__":
    main()

"""
Batch orchestration pipeline for the Skin Lesion Analytics project.

This script implements a lightweight DAG (Directed Acyclic Graph) runner
that executes the pipeline steps in dependency order:

    ingest  ──►  load_warehouse  ──►  dbt_transform
                                           │
                                    (all 4 dbt models)

Each task:
 - Has declared dependencies (runs only after its parents succeed).
 - Logs start/end/duration.
 - Fails fast: if any task fails, downstream tasks are skipped.
"""

import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent


@dataclass
class Task:
    name: str
    command: list[str]
    cwd: Path = PROJECT_DIR
    depends_on: list[str] = field(default_factory=list)
    status: str = "pending"  # pending | running | success | failed | skipped
    duration: float = 0.0


# ── Define the DAG ──────────────────────────────────────────────────
TASKS: list[Task] = [
    Task(
        name="ingest",
        command=[sys.executable, "pipeline/ingest.py"],
        depends_on=[],
    ),
    Task(
        name="load_warehouse",
        command=[sys.executable, "pipeline/load_warehouse.py"],
        depends_on=["ingest"],
    ),
    Task(
        name="dbt_transform",
        command=["dbt", "run", "--profiles-dir", "."],
        cwd=PROJECT_DIR / "dbt_skin_lesion",
        depends_on=["load_warehouse"],
    ),
    Task(
        name="dbt_test",
        command=["dbt", "test", "--profiles-dir", "."],
        cwd=PROJECT_DIR / "dbt_skin_lesion",
        depends_on=["dbt_transform"],
    ),
]


def _run_task(task: Task) -> bool:
    """Execute a single task. Returns True on success."""
    task.status = "running"
    print(f"\n{'='*60}")
    print(f"  TASK: {task.name}")
    print(f"  CMD:  {' '.join(task.command)}")
    print(f"{'='*60}")

    start = time.time()
    result = subprocess.run(task.command, cwd=task.cwd)
    task.duration = round(time.time() - start, 2)

    if result.returncode == 0:
        task.status = "success"
        print(f"  ✓ {task.name} completed in {task.duration}s")
        return True
    else:
        task.status = "failed"
        print(f"  ✗ {task.name} FAILED (exit {result.returncode}) after {task.duration}s")
        return False


def run_dag(tasks: list[Task]):
    """Execute tasks in topological (dependency) order."""
    task_map = {t.name: t for t in tasks}
    completed: set[str] = set()

    print("\n" + "=" * 60)
    print("  SKIN LESION ANALYTICS — BATCH PIPELINE")
    print("  DAG: ingest → load_warehouse → dbt_transform → dbt_test")
    print("=" * 60)

    pipeline_start = time.time()

    for task in tasks:
        # Check dependencies
        failed_deps = [
            d for d in task.depends_on if task_map[d].status == "failed"
        ]
        skipped_deps = [
            d for d in task.depends_on if task_map[d].status == "skipped"
        ]

        if failed_deps or skipped_deps:
            task.status = "skipped"
            print(f"\n  ⊘ Skipping {task.name} (dependency failed: {failed_deps + skipped_deps})")
            continue

        unmet = [d for d in task.depends_on if d not in completed]
        if unmet:
            task.status = "skipped"
            print(f"\n  ⊘ Skipping {task.name} (unmet dependencies: {unmet})")
            continue

        success = _run_task(task)
        if success:
            completed.add(task.name)

    total_time = round(time.time() - pipeline_start, 2)

    # ── Summary ───────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("  PIPELINE SUMMARY")
    print("=" * 60)
    for t in tasks:
        icon = {"success": "✓", "failed": "✗", "skipped": "⊘"}.get(t.status, "?")
        print(f"  {icon} {t.name:20s}  {t.status:8s}  ({t.duration}s)")
    print(f"\n  Total: {total_time}s")
    print("=" * 60)

    if any(t.status == "failed" for t in tasks):
        sys.exit(1)


if __name__ == "__main__":
    run_dag(TASKS)

#!/usr/bin/env python3
"""
inline_jdoc_behavior.py
=======================

Comment-only content fixer that makes ``jdoc.py validate`` green for two of its
issue reports, by editing the *example* comments (never the validator):

1. **Inline mutation post-state comments** (clears ``missing-behavior-comments``).
   The report flags an example CALL statement (a side-effecting call ending in
   ``;`` with no ``=``) that has no trailing ``// ...`` and no recognised
   follow-up behaviour line. For each flagged call this fixer gives the call a
   trailing comment so the post-state lives on the same example line -- the
   style the project already uses for short mutations and the one the task's
   style guide prescribes ("document the post-state in the same example line"):

     - if the next example line is a standalone ``// <behaviour>`` comment, that
       comment's text is moved inline onto the call line and the now-redundant
       comment line is dropped (the comment text is preserved, only relocated);
     - otherwise (the flagged calls with no follow-up are all null/empty no-op
       mutations whose next line is blank or ``}</pre>``) a trailing
       ``// no-op (input unchanged)`` is appended.

   The ``missing-behavior-comments`` report skips any call line that already
   carries ``//``, so either edit clears the finding.

2. **Strip the " (edge case)" parenthetical** from example label comments
   (clears ``placeholders``, which flags the literal phrase "edge case"). Only
   the parenthetical is removed; the descriptive label and its ``// returns ...``
   line are preserved.

Scope & safety:
  - Only lines inside Javadoc ``<pre>{@code ... }</pre>`` blocks are touched.
  - Flagged call lines are located with ``jdoc_tools.reports`` so transform 1
    edits *exactly* the sites the validator flags and nothing else.
  - A flagged call whose next line is neither a ``//`` comment nor blank/``</pre>``
    (e.g. two adjacent statements) is left untouched and reported, to avoid
    mislabeling a non-no-op call.
  - Dry-run by default; ``--apply`` writes. Per-file newline style is detected
    and preserved.

Usage:
    python scripts/inline_jdoc_behavior.py [--apply] PATH [PATH ...]
"""
from __future__ import annotations

import argparse
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from jdoc_tools import region, reports  # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

EDGE_CASE_RE = re.compile(r"\s*\(edge case\)")
COMMENT_LINE_RE = re.compile(r"^(\s*\*)\s*//\s?(.*)$")     # a standalone " * // ..." line
GUTTER_RE = re.compile(r"^(\s*\*)")
NOOP_COMMENT = "  // no-op (input unchanged)"
# A genuine no-op signal: the call passes a (possibly cast) null literal, e.g.
#   updateAll((boolean[]) null, ...)   replaceIf((int[]) null, ...)
# Side-effecting calls (forEach/accept/print/assert) do NOT match and are left
# for manual handling rather than mislabeled "no-op".
NULL_ARG_RE = re.compile(r"\(\s*(?:\([^()]*\)\s*)?null\s*[,)]")


def _flagged_call_indices(path: str, lines: list[str]) -> list[int]:
    """0-based line indices of calls flagged by missing-behavior-comments."""
    out = []
    for finding in reports.check_missing_behavior_comments(path, lines):
        # format: "<path>:<lineno>: <code>"   (lineno is 1-based)
        m = re.match(r"^.*:(\d+): ", finding)
        if m:
            out.append(int(m.group(1)) - 1)
    return out


def transform(path: str, lines: list[str]):
    """Return (new_lines, stats, skipped) without mutating the input list."""
    stats = {"edge_case_stripped": 0, "inlined": 0, "noop_added": 0}
    skipped: list[str] = []

    # ---- transform 2 first (content-only, no line-count change) ----
    code_idx = {i for i, _ in region.iter_code_lines(lines)}
    work = list(lines)
    for i in code_idx:
        if "(edge case)" in work[i]:
            new = EDGE_CASE_RE.sub("", work[i])
            if new != work[i]:
                work[i] = new
                stats["edge_case_stripped"] += 1

    # ---- transform 1 (uses flagged indices; may drop follow-up lines) ----
    flagged = _flagged_call_indices(path, work)
    drop = set()
    for idx in flagged:
        if idx in drop:
            continue
        call = work[idx].rstrip()
        nxt = work[idx + 1] if idx + 1 < len(work) else ""
        nxt_stripped = nxt.strip()
        m = COMMENT_LINE_RE.match(nxt)
        if m:                                    # move follow-up comment inline
            text = m.group(2).rstrip()
            work[idx] = f"{call}  // {text}"
            drop.add(idx + 1)
            stats["inlined"] += 1
        elif (nxt_stripped in ("*", "") or "}</pre>" in nxt_stripped) and NULL_ARG_RE.search(call):
            work[idx] = f"{call}{NOOP_COMMENT}"  # genuine null-argument no-op call
            stats["noop_added"] += 1
        else:
            reason = "next line not blank/comment" if nxt_stripped not in ("*", "") and "}</pre>" not in nxt_stripped \
                else "no follow-up and not a null no-op"
            skipped.append(f"{path}:{idx + 1}: {call.strip()}  ({reason})")

    new_lines = [ln for i, ln in enumerate(work) if i not in drop]
    return new_lines, stats, skipped


def process_file(path: str, apply: bool) -> int:
    with open(path, "r", encoding="utf-8", newline="") as fh:   # raw: keep CR so EOL is preserved
        text = fh.read()
    newline = region.detect_newline(text)
    lines = region.split_lines(text)
    new_lines, stats, skipped = transform(path, lines)
    changed = sum(stats.values())
    if changed or skipped:
        verb = "applied" if apply else "would apply"
        print(f"{path}: {verb} edge_case={stats['edge_case_stripped']} "
              f"inlined={stats['inlined']} noop={stats['noop_added']}"
              + (f"  SKIPPED={len(skipped)}" if skipped else ""))
        for s in skipped:
            print(f"  SKIP {s}")
    if changed and apply:
        with open(path, "w", encoding="utf-8", newline="") as fh:
            fh.write(region.join_lines(new_lines, newline))
    return changed


def iter_java_files(paths):
    for p in paths:
        if os.path.isdir(p):
            for root, _dirs, files in os.walk(p):
                if ".git" in root or "target" in root:
                    continue
                for name in sorted(files):
                    if name.endswith(".java") and name != "package-info.java":
                        yield os.path.join(root, name)
        else:
            yield p


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry-run)")
    ap.add_argument("paths", nargs="+", metavar="PATH")
    args = ap.parse_args(argv)

    total = 0
    for path in iter_java_files(args.paths):
        if os.path.isfile(path):
            total += process_file(path, args.apply)
    verb = "applied" if args.apply else "would apply"
    print(f"\n{verb} {total} edit(s).")
    return 1 if (total and not args.apply) else 0


if __name__ == "__main__":
    sys.exit(main())

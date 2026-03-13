"""
Local dry-run: clean a .lay file on disk and show what changed.
No Pennsieve API calls — just file I/O.

Usage:
    python clean_local.py path/to/file.lay
    python clean_local.py path/to/file.lay --words MRN,DOB,SSN
"""

import argparse
import shutil
import sys
import tempfile

from cleaners.lay_cleaner import LayCleaner


def main():
    parser = argparse.ArgumentParser(description="Clean PHI from a .lay file (local dry-run)")
    parser.add_argument("file", help="Path to the .lay file")
    parser.add_argument("--words", default="MRN,DOB", help="Comma-separated restricted words (default: MRN,DOB)")
    parser.add_argument("--in-place", action="store_true", help="Modify the file in place (default: work on a copy)")
    args = parser.parse_args()

    restricted_words = [w.strip() for w in args.words.split(",") if w.strip()]

    if args.in_place:
        target = args.file
    else:
        # Work on a copy so original is untouched
        tmpdir = tempfile.mkdtemp()
        target = shutil.copy(args.file, tmpdir)

    # Read original for comparison
    with open(args.file, "r", encoding="cp1252") as f:
        original_lines = f.readlines()

    cleaner = LayCleaner()
    modified = cleaner.clean(target, restricted_words)

    if not modified:
        print("No PHI found — file is clean.")
        return

    # Read cleaned version
    with open(target, "r", encoding="cp1252") as f:
        cleaned_lines = f.readlines()

    # Show what was removed
    original_set = set(original_lines)
    cleaned_set = set(cleaned_lines)
    removed = original_set - cleaned_set

    print(f"Removed {len(removed)} line(s) containing restricted words {restricted_words}:\n")
    for line in sorted(removed):
        print(f"  - {line.rstrip()}")

    if not args.in_place:
        print(f"\nCleaned copy written to: {target}")
        print("Original file was NOT modified. Use --in-place to modify directly.")
    else:
        print(f"\nFile modified in place: {target}")


if __name__ == "__main__":
    main()

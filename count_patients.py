"""
Count unique patients from the merged audit CSV.

A patient is identified by the root of new_name (stripping trailing _2, _3, etc.).

Usage:
    python count_patients.py
    python count_patients.py path/to/some_other.csv
"""

import csv
import re
import sys


def count_patients(csv_path):
    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        patients = {}
        total_rows = 0
        for row in reader:
            total_rows += 1
            name = row["new_name"]
            root = re.sub(r"_(\d+)$", "", name)
            patients[root] = patients.get(root, 0) + 1

    print(f"Total rows: {total_rows}")
    print(f"Unique patients: {len(patients)}")
    print(f"\nTop 10 by EEG count:")
    for name, count in sorted(patients.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  {name}: {count} EEGs")


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "output/audit/merged_all.csv"
    count_patients(path)

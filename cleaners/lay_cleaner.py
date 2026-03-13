import logging
import os
import re

from cleaners.base import FileCleaner

log = logging.getLogger()


class LayCleaner(FileCleaner):
    """Remove PHI from .lay file [Comments] sections.

    Reads the file as plain text (NOT configparser — .lay isn't valid INI).
    Within [Comments], removes any line whose text field contains a restricted
    word as a whole word (case-insensitive, word-boundary match). All other
    sections are untouched.

    Comment line format: timestamp,duration,flag1,flag2,text
    """

    def clean(self, file_path: str, restricted_words: list[str]) -> bool:
        with open(file_path, "r", encoding="cp1252") as f:
            lines = f.readlines()

        in_comments = False
        lines_to_remove = set()

        for i, line in enumerate(lines):
            stripped = line.strip()

            # Detect section headers
            if stripped.startswith("[") and stripped.endswith("]"):
                in_comments = stripped == "[Comments]"
                continue

            if not in_comments:
                continue

            # Comment line: timestamp,duration,flag1,flag2,text
            parts = stripped.split(",", 4)
            if len(parts) < 5:
                continue

            text = parts[4]

            for word in restricted_words:
                if re.search(r"\b" + re.escape(word) + r"\b", text, re.IGNORECASE):
                    lines_to_remove.add(i)
                    break

        if not lines_to_remove:
            return False

        log.info(f"Removing {len(lines_to_remove)} PHI line(s) from {os.path.basename(file_path)}")
        new_lines = [line for i, line in enumerate(lines) if i not in lines_to_remove]
        with open(file_path, "w", encoding="cp1252") as f:
            f.writelines(new_lines)

        return True

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

    def find_restricted_words_in_comments(self, file_path: str, restricted_words: list[str]) -> list[dict]:
        """Find restricted words only in [Comments] text fields.

        Returns a list of matches with keys:
        - line_number: 1-based line number in the file
        - words: restricted words found on the line
        - text: comment text field
        """
        with open(file_path, "r", encoding="cp1252") as f:
            lines = f.readlines()

        matches = []
        in_comments = False

        for i, line in enumerate(lines):
            stripped = line.strip()

            if stripped.startswith("[") and stripped.endswith("]"):
                in_comments = stripped == "[Comments]"
                continue

            if not in_comments:
                continue

            parts = stripped.split(",", 4)
            if len(parts) < 5:
                continue

            text = parts[4]
            found_words = []

            for word in restricted_words:
                if re.search(r"\b" + re.escape(word) + r"\b", text, re.IGNORECASE):
                    found_words.append(word)

            if found_words:
                matches.append({
                    "line_number": i + 1,
                    "words": found_words,
                    "text": text,
                })

        return matches

    def clean(self, file_path: str, restricted_words: list[str]) -> bool:
        matches = self.find_restricted_words_in_comments(file_path, restricted_words)
        if not matches:
            return False

        with open(file_path, "r", encoding="cp1252") as f:
            lines = f.readlines()

        lines_to_remove = {m["line_number"] - 1 for m in matches}

        log.info(f"Removing {len(lines_to_remove)} PHI line(s) from {os.path.basename(file_path)}")
        new_lines = [line for i, line in enumerate(lines) if i not in lines_to_remove]
        with open(file_path, "w", encoding="cp1252") as f:
            f.writelines(new_lines)

        return True

import os
import shutil
import tempfile

from cleaners.lay_cleaner import LayCleaner

EXAMPLE_LAY = os.path.join(os.path.dirname(__file__), "..", "example_lay", "ex1.lay")


def _make_temp_lay(content=None):
    """Copy ex1.lay (or custom content) to a temp file and return its path + cleanup dir."""
    tmpdir = tempfile.mkdtemp()
    dest = os.path.join(tmpdir, "test.lay")
    if content is not None:
        with open(dest, "w", encoding="cp1252") as f:
            f.write(content)
    else:
        shutil.copy(EXAMPLE_LAY, dest)
    return tmpdir, dest


def _read_cleaned(path):
    with open(path, "r", encoding="cp1252") as f:
        return f.read()


class TestLayCleaner:
    def test_removes_line_with_mrn_and_dob(self):
        """Line 273 in ex1.lay has 'MRN 1010269; DOB 12/15/1986' — should be removed."""
        tmpdir, path = _make_temp_lay()
        try:
            cleaner = LayCleaner()
            result = cleaner.clean(path, ["MRN", "DOB"])
            assert result is True

            cleaned = _read_cleaned(path)
            assert "MRN" not in cleaned
            assert "DOB" not in cleaned
            assert "Jamie, Ford" not in cleaned
        finally:
            shutil.rmtree(tmpdir)

    def test_case_insensitive_match(self):
        content = (
            "[FileInfo]\nFile=test.dat\n\n"
            "[Comments]\n"
            "0.000,0.000,0,131072,Patient mrn is 12345\n"
            "1.000,0.000,0,131072,Normal comment\n"
        )
        tmpdir, path = _make_temp_lay(content)
        try:
            cleaner = LayCleaner()
            result = cleaner.clean(path, ["MRN"])
            assert result is True

            cleaned = _read_cleaned(path)
            assert "Patient mrn is 12345" not in cleaned
            assert "Normal comment" in cleaned
        finally:
            shutil.rmtree(tmpdir)

    def test_no_phi_returns_false_file_unchanged(self):
        tmpdir, path = _make_temp_lay()
        try:
            with open(path, "rb") as f:
                original = f.read()

            cleaner = LayCleaner()
            result = cleaner.clean(path, ["NONEXISTENT_WORD"])
            assert result is False

            with open(path, "rb") as f:
                after = f.read()

            assert original == after
        finally:
            shutil.rmtree(tmpdir)

    def test_preserves_non_comments_sections(self):
        """All sections other than [Comments] should be byte-for-byte identical."""
        tmpdir, path = _make_temp_lay()
        try:
            with open(path, "r", encoding="cp1252") as f:
                original_lines = f.readlines()

            # Collect non-Comments lines from original
            original_other = []
            in_comments = False
            for line in original_lines:
                stripped = line.strip()
                if stripped.startswith("[") and stripped.endswith("]"):
                    in_comments = stripped == "[Comments]"
                    original_other.append(line)
                    continue
                if not in_comments:
                    original_other.append(line)

            cleaner = LayCleaner()
            cleaner.clean(path, ["MRN", "DOB"])

            with open(path, "r", encoding="cp1252") as f:
                cleaned_lines = f.readlines()

            cleaned_other = []
            in_comments = False
            for line in cleaned_lines:
                stripped = line.strip()
                if stripped.startswith("[") and stripped.endswith("]"):
                    in_comments = stripped == "[Comments]"
                    cleaned_other.append(line)
                    continue
                if not in_comments:
                    cleaned_other.append(line)

            assert original_other == cleaned_other
        finally:
            shutil.rmtree(tmpdir)

    def test_empty_comments_section(self):
        content = "[FileInfo]\nFile=test.dat\n\n[Comments]\n\n[Patient]\nFirst=Test\n"
        tmpdir, path = _make_temp_lay(content)
        try:
            cleaner = LayCleaner()
            result = cleaner.clean(path, ["MRN"])
            assert result is False
        finally:
            shutil.rmtree(tmpdir)

    def test_missing_comments_section(self):
        content = "[FileInfo]\nFile=test.dat\n\n[Patient]\nFirst=Test\n"
        tmpdir, path = _make_temp_lay(content)
        try:
            cleaner = LayCleaner()
            result = cleaner.clean(path, ["MRN"])
            assert result is False
        finally:
            shutil.rmtree(tmpdir)

    def test_removes_line_matching_any_restricted_word(self):
        """A line with DOB but not MRN should still be removed when both are restricted."""
        content = (
            "[Comments]\n"
            "0.000,0.000,0,131072,DOB 01/01/2000\n"
            "1.000,0.000,0,131072,Normal EEG\n"
            "2.000,0.000,0,131072,MRN 999\n"
        )
        tmpdir, path = _make_temp_lay(content)
        try:
            cleaner = LayCleaner()
            result = cleaner.clean(path, ["MRN", "DOB"])
            assert result is True

            cleaned = _read_cleaned(path)
            assert "DOB 01/01/2000" not in cleaned
            assert "MRN 999" not in cleaned
            assert "Normal EEG" in cleaned
        finally:
            shutil.rmtree(tmpdir)

    def test_comments_at_end_of_file_no_trailing_section(self):
        """[Comments] is the last section (no closing bracket) — like ex1.lay."""
        content = (
            "[FileInfo]\nFile=test.dat\n\n"
            "[Comments]\n"
            "0.000,0.000,0,131072,Safe line\n"
            "1.000,0.000,0,131072,Has MRN 123\n"
        )
        tmpdir, path = _make_temp_lay(content)
        try:
            cleaner = LayCleaner()
            result = cleaner.clean(path, ["MRN"])
            assert result is True

            cleaned = _read_cleaned(path)
            assert "Has MRN 123" not in cleaned
            assert "Safe line" in cleaned
        finally:
            shutil.rmtree(tmpdir)

    def test_word_boundary_matching(self):
        """'ID' should match standalone 'ID' but not 'bedside' or 'video'."""
        content = (
            "[Comments]\n"
            "0.000,0.000,0,131072,rn at bedside\n"
            "1.000,0.000,0,131072,Patient ID 12345\n"
            "2.000,0.000,0,131072,video recording\n"
        )
        tmpdir, path = _make_temp_lay(content)
        try:
            cleaner = LayCleaner()
            result = cleaner.clean(path, ["ID"])
            assert result is True

            cleaned = _read_cleaned(path)
            assert "bedside" in cleaned
            assert "video recording" in cleaned
            assert "Patient ID 12345" not in cleaned
        finally:
            shutil.rmtree(tmpdir)

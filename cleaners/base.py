from abc import ABC, abstractmethod


class FileCleaner(ABC):
    @abstractmethod
    def clean(self, file_path: str, restricted_words: list[str]) -> bool:
        """Clean file in-place. Return True if modified."""

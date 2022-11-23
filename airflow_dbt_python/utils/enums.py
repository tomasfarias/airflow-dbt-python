"""Enumerations with allowed set of values."""
from enum import Enum


class FromStrEnum(Enum):
    """Access enum variants with strings ensuring uppercase."""

    @classmethod
    def from_str(cls, s: str):
        """Instantiate an Enum from a string."""
        return cls[s.replace("-", "_").upper()]


class LogFormat(FromStrEnum):
    """Allowed dbt log formats."""

    DEFAULT = "default"
    JSON = "json"
    TEXT = "text"


class Output(FromStrEnum):
    """Allowed output arguments."""

    JSON = "json"
    NAME = "name"
    PATH = "path"
    SELECTOR = "selector"

    def __eq__(self, other):
        """Override equality for string comparison."""
        if isinstance(other, str):
            return other.upper() == self.name
        return Enum.__eq__(self, other)

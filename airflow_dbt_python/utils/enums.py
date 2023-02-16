"""Enumeration subclasses used in dbt configurations."""
from enum import Enum
from typing import Type, TypeVar

FromStrEnumSelf = TypeVar("FromStrEnumSelf", bound="FromStrEnum")


class FromStrEnum(Enum):
    """Access enum variants with strings ensuring uppercase."""

    @classmethod
    def from_str(cls: Type[FromStrEnumSelf], name: str) -> FromStrEnumSelf:
        """Instantiate an Enum from a string."""
        return cls[name.replace("-", "_").upper()]

    def __eq__(self, other) -> bool:
        """Override equality for string comparison."""
        if isinstance(other, str):
            return other.upper() == self.name
        return Enum.__eq__(self, other)


class LogFormat(str, Enum):
    """Allowed dbt log formats."""

    DEFAULT = "default"
    JSON = "json"
    TEXT = "text"


class Output(str, Enum):
    """Allowed output arguments."""

    JSON = "json"
    NAME = "name"
    PATH = "path"
    SELECTOR = "selector"

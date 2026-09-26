"""Attribute mapping configuration and transform chains."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

ATTRS = ("displayName", "email", "department", "jobTitle")

# A transform chain is a tuple of ops applied left to right:
#   "upper"            -> value.upper()
#   "trim"             -> value.strip()
#   ("default", v)     -> v if value == "" else value


def normalize_chain(chain) -> tuple:
    if chain is None:
        return ()
    out = []
    for op in chain:
        if isinstance(op, str):
            if op not in ("upper", "trim"):
                raise ValueError(f"unknown transform {op!r}")
            out.append(op)
        else:
            name, arg = op
            if name != "default":
                raise ValueError(f"unknown transform {name!r}")
            if not isinstance(arg, str):
                raise TypeError("default value must be a string")
            out.append(("default", arg))
    return tuple(out)


def apply_chain(value: str, chain) -> str:
    for op in chain:
        if op == "upper":
            value = value.upper()
        elif op == "trim":
            value = value.strip()
        elif op[0] == "default":
            if value == "":
                value = op[1]
        else:  # pragma: no cover - normalize_chain guards this
            raise ValueError(f"unknown transform {op!r}")
    return value


@dataclass(frozen=True)
class FieldMapping:
    """One connector field maps to at most one central attribute.
    inbound/outbound are normalized transform chains; None disables that
    direction."""
    attr: str
    inbound: Optional[tuple] = None
    outbound: Optional[tuple] = None

    def __post_init__(self):
        if self.attr not in ATTRS:
            raise ValueError(f"unknown attribute {self.attr!r}")
        object.__setattr__(self, "inbound",
                           None if self.inbound is None else normalize_chain(self.inbound))
        object.__setattr__(self, "outbound",
                           None if self.outbound is None else normalize_chain(self.outbound))

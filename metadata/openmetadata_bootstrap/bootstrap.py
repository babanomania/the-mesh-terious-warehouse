"""Utilities for bootstrapping OpenMetadata with default glossary terms."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import List


@dataclass
class GlossaryTerm:
    """Represents a single glossary term."""

    name: str
    description: str


@dataclass
class Glossary:
    """Container for a glossary and its terms."""

    name: str
    description: str
    terms: List[GlossaryTerm] = field(default_factory=list)


def load_default_glossary(glossary_path: Path | None = None) -> Glossary:
    """Load the default glossary definition.

    Parameters
    ----------
    glossary_path:
        Optional path to a JSON file containing the glossary definition. If not
        provided, ``glossary.json`` within the package directory is used.

    Returns
    -------
    Glossary
        Parsed glossary instance with all terms loaded.
    """

    if glossary_path is None:
        glossary_path = Path(__file__).with_name("glossary.json")

    with open(glossary_path, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    glossary_data = data.get("glossary", {})
    terms = [GlossaryTerm(**term) for term in glossary_data.get("terms", [])]

    return Glossary(
        name=glossary_data.get("name", ""),
        description=glossary_data.get("description", ""),
        terms=terms,
    )

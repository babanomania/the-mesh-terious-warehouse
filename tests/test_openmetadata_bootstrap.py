import importlib.util
from pathlib import Path
import sys


def _load_bootstrap_module():
    bootstrap_path = (
        Path(__file__).resolve().parents[1]
        / "metadata"
        / "openmetadata_bootstrap"
        / "bootstrap.py"
    )
    spec = importlib.util.spec_from_file_location("bootstrap", bootstrap_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_load_default_glossary():
    module = _load_bootstrap_module()
    glossary = module.load_default_glossary()
    assert glossary.name == "default_glossary"
    assert glossary.description
    term_names = {term.name for term in glossary.terms}
    assert {"SLA", "Restock", "Forecast"}.issubset(term_names)


def test_custom_path(tmp_path):
    module = _load_bootstrap_module()
    # Create a temporary glossary file and ensure loader can read it
    data = {
        "glossary": {
            "name": "temp",
            "description": "tmp glossary",
            "terms": [{"name": "Example", "description": "tmp"}],
        }
    }
    file_path = tmp_path / "custom.json"
    file_path.write_text(__import__("json").dumps(data))

    glossary = module.load_default_glossary(file_path)
    assert glossary.name == "temp"
    assert glossary.terms[0].name == "Example"

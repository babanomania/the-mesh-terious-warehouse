def _generate_sequences():
    import importlib.util
    from pathlib import Path

    BASE_GENERATOR_PATH = (
        Path(__file__).resolve().parents[1] / "ingestion" / "producers" / "base_generator.py"
    )
    spec = importlib.util.spec_from_file_location("base_generator", BASE_GENERATOR_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    BaseGenerator = module.BaseGenerator
    gen = BaseGenerator("test_seed")
    seq = [gen.randint(1, 100) for _ in range(5)]
    choices = [gen.choice(["a", "b", "c"]) for _ in range(5)]
    return seq, choices


def test_seeded_randomness_is_reproducible():
    seq1, choices1 = _generate_sequences()
    seq2, choices2 = _generate_sequences()
    assert seq1 == seq2
    assert choices1 == choices2

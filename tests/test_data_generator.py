"""
Tests for Synthetic Data Generator
"""

from src.data_generator import SyntheticDataGenerator, generate_starter_dataset


def test_generator_determinism_with_seed():
    """Verifies that identical seed produces byte-for-byte identical datasets."""
    gen1 = SyntheticDataGenerator(seed=123, days_span=5)
    df1, meta1 = gen1.generate_events(total_rows=100)

    gen2 = SyntheticDataGenerator(seed=123, days_span=5)
    df2, meta2 = gen2.generate_events(total_rows=100)

    assert df1.equals(df2)
    assert meta1 == meta2


def test_generator_contains_all_event_types():
    """Verifies that generated stream includes CREATE, UPDATE, CANCEL, and REVERSAL."""
    gen = SyntheticDataGenerator(seed=999, days_span=7)
    df, meta = gen.generate_events(total_rows=300)

    types = set(df["event_type"].unique())
    assert "CREATE" in types
    assert "UPDATE" in types
    assert "CANCEL" in types
    assert "REVERSAL" in types


def test_starter_dataset_generation(tmp_path):
    """Verifies starter dataset helper produces valid sample file within 100-500 row requirement."""
    out_file = tmp_path / "test_starter.csv"
    saved_path, df, meta = generate_starter_dataset(output_path=out_file, seed=42)

    assert saved_path.exists()
    assert 100 <= len(df) <= 500
    assert meta["duplicates_injected"] > 0
    assert meta["invalid_injected"] > 0

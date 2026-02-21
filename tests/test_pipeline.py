import pytest
import pandas as pd

from src.pipeline.ingest.normalize import _normalize_data
from src.pipeline.ingest.validate import _validate_schema, _validate_data_quality

# --- VALIDATION TESTS ---

def test_validate_schema_passes_with_correct_keys():
    """Test that schema validation passes when all required keys are present."""
    valid_data = {
        "latitude": 42.36, "longitude": -71.06,
        "generationtime_ms": 0.5, "utc_offset_seconds": -18000,
        "timezone": "America/New_York", "timezone_abbreviation": "EST",
        "elevation": 10.0, "hourly_units": {}, "hourly": {}
    }
    assert _validate_schema(valid_data) is True

def test_validate_schema_fails_missing_keys():
    """Test that schema validation raises an error if keys are missing."""
    invalid_data = {"latitude": 42.36}
    
    with pytest.raises(ValueError, match="missing required keys"):
        _validate_schema(invalid_data)

def test_validate_data_quality_detects_duplicates():
    """Test that quality validation catches duplicate natural keys (location + time)."""
    duplicate_data = {
        "latitude": 42.36,
        "longitude": -71.06,
        "hourly": {
            "time": ["2026-01-25T12:00:00", "2026-01-25T12:00:00"],
            "temperature_2m": [72.5, 72.5]
        }
    }
    
    with pytest.raises(ValueError, match="duplicates on natural key"):
        _validate_data_quality(duplicate_data)

def test_validate_data_quality_passes_clean_data():
    """Test that quality validation passes with clean, non-duplicate data."""
    clean_data = {
        "latitude": 42.36,
        "longitude": -71.06,
        "hourly": {
            "time": ["2026-01-25T12:00:00", "2026-01-25T13:00:00"],
            "temperature_2m": [35.5, 36.0]
        }
    }
    assert _validate_data_quality(clean_data) is True

def test_validate_data_quality_rejects_empty_hourly():
    """Test that empty hourly data raises ValueError."""
    empty_data = {
        "latitude": 42.36,
        "longitude": -71.06,
        "hourly": {
            "time": [],
            "temperature_2m": []
        }
    }
    with pytest.raises(ValueError, match="Dataset is empty"):
        _validate_data_quality(empty_data)

# --- NORMALIZATION TESTS ---

def test_normalize_data_formatting():
    """Test that normalization correctly flattens JSON, adds location, and parses dates."""
    bronze_data = {
        "latitude": 42.36,
        "longitude": -71.06,
        "hourly": {
            "time": ["2026-01-25T12:00:00", "2026-01-25T13:00:00"],
            "temperature_2m": [35.5, 36.0]
        }
    }
    
    df = _normalize_data(bronze_data)
    
    assert isinstance(df, pd.DataFrame)
    
    assert "latitude" in df.columns
    assert df["latitude"].iloc[0] == 42.36
    
    assert pd.api.types.is_datetime64_any_dtype(df["time"])
    
    assert len(df) == 2
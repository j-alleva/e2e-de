from src.pipeline.config import Project_Config

def test_bronze_path_generation():
    """
    Verifies that the bronze_path method constructs the correct directory structure
    based on the source and run_date.
    """
    source = "openmeteo"
    run_date = "2026-02-10"

    # Mock base path to ensure the test is deterministic
    original_path = Project_Config.Paths.LOCAL_BRONZE
    Project_Config.Paths.LOCAL_BRONZE = "/tmp/bronze"

    try:

        result = Project_Config.Paths.bronze_path(source, run_date)

        expected = "/tmp/bronze/source=openmeteo/run_date=2026-02-10"
        assert result == expected
    
    finally:
        # Reset config to ensure future test compatibility
        Project_Config.Paths.LOCAL_BRONZE = original_path

def test_location_lookup_exists():
    """
    Verifies that 'Boston' is a configured location.
    """
    assert "Boston" in Project_Config.LOCATION_LOOKUP
    assert Project_Config.LOCATION_LOOKUP["Boston"]["latitude"] == 42.3601

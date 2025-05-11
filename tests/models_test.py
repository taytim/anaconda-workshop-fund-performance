import pytest
from pydantic import ValidationError
from models.config import Config


def test_valid_config():
    """Test valid config"""
    valid_config = {
        "database_path": "value1",
        "database_setup_scripts_path": "value2",
        "input_data_path": "value3",
        "output_data_path": "value4",
    }
    config = Config(**valid_config)
    assert config.database_path == valid_config["database_path"]
    assert (
        config.database_setup_scripts_path
        == valid_config["database_setup_scripts_path"]
    )


def test_missing_attribute():
    """Test missing attribute in config"""
    missing_attr_config = {"database_path": "value1"}
    with pytest.raises(ValidationError):
        Config(**missing_attr_config)


def test_invalid_attribute_type():
    """Test invalid attribute type in config"""
    invalid_attr_type_config = {
        "database_path": "value1",
        "database_setup_scripts_path": 123,
        "input_data_path": "value3",
        "output_data_path": "value4",
    }
    with pytest.raises(ValidationError):
        Config(**invalid_attr_type_config)

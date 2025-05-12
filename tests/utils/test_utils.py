import pytest
import logging
import sqlite3
from models.config import Config
from utils.utils import Utils
import yaml
from unittest.mock import patch, mock_open, MagicMock


def test_load_config_success():
    mock_config_data = {
        "database_path": "/fake/path/db.sqlite",
        "database_setup_scripts_path": "/fake/path/scripts",
        "input_data_path": "/fake/path/input",
        "output_data_path": "/fake/path/output",
    }
    yaml_str = yaml.dump(mock_config_data)
    with patch("builtins.open", mock_open(read_data=yaml_str)):
        with patch("yaml.safe_load", return_value=mock_config_data):
            config = Utils.load_config("fake/path/config.yml")
    assert isinstance(config, Config)
    assert config.database_path == "/fake/path/db.sqlite"
    assert config.database_setup_scripts_path == "/fake/path/scripts"


def test_load_config_file_not_found():
    """Test loading config file that does not exist."""
    with patch("builtins.open", side_effect=FileNotFoundError):
        with pytest.raises(FileNotFoundError):
            Utils.load_config("nonexistent.yml")


def test_load_config_invalid_yaml():
    """Test loading config file with invalid YAML."""
    with (
        patch("builtins.open", mock_open(read_data=":invalid_yaml")),
        patch("yaml.safe_load", side_effect=yaml.YAMLError),
    ):
        with pytest.raises(yaml.YAMLError):
            Utils.load_config("bad.yml")


def test_setup_logger():
    """Test logger setup."""
    logger = logging.getLogger()
    logger.handlers.clear()
    logger = Utils.setup_logger()
    assert isinstance(logger, logging.Logger)
    assert logger.level == logging.INFO
    assert logger.hasHandlers()
    formatter = logger.handlers[0].formatter
    assert formatter._fmt == "%(asctime)s - %(levelname)s - %(filename)s - %(message)s"


def test_connect_to_database():
    """Test connecting to database."""
    mock_conn = MagicMock(spec=sqlite3.Connection)
    with (
        patch("sqlite3.connect", return_value=mock_conn) as mock_connect,
        patch("logging.getLogger") as mock_logger,
    ):
        conn = Utils.connect_to_database("/fake/db/path")
    mock_connect.assert_called_once_with("/fake/db/path")
    mock_logger().info.assert_called_once_with(
        "Connected to the database at /fake/db/path"
    )
    assert conn == mock_conn

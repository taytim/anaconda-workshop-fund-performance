from models.config import Config
from setup_database import get_db_config, initialise_database
from utils.utils import Utils
from unittest.mock import patch, MagicMock, mock_open


@patch.object(Utils, "load_config")
def test_get_db_config(mock_load_config):
    """Test get_db_config function with valid config."""
    mock_config_data = {
        "database_path": "/fake/path/db.sqlite",
        "database_setup_scripts_path": "/fake/path/scripts",
    }
    mock_load_config.return_value = Config(**mock_config_data)
    database_path, database_setup_scripts_path = get_db_config()
    assert database_path == "/fake/path/db.sqlite"
    assert database_setup_scripts_path == "/fake/path/scripts"
    mock_load_config.assert_called_once()


@patch("os.path.exists", return_value=False)
@patch("os.listdir", return_value=["script1.sql", "script2.sql"])
@patch("builtins.open", new_callable=mock_open)
def test_initialise_database(mock_open, mock_listdir, mock_exists):
    """Test the initialise_database function when the database does not exist."""
    mock_connect_to_database = MagicMock()
    mock_logger = MagicMock()
    with (
        patch("logging.getLogger", return_value=mock_logger),
        patch.object(
            Utils, "connect_to_database", return_value=mock_connect_to_database
        ),
    ):
        mock_open.return_value.read.side_effect = [
            "CREATE TABLE test_table;",
            "INSERT INTO test_table (id) VALUES (1);",
        ]
        initialise_database("/fake/path/db.sqlite", "/fake/path/scripts")
    mock_exists.assert_called_once_with("/fake/path/db.sqlite")
    mock_listdir.assert_called_once_with("/fake/path/scripts")
    mock_open.assert_any_call("/fake/path/scripts\\script1.sql", "r")
    mock_open.assert_any_call("/fake/path/scripts\\script2.sql", "r")
    assert mock_open.call_count == 2
    mock_connect_to_database.executescript.assert_any_call("CREATE TABLE test_table;")
    mock_connect_to_database.executescript.assert_any_call(
        "INSERT INTO test_table (id) VALUES (1);"
    )
    assert mock_connect_to_database.executescript.call_count == 2
    mock_logger.info.assert_called_once_with("Database created at /fake/path/db.sqlite")


@patch("os.path.exists", return_value=True)
def test_initialise_database_db_exists(mock_exists):
    """Test the initialise_database function when the database already exists."""
    mock_logger = MagicMock()
    with patch("logging.getLogger", return_value=mock_logger):
        initialise_database("/fake/path/db.sqlite", "/fake/path/scripts")
    mock_exists.assert_called_once_with("/fake/path/db.sqlite")
    mock_logger.info.assert_called_once_with(
        "Database already exists at /fake/path/db.sqlite"
    )

from configs.constants import TableNames
from load_fund_data import (
    get_config_n_constant,
    clean_fund_name,
    clean_date,
    parse_filename,
    pandera_to_polars_dtype,
    append_metadata,
    load_data_to_db,
    process_fund_data,
    main,
)
import os
import pandera as pa
import polars as pl
import pytest
import tempfile
from utils.utils import Utils
from unittest.mock import patch, MagicMock


@pytest.fixture
def mock_fund_holdings():
    with patch.object(TableNames, "FUND_HOLDINGS", "mocked_fund_holdings"):
        yield


@patch.object(Utils, "load_config")
def test_get_config_n_constant(mock_load_config, mock_fund_holdings):
    """Test get_config_n_constant function."""
    mock_load_config.return_value = type(
        "Config",
        (),
        {"database_path": "path/to/db.sqlite", "input_data_path": "path/to/data"},
    )
    database_path, input_data_path, fund_holdings_table_name = get_config_n_constant()
    assert database_path == "path/to/db.sqlite"
    assert input_data_path == "path/to/data"
    assert fund_holdings_table_name == "mocked_fund_holdings"


@pytest.mark.parametrize(
    "fund_name_raw, expected_cleaned_name",
    [
        ("TT_monthly_Trustmind", "Trustmind"),
        ("rpt-Catalysm", "Catalysm"),
        ("Fund Whitestone", "Whitestone"),
        ("Report-of-Gohen", "Gohen"),
        ("mend-report Wallington", "Wallington"),
        ("Applebead", "Applebead"),
        (" ", None),
        ("!@#$%^&*()", None),
    ],
)
def test_clean_fund_name(fund_name_raw, expected_cleaned_name):
    """Test clean_fund_name function with various fund name formats."""
    cleaned_name = clean_fund_name(fund_name_raw)
    assert cleaned_name == expected_cleaned_name


@pytest.mark.parametrize(
    "date_raw, expected_cleaned_date",
    [
        ("12-05-2025", "2025-05-12"),
        ("05-12-2025", "2025-12-05"),
        ("12_05_2025", "2025-05-12"),
        ("2025-05-12", "2025-05-12"),
        ("20250512", "2025-05-12"),
        ("12/05/2025", None),
        ("05/12/2025", None),
        ("2025/05/12", None),
        ("invalid_date", None),
        (" ", None),
        ("!@#$%^&*()", None),
    ],
)
def test_clean_date(date_raw, expected_cleaned_date):
    """Test clean_date function with various date formats."""
    cleaned_date = clean_date(date_raw)
    assert cleaned_date == expected_cleaned_date


@patch("load_fund_data.clean_fund_name")
@patch("load_fund_data.clean_date")
def test_parse_filename_valid(mock_clean_date, mock_clean_fund_name):
    """Test parse_filename function with valid filename."""
    mock_clean_fund_name.return_value = "FundName"
    mock_clean_date.return_value = "2025-05-12"

    filename = "FundName.20250512.csv"
    fund_name, date = parse_filename(filename)

    assert fund_name == "FundName"
    assert date == "2025-05-12"


@patch("load_fund_data.clean_fund_name")
@patch("load_fund_data.clean_date")
def test_parse_filename_invalid(mock_clean_date, mock_clean_fund_name):
    """Test parse_filename function with invalid fund name and date."""
    mock_clean_fund_name.return_value = None
    mock_clean_date.return_value = None
    with pytest.raises(ValueError):
        parse_filename("!@#.InvalidDate.csv")


def test_parse_filename_invalid_format_assertion():
    """Test parse_filename function with invalid filename format not consistent with fund.date.csv."""
    with pytest.raises(AssertionError, match="Invalid filename format"):
        parse_filename("InvalidFilename.csv")


def test_pandera_to_polars_dtype():
    """Test pandera_to_polars_dtype conversion."""
    assert pandera_to_polars_dtype(pa.String()) == pl.String
    assert pandera_to_polars_dtype(pa.Float()) == pl.Float64
    assert pandera_to_polars_dtype(pa.Int()) == pl.Int64
    assert pandera_to_polars_dtype(pa.Bool()) == pl.Boolean
    assert pandera_to_polars_dtype(pa.DateTime()) == pl.Datetime
    assert pandera_to_polars_dtype(pa.Object()) == pl.String


def test_append_metadata_adds_fund_and_date():
    """Test append_metadata function to ensure it adds fund name and date, as well as convert column name to screaming snake case."""
    input_df = pl.DataFrame(
        {
            "FINANCIAL TYPE": ["Equity"],
            "SYMBOL": ["AAPL"],
            "SECURITY_NAME": ["Apple Inc."],
            "SEDOL": [None],
            "ISIN": ["US0378331005"],
            "PRICE": [150.25],
            "QUANTITY": [10.0],
            "REALISED P/L": [50.0],
            "MARKET VALUE": [1502.5],
        }
    )
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv") as tmp_file:
        input_df.write_csv(tmp_file.name)
        fund_name = "FundA"
        date = "2025-05-12"
        result_df = append_metadata(tmp_file.name, fund_name, date)
    assert "FUND" in result_df.columns
    assert "DATE" in result_df.columns
    assert "FINANCIAL_TYPE" in result_df.columns
    assert "SYMBOL" in result_df.columns
    assert "SECURITY_NAME" in result_df.columns
    assert "SEDOL" in result_df.columns
    assert "ISIN" in result_df.columns
    assert "PRICE" in result_df.columns
    assert "QUANTITY" in result_df.columns
    assert "REALISED_P_L" in result_df.columns
    assert "MARKET_VALUE" in result_df.columns
    assert all(result_df["FUND"] == fund_name)
    assert all(result_df["DATE"] == date)


def test_load_data_to_db_successful():
    """Test successful load_data_to_db function."""
    mock_logger = MagicMock()
    df = MagicMock(spec=pl.DataFrame)
    database_path = "path/to/db.sqlite"
    table_name = "fund_holdings"
    with patch("logging.getLogger", return_value=mock_logger):
        load_data_to_db(database_path, table_name, df)
    expected_connection_string = f"sqlite:///{os.path.abspath(database_path)}"
    df.write_database.assert_called_once_with(
        connection=expected_connection_string,
        table_name="fund_holdings",
        if_table_exists="replace",
    )


def test_load_data_to_db_failure():
    """Test failure scenario for load_data_to_db function."""
    mock_logger = MagicMock()
    df = MagicMock(spec=pl.DataFrame)
    df.write_database.side_effect = Exception("Database write failed")
    database_path = "path/to/db.sqlite"
    table_name = "fund_holdings"
    with (
        pytest.raises(Exception, match="Database write failed"),
        patch("logging.getLogger", return_value=mock_logger),
    ):
        load_data_to_db(database_path, table_name, df)
    mock_logger.error.assert_called_once_with(
        "Error loading data to database: Database write failed"
    )


@patch("os.path.isfile", return_value=True)
@patch("os.listdir", return_value=["FundName.20250512.csv"])
@patch("load_fund_data.append_metadata")
@patch("load_fund_data.parse_filename")
@patch("polars.concat")
@patch("models.external_fund.ExternalFund.validate")
@patch("load_fund_data.load_data_to_db")
def test_process_fund_data(
    mock_load_data_to_db,
    mock_validate,
    mock_concat,
    mock_parse_filename,
    mock_append_metadata,
    mock_listdir,
    mock_isfile,
):
    """Test process_fund_data function."""
    mock_parse_filename.return_value = ("FundName", "2025-05-12")
    mock_concat.return_value = MagicMock(spec=pl.DataFrame)
    mock_append_metadata.return_value = MagicMock(spec=pl.DataFrame)
    database_path = "path/to/db.sqlite"
    input_data_path = "path/to/data"
    fund_holdings_table_name = "fund_holdings"
    process_fund_data(database_path, input_data_path, fund_holdings_table_name)
    mock_append_metadata.assert_called_once()
    mock_concat.assert_called_once()
    mock_validate.assert_called_once()
    mock_listdir.assert_called_once_with(input_data_path)
    mock_isfile.assert_called_once()
    mock_load_data_to_db.assert_called_once()


@patch("load_fund_data.process_fund_data")
@patch(
    "load_fund_data.get_config_n_constant",
    return_value=("path/to/db.sqlite", "path/to/data", "fund_holdings"),
)
@patch("load_fund_data.Utils.setup_logger")
def test_main(mock_setup_logger, mock_get_config, mock_process_fund_data):
    """Test main function."""
    mock_logger = MagicMock()
    mock_setup_logger.return_value = mock_logger
    main()
    mock_process_fund_data.assert_called_once_with(
        "path/to/db.sqlite", "path/to/data", "fund_holdings"
    )
    mock_get_config.assert_called_once()
    mock_logger.info.assert_called_with("Fund data processing completed.")

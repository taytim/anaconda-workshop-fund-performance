import polars as pl
import pytest
import pandera as pa
from models.external_fund import ExternalFund


def test_valid_external_fund():
    """Test valid external fund data."""
    mock_valid_df = pl.DataFrame(
        {
            "FUND": ["Fund A"],
            "DATE": ["2025-05-01"],
            "FINANCIAL_TYPE": ["Equity"],
            "SYMBOL": ["AAPL"],
            "SECURITY_NAME": ["Apple Inc."],
            "SEDOL": pl.Series([None], dtype=pl.String),
            "ISIN": pl.Series([None], dtype=pl.String),
            "PRICE": [150.25],
            "QUANTITY": [10.123],
            "REALISED_P_L": [100.0],
            "MARKET_VALUE": [1502.5],
        }
    )
    validated_df = ExternalFund.validate(mock_valid_df)
    assert isinstance(validated_df, pl.DataFrame)


def test_missing_column():
    """Test missing columns."""
    mock_missing_column_df = pl.DataFrame(
        {
            "DATE": ["2025-05-01"],
            "FINANCIAL_TYPE": ["Equity"],
            "SYMBOL": ["AAPL"],
            "SECURITY_NAME": ["Apple Inc."],
        }
    )

    with pytest.raises(pa.errors.SchemaError):
        ExternalFund.validate(mock_missing_column_df)


def test_invalid_data_type():
    """Test invalid data type."""
    mock_invalid_data_type_df = pl.DataFrame(
        {
            "FUND": ["Fund A"],
            "DATE": ["2025-05-01"],
            "FINANCIAL_TYPE": ["Equity"],
            "SYMBOL": ["AAPL"],
            "SECURITY_NAME": ["Apple Inc."],
            "PRICE": ["not_a_number"],
            "QUANTITY": [10],
            "REALISED_P_L": [100.0],
            "MARKET_VALUE": [1502.5],
            "SEDOL": [None],
            "ISIN": [None],
        }
    )
    with pytest.raises(pa.errors.SchemaError):
        ExternalFund.validate(mock_invalid_data_type_df)


def test_null_in_non_nullable_field():
    """Test null value in a non-nullable field."""
    mock_null_df = pl.DataFrame(
        {
            "FUND": [None],
            "DATE": ["2025-05-01"],
            "FINANCIAL_TYPE": ["Equity"],
            "SYMBOL": ["AAPL"],
            "SECURITY_NAME": ["Apple Inc."],
            "PRICE": [150.25],
            "QUANTITY": [10],
            "REALISED_P_L": [100.0],
            "MARKET_VALUE": [1502.5],
            "SEDOL": [None],
            "ISIN": [None],
        }
    )

    with pytest.raises(pa.errors.SchemaError):
        ExternalFund.validate(mock_null_df)

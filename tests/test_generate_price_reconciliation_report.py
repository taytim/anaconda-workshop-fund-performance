import polars as pl
from polars.testing import assert_frame_equal
from generate_price_reconciliation_report import (
    get_config_n_constant,
    get_bond_prices,
    perform_price_reconciliation,
)
from utils.utils import Utils


def test_price_reconciliation():
    """Test perform_price_reconciliation function by comparing data to ensure a match."""
    mock_price_reconciliation_report = pl.read_csv(
        "tests/mock_data/price_reconciliation_report.csv"
    )
    (
        database_path,
        _,
        fund_holdings_table_name,
        bond_prices_table_name,
        bond_reference_table_name,
        equity_prices_table_name,
    ) = get_config_n_constant()
    conn = Utils.connect_to_database(database_path)
    bond_prices_df = get_bond_prices(
        conn, bond_prices_table_name, bond_reference_table_name
    )
    fund_holdings_df = Utils.get_data_from_database(conn, fund_holdings_table_name)
    equity_prices_df = Utils.get_data_from_database(conn, equity_prices_table_name)
    price_reconciliation_report = perform_price_reconciliation(
        fund_holdings_df, bond_prices_df, equity_prices_df
    )
    expected_cols = {
        "FUND",
        "DATE",
        "FINANCIAL_TYPE",
        "SYMBOL",
        "SECURITY_NAME",
        "ISIN",
        "SEDOL",
        "FUND_PRICE",
        "REF_PRICE",
        "PRICE_DIFF",
    }
    assert set(expected_cols) == set(price_reconciliation_report.columns)
    assert_frame_equal(price_reconciliation_report, mock_price_reconciliation_report)

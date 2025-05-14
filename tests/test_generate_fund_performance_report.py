import polars as pl
from polars.testing import assert_frame_equal
from generate_fund_performance_report import (
    get_config_n_constant,
    calculate_fund_performance,
)
from utils.utils import Utils


def test_calculate_fund_performance():
    """Test calculate_fund_performance function by comparing data to ensure a match."""
    mock_fund_performance_report = pl.read_csv(
        "tests/mock_data/fund_performance_report.csv"
    )
    (database_path, _, fund_holdings_table_name) = get_config_n_constant()
    conn = Utils.connect_to_database(database_path)
    fund_holdings_df = Utils.get_data_from_database(conn, fund_holdings_table_name)
    fund_performance_report = calculate_fund_performance(fund_holdings_df)
    expected_cols = {
        "DATE",
        "FUND",
        "FUND_MV_START",
        "FUND_MV_END",
        "FUND_REALISED_P_L",
        "RATE_OF_RETURN",
    }
    assert set(expected_cols) == set(fund_performance_report.columns)
    assert_frame_equal(fund_performance_report, mock_fund_performance_report)

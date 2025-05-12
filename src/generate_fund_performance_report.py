from configs.constants import TableNames
import os
import polars as pl
from utils.utils import Utils


def get_config_n_constant() -> tuple[str, str, str]:
    """
    Retrieves:
        - Database and output data paths from config
        - Fund holdings table name from constants.

    Returns:
        tuple[str, str, str]: A tuple containing the database path, output data path, and the table name for fund holdings data.
    """
    config = Utils.load_config()
    database_path = config.database_path
    output_data_path = config.output_data_path
    fund_holdings_table_name = TableNames.FUND_HOLDINGS
    return (database_path, output_data_path, fund_holdings_table_name)


def calculate_fund_performance(fund_holdings_df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculates the performance of funds based on their holdings, returning the best performing fund for each month.

    Args:
        fund_holdings_df (pl.DataFrame): DataFrame containing fund holdings data.

    Returns:
        pl.DataFrame: DataFrame containing the calculated fund performance.
    """
    fund_performance = (
        fund_holdings_df.group_by(["FUND", "DATE"])
        .agg(
            [
                pl.col("MARKET_VALUE").sum().alias("FUND_MV_END"),
                pl.col("REALISED_P_L").sum().alias("FUND_REALISED_P_L"),
            ]
        )
        .sort(["FUND", "DATE"])
        .with_columns(
            pl.col("FUND_MV_END").shift(1).over("FUND").alias("FUND_MV_START")
        )
        .with_columns(pl.col("FUND_MV_START").fill_null(pl.col("FUND_MV_END")))
    )
    fund_performance = fund_performance.with_columns(
        [
            (
                (
                    pl.col("FUND_MV_END")
                    - pl.col("FUND_MV_START")
                    + pl.col("FUND_REALISED_P_L")
                )
                / pl.col("FUND_MV_START")
            ).alias("RATE_OF_RETURN")
        ]
    )
    fund_performance_report = (
        fund_performance.sort(["DATE", "RATE_OF_RETURN"], descending=True)
        .unique(subset=["DATE"])
        .select(
            [
                pl.col("DATE"),
                pl.col("FUND"),
                pl.col("FUND_MV_START"),
                pl.col("FUND_MV_END"),
                pl.col("FUND_REALISED_P_L"),
                pl.col("RATE_OF_RETURN"),
            ]
        )
    )
    return fund_performance_report


def generate_report(
    database_path: str, output_data_path: str, fund_holdings_table_name: str
) -> None:
    """
    Generates a price reconciliation report for fund holdings.

    Args:
        database_path (str): Path to the database.
        output_data_path (str): Path to the output directory.
        fund_holdings_table_name (str): Name of the fund holdings table.

    Returns:
        None
    """
    conn = Utils.connect_to_database(database_path)
    fund_holdings_df = Utils.get_data_from_database(conn, fund_holdings_table_name)
    fund_performance_report = calculate_fund_performance(fund_holdings_df)
    if not os.path.exists(output_data_path):
        os.makedirs(output_data_path)
    fund_performance_report.write_csv(
        os.path.join(output_data_path, "fund_performance_report.csv")
    )


def main() -> None:
    """
    Main function to run the fund data processing.

    Returns:
        None
    """
    logger = Utils.setup_logger()
    logger.info("Generating fund performance report.")
    (database_path, output_data_path, fund_holdings_table_name) = (
        get_config_n_constant()
    )
    generate_report(database_path, output_data_path, fund_holdings_table_name)
    logger.info("Fund performance report generated.")


if __name__ == "__main__":
    main()

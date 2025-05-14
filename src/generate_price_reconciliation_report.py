from configs.constants import TableNames
import os
import polars as pl
import sqlite3
from utils.utils import Utils


def get_config_n_constant() -> tuple[str, str, str, str, str, str, str]:
    """
    Retrieves:
        - Database path and output data path from config
        - Table name from constants:
            - FUND_HOLDINGS
            - BOND_PRICES
            - BOND_REFERENCE
            - EQUITY_PRICES
            - EQUITY_REFERENCE

    Returns:
        tuple[str, str, str, str, str, str, str]: A tuple containing the database path, output data path, and table names.
    """
    config = Utils.load_config()
    database_path = config.database_path
    output_data_path = config.output_data_path
    fund_holdings_table_name = TableNames.FUND_HOLDINGS
    bond_prices_table_name = TableNames.BOND_PRICES
    bond_reference_table_name = TableNames.BOND_REFERENCE
    equity_prices_table_name = TableNames.EQUITY_PRICES
    return (
        database_path,
        output_data_path,
        fund_holdings_table_name,
        bond_prices_table_name,
        bond_reference_table_name,
        equity_prices_table_name,
    )


def get_bond_prices(
    conn: sqlite3.Connection,
    bond_prices_table_name: str,
    bond_reference_table_name: str,
) -> pl.DataFrame:
    """
    Retrieves bond prices from the database.
    Joined with the bond reference table to get SEDOL as not all equities provided has ISIN.

    Args:
        conn (sqlite3.Connection): SQLite connection object.
        bond_prices_table_name (str): Name of the bond prices table.
        bond_reference_table_name (str): Name of the bond reference table.

    Returns:
        pl.DataFrame: DataFrame containing bond prices.
    """
    query = f"""SELECT 
                br.SEDOL, 
                bp.* 
                FROM {bond_prices_table_name} bp
                JOIN {bond_reference_table_name} br
                ON bp.ISIN = br.ISIN"""
    bond_prices_df = pl.read_database(query, conn)
    return bond_prices_df


def perform_price_reconciliation(
    fund_holdings_df: pl.DataFrame,
    bond_prices_df: pl.DataFrame,
    equity_prices_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Performs price reconciliation between fund holdings and bond/equity prices.

    Args:
        fund_holdings_df (pl.DataFrame): DataFrame containing fund holdings.
        bond_prices_df (pl.DataFrame): DataFrame containing bond prices.
        equity_prices_df (pl.DataFrame): DataFrame containing equity prices.

    Returns:
        pl.DataFrame: DataFrame containing the price reconciliation report.
    """
    # Clean and prepare the data
    holdings_government_bonds_df = fund_holdings_df.filter(
        pl.col("FINANCIAL_TYPE") == "Government Bond"
    )
    holdings_government_bonds_df = holdings_government_bonds_df.rename(
        {"PRICE": "FUND_PRICE"}
    ).sort(["ISIN", "SEDOL", "DATE"])
    holdings_government_bonds_df_null_sedol = holdings_government_bonds_df.filter(
        pl.col("SEDOL").is_null()
    )
    holdings_government_bonds_df_null_isin = holdings_government_bonds_df.filter(
        pl.col("ISIN").is_null()
    )
    holdings_equities_df = fund_holdings_df.filter(
        pl.col("FINANCIAL_TYPE") == "Equities"
    )
    holdings_equities_df = holdings_equities_df.rename({"PRICE": "FUND_PRICE"}).sort(
        ["SYMBOL", "DATE"]
    )
    bond_prices_df = bond_prices_df.rename({"PRICE": "REF_PRICE"}).sort(
        ["ISIN", "SEDOL", "DATETIME"]
    )
    equity_prices_df = equity_prices_df.rename({"PRICE": "REF_PRICE"}).sort(
        ["SYMBOL", "DATETIME"]
    )

    # Perform the reconciliation
    bond_reconciliation_null_sedol = (
        holdings_government_bonds_df_null_sedol.join_asof(
            bond_prices_df,
            left_on="DATE",
            right_on="DATETIME",
            by=["ISIN"],
            strategy="backward",
        )
        .with_columns(
            [(pl.col("FUND_PRICE") - pl.col("REF_PRICE")).alias("PRICE_DIFF")]
        )
        .select(
            [
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
            ]
        )
    )
    bond_reconciliation_null_isin = (
        holdings_government_bonds_df_null_isin.join_asof(
            bond_prices_df,
            left_on="DATE",
            right_on="DATETIME",
            by=["SEDOL"],
            strategy="backward",
        )
        .with_columns(
            [(pl.col("FUND_PRICE") - pl.col("REF_PRICE")).alias("PRICE_DIFF")]
        )
        .select(
            [
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
            ]
        )
    )
    equity_reconciliation = (
        holdings_equities_df.join_asof(
            equity_prices_df,
            left_on="DATE",
            right_on="DATETIME",
            by=["SYMBOL"],
            strategy="backward",
        )
        .with_columns(
            [(pl.col("FUND_PRICE") - pl.col("REF_PRICE")).alias("PRICE_DIFF")]
        )
        .select(
            [
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
            ]
        )
    )
    reconciliation_report = pl.concat(
        [
            bond_reconciliation_null_sedol,
            bond_reconciliation_null_isin,
            equity_reconciliation,
        ]
    ).sort(["FUND", "DATE", "FINANCIAL_TYPE", "ISIN", "SEDOL", "SYMBOL"])
    return reconciliation_report


def generate_report(
    database_path: str,
    output_data_path: str,
    fund_holdings_table_name: str,
    bond_prices_table_name: str,
    bond_reference_table_name: str,
    equity_prices_table_name: str,
) -> None:
    """
    Generates a price reconciliation report for fund holdings.

    Args:
        database_path (str): Path to the database.
        output_data_path (str): Path to the output directory.
        fund_holdings_table_name (str): Name of the fund holdings table.
        bond_prices_table_name (str): Name of the bond prices table.
        bond_reference_table_name (str): Name of the bond reference table.
        equity_prices_table_name (str): Name of the equity prices table.

    Returns:
        None
    """
    conn = Utils.connect_to_database(database_path)
    bond_prices_df = get_bond_prices(
        conn, bond_prices_table_name, bond_reference_table_name
    )
    fund_holdings_df = Utils.get_data_from_database(conn, fund_holdings_table_name)
    equity_prices_df = Utils.get_data_from_database(conn, equity_prices_table_name)
    report_df = perform_price_reconciliation(
        fund_holdings_df, bond_prices_df, equity_prices_df
    )
    if not os.path.exists(output_data_path):
        os.makedirs(output_data_path)
    report_df.write_csv(
        os.path.join(output_data_path, "price_reconciliation_report.csv")
    )


def main() -> None:
    """
    Main function to run the fund data processing.

    Returns:
        None
    """
    logger = Utils.setup_logger()
    logger.info("Generating price reconciliation report.")
    (
        database_path,
        output_data_path,
        fund_holdings_table_name,
        bond_prices_table_name,
        bond_reference_table_name,
        equity_prices_table_name,
    ) = get_config_n_constant()
    generate_report(
        database_path,
        output_data_path,
        fund_holdings_table_name,
        bond_prices_table_name,
        bond_reference_table_name,
        equity_prices_table_name,
    )
    logger.info("Price reconciliation report generated.")


if __name__ == "__main__":
    main()

from configs.constants import TableNames
from datetime import datetime
from models.external_fund import ExternalFund
import logging
import os
import pandera as pa
from pandera.dtypes import (
    String,
    Float,
    Int,
    Bool,
    DateTime,
)
import polars as pl
import re
from utils.utils import Utils


def get_config_n_constant() -> tuple[str, str, str]:
    """
    Retrieves:
        - Database and input data paths from config
        - Fund holdings table name from constants.

    Returns:
        tuple[str, str, str]: A tuple containing the database path and the table name for fund holdings data.
    """
    config = Utils.load_config()
    database_path = config.database_path
    input_data_path = config.input_data_path
    fund_holdings_table_name = TableNames.FUND_HOLDINGS
    return (database_path, input_data_path, fund_holdings_table_name)


def clean_fund_name(fund_name_raw: str) -> str:
    """
    Cleans the fund name by removing unwanted characters and converting it to Title case.
    Assumes that the fund name is the last part of the string after splitting by spaces or special characters.
    e.g. "TT_monthly_Trustmind", "rpt-Catalysm", "Fund Whitestone", "Applebead"

    Args:
        fund_name_raw (str): The raw fund name.

    Returns:
        str: The cleaned fund name.
    """
    split_name = re.split(r"[\s\-_.]+", fund_name_raw)
    return split_name[-1].title() if split_name else None


def clean_date(date_raw: str) -> str:
    """
    Cleans the date string by removing unwanted characters and converting it to a standard format.
    Supported formats:
        - "DD-MM-YYYY"
        - "MM-DD-YYYY"
        - "DD_MM_YYYY"
        - "MM_DD_YYYY"
        - "YYYY-MM-DD"
        - "YYYYMMDD"
    Args:
        date_raw (str): The raw date string.

    Returns:
        str: The cleaned date string in "YYYY-MM-DD" format.
    """
    date_patterns = [
        (r"\b\d{2}-\d{2}-\d{4}\b", "%d-%m-%Y"),
        (r"\b\d{2}-\d{2}-\d{4}\b", "%m-%d-%Y"),
        (r"\b\d{2}_\d{2}_\d{4}\b", "%d_%m_%Y"),
        (r"\b\d{2}_\d{2}_\d{4}\b", "%m_%d_%Y"),
        (r"\b\d{4}-\d{2}-\d{2}\b", "%Y-%m-%d"),
        (r"\b\d{8}\b", "%Y%m%d"),
    ]
    for pattern, date_format in date_patterns:
        date_match = re.search(pattern, date_raw)
        if date_match:
            raw_date = date_match.group()
            try:
                parsed_date = datetime.strptime(raw_date, date_format)
                normalized_date = parsed_date.strftime("%Y-%m-%d")
                return normalized_date
            except ValueError:
                continue
    return None


def parse_filename(filename: str) -> tuple[str, str]:
    """
    Parses the filename to extract the fund name and date.
    This assumes the filename is in the format "fund_name_part.date_part".

    Args:
        filename (str): The filename to parse.

    Returns:
        tuple[str, str]: A tuple containing the fund name and date.

    Raises:
        ValueError: If the filename does not contain a valid fund name or date.
    """
    base_file_name = os.path.splitext(filename)[0]
    fund_name_raw = base_file_name.split(".")[0]
    date_raw = base_file_name.split(".")[1]
    fund_name = clean_fund_name(fund_name_raw)
    date = clean_date(date_raw)
    if not fund_name or not date:
        raise ValueError(
            f"Invalid filename format: {filename}. Parsed fund name: {fund_name}, Parsed date: {date}"
        )
    return fund_name, date


def pandera_to_polars_dtype(pa_dtype: pa.dtypes.DataType) -> pl.datatypes.DataType:
    """
    Map Pandera data types to Polars data types.
    Falls back to pl.Object if mapping is unknown.
    """
    mapping = {
        String: pl.String,
        Float: pl.Float64,
        Int: pl.Int64,
        Bool: pl.Boolean,
        DateTime: pl.Datetime,
    }
    return mapping.get(pa_dtype.__class__, pl.String)


def append_metadata(file_path: str, fund_name: str, date: str) -> pl.DataFrame:
    """
    Reads the csv and appends fund_name and date as metadata.
    """
    df = pl.read_csv(file_path)
    df = df.with_columns([pl.lit(fund_name).alias("FUND"), pl.lit(date).alias("DATE")])
    df = df.rename(
        {col: Utils.convert_to_screaming_snake_case(col) for col in df.columns}
    )
    expected_columns = ExternalFund.to_schema().columns.items()
    for column_name, column_schema in expected_columns:
        if column_name not in df.columns:
            pl_dtype = pandera_to_polars_dtype(column_schema.dtype)
            df = df.with_columns(pl.lit(None, dtype=pl_dtype).alias(column_name))
    df = df.select([col for col, _ in expected_columns]).filter(
        ~pl.all_horizontal(pl.all().is_null())
    )
    return df


def load_data_to_db(
    database_path: str, fund_holdings_table_name: str, df: pl.DataFrame
) -> None:
    """
    Loads the DataFrame into the specified SQLite database table.

    Args:
        database_path (str): The path to the SQLite database file.
        fund_holdings_table_name (str): The name of the table to load data into.
        df (pl.DataFrame): The DataFrame to load.

    Returns:
        None

    Raises:
        Exception: If there is an error while loading data to the database.
    """
    try:
        logger = logging.getLogger()
        connection_string = f"sqlite:///{os.path.abspath(database_path)}"
        df.write_database(
            connection=connection_string,
            table_name=fund_holdings_table_name,
            if_table_exists="replace",
        )
    except Exception as e:
        logger.error(f"Error loading data to database: {e}")
        raise


def process_fund_data(
    database_path: str, input_data_path: str, fund_holdings_table_name: str
) -> None:
    """
    Loads fund data from the specified input data path and stores it in the database.

    Args:
        database_path (str): The path to the database.
        input_data_path (str): The path to the input data files.
        fund_holdings_table_name (str): The name of the table to store fund holdings data.

    Returns:
        None
    """
    logger = logging.getLogger()
    df_list = []
    files = [
        f
        for f in os.listdir(input_data_path)
        if os.path.isfile(os.path.join(input_data_path, f)) and f.endswith(".csv")
    ]
    if not files:
        logger.info("No CSV files found in the input data path.")
        return
    for file in files:
        fund_name, date = parse_filename(file)
        df = append_metadata(os.path.join(input_data_path, file), fund_name, date)
        df_list.append(df)
    if not df_list:
        logger.info("No valid fund data found in the input data path.")
        return
    if df_list:
        df_combined = pl.concat(df_list)
        ExternalFund.validate(df_combined)
        load_data_to_db(database_path, fund_holdings_table_name, df_combined)


if __name__ == "__main__":
    logger = Utils.setup_logger()
    logger.info("Starting fund data processing.")
    database_path, input_data_path, fund_holdings_table_name = get_config_n_constant()
    process_fund_data(database_path, input_data_path, fund_holdings_table_name)
    logger.info("Fund data processing completed.")

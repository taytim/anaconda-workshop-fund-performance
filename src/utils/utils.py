import logging
from models.config import Config
import polars as pl
import re
import sqlite3
import yaml


class Utils:
    """
    Utility class for the application.
    """

    @staticmethod
    def load_config(config_path: str = "./src/configs/config.yml") -> Config:
        """
        Load the configuration from a YAML file.

        Args:
            None

        Returns:
            Config: Configuration object.
        """
        with open(config_path, "r") as file:
            config_data = yaml.safe_load(file)
        return Config(**config_data)

    @staticmethod
    def setup_logger() -> logging.Logger:
        """
        Sets up a logger with basic formatting.

        Args:
            name (str): Name of the logger.

        Returns:
            logging.Logger: Configured logger instance.
        """
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        if logger.hasHandlers():
            logger.handlers.clear()
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(filename)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    @staticmethod
    def connect_to_database(database_path: str) -> sqlite3.Connection:
        """
        Connect to the SQLite database.

        Args:
            database_path (str): Path to the SQLite database file.

        Returns:
            None
        """
        logger = logging.getLogger()
        conn = sqlite3.connect(database_path)
        logger.info(f"Connected to the database at {database_path}")
        return conn

    @staticmethod
    def convert_to_screaming_snake_case(input: str) -> str:
        """
        Convert a string to screaming snake case.

        Args:
            input (str): String to convert.

        Returns:
            str: The converted string in screaming snake case.
        """
        return re.sub(r"[^\w]+", "_", input.upper())

    @staticmethod
    def get_data_from_database(
        conn: sqlite3.Connection, table_name: str
    ) -> pl.DataFrame:
        """
        Retrieves a DataFrame from the SQLite database for the interested table.

        Args:
            conn (sqlite3.Connection): SQLite connection object.
            table_name (str): Name of the table to retrieve data from.

        Returns:
            pl.DataFrame: DataFrame containing the data from the specified table.
        """
        query = f"SELECT * FROM {table_name}"
        df = pl.read_database(query, conn, infer_schema_length=None)
        return df

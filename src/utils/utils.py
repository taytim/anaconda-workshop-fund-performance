import logging
from models.config import Config
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
            print(config_data)
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

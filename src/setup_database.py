import argparse
import logging
import os
from utils.utils import Utils


def get_db_config() -> tuple[str, str]:
    """
    Get the database configuration from the config file.

    Returns:
        tuple[str, str]: A tuple containing the database path and the database setup scripts path.
    """
    config = Utils.load_config()
    database_path = config.database_path
    database_setup_scripts_path = config.database_setup_scripts_path
    return (database_path, database_setup_scripts_path)


def initialise_database(
    database_path: str, database_setup_scripts_path: str, recreate: bool
) -> None:
    """
    Create the SQLite database and execute the setup scripts if the database does not exist.

    Args:
        database_path (str): Path to the SQLite database file.
        database_setup_scripts_path (str): Path to the directory containing setup scripts.
        recreate (bool): Flag to indicate whether to recreate the database if it already exists.

    Returns:
        None
    """
    logger = logging.getLogger()
    if os.path.exists(database_path) and recreate:
        os.remove(database_path)
        logger.info(f"Existing database at {database_path} removed.")
    if not os.path.exists(database_path):
        conn = Utils.connect_to_database(database_path)
        for script_file in os.listdir(database_setup_scripts_path):
            script_path = os.path.join(database_setup_scripts_path, script_file)
            with open(script_path, "r") as script:
                conn.executescript(script.read())
        logger.info(f"Database created at {database_path}")
    else:
        logger.info(f"Database already exists at {database_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Setup the SQLite database.")
    parser.add_argument(
        "--recreate",
        action="store_true",
        help="Recreate the database if it already exists.",
    )
    args = parser.parse_args()
    logger = Utils.setup_logger()
    database_path, database_setup_scripts_path = get_db_config()
    initialise_database(database_path, database_setup_scripts_path, args.recreate)
    logger.info("Database setup completed.")

from pydantic import BaseModel


class Config(BaseModel):
    """
    Configuration class for the application.
    """

    database_path: str
    database_setup_scripts_path: str
    input_data_path: str
    output_data_path: str

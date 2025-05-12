# Fund Performance Analysis

## About the Project
This project is designed to analyze the performance of various funds using Python, SQLite, and a range of libraries for ETL (Extract, Transform, Load) and data analysis. It provides a robust environment for data processing and reporting.

## Features
- Extract, transform, and load (ETL) fund performance data using SQLite and Python.
- Analyze historical fund performance data.
- Perform statistical analysis to evaluate fund performance.
- Export results in user-friendly formats (CSV).

## Folder Structure
The project follows this folder structure:
```
anaconda-workshop-fund-performance/
├── .github/
│ ├── workflows/        # yml for github workflows
├── input/              
│ ├── external-funds/   # Input data files
├── output/             # Generated reports
├── db/                 # SQLite database file
├── db_scripts/         # DDL & DML scripts for database initialisation
├── src/                # Source code
│ ├── configs/          # Configuration files
│ ├── models/           # Models used for schema validation
│ ├── utils/            # Util functions
├── tests/              # Test suite
│ ├── mock_data/        # Mock report data
│ ├── models/           # Tests for models
│ ├── utils/            # Test for utils
├── README.md           # Project documentation
├── pyproject.toml      # Poetry configuration file
├── poetry.lock         # Poetry lock file
├── pytest.ini          # Pytest configuration file
└── .pre-commit-config.yaml # Configuration for pre-commit hooks
```

## Setup
1. Ensure you have Python 3.13 installed on your system.
2. Clone this repository:
    ```bash
    git clone https://github.com/taytim/anaconda-workshop-fund-performance.git
    ```
3. Navigate to the project directory:
    ```bash
    cd anaconda-workshop-fund-performance
    ```
4. Create a virtual environment (optional but recommended):
    ```bash
    python -m venv venv
    source venv/bin/activate
    ```
5. Install Poetry if not already installed:
    ```bash
    pip install poetry
    ```
6. Install the project dependencies:
    ```bash
    poetry install
    ```
7. Set up pre-commit hooks:
    ```bash
    poetry run pre-commit install
    ```

## Usage
1. Place your fund performance data in the `data/` directory.
2. Run the analysis script:
    ```bash
    poetry run python analyze_funds.py
    ```
3. View the generated visualizations and reports in the `output/` directory.

## Testing
Run the test suite using `pytest`:
```bash
poetry run pytest --cov
```

## Linting and Formatting
Linting and Formatting with ruff has been setup through pre-commit hooks to ensure code quality.
Should you want to manually trigger:
```bash
poetry run ruff check .
poetry run ruff format .
```

## 
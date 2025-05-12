echo "Setting up the database..."
poetry run python ./src/setup_database.py

echo "Loading fund data..."
poetry run python ./src/load_fund_data.py

echo "Generating price reconciliation report..."
poetry run python ./src/generate_price_reconciliation_report.py

echo "Generating fund performance report..."
poetry run python ./src/generate_fund_performance_report.py

echo "All tasks completed successfully!"
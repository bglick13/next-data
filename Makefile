.PHONY: format lint typecheck check clean

# Format code with black and ruff
format:
	ruff format .
	ruff check --fix .

# Run linting checks
lint:
	ruff check .

# Run type checking
typecheck:
	mypy .

# Run all code quality checks
check: format lint typecheck

# Clean up python cache files
clean:
	find . -type d -name "__pycache__" -exec rm -r {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete
	find . -type d -name "*.egg-info" -exec rm -r {} +
	find . -type d -name "*.egg" -exec rm -r {} +
	find . -type d -name ".mypy_cache" -exec rm -r {} +
	find . -type d -name ".ruff_cache" -exec rm -r {} + 
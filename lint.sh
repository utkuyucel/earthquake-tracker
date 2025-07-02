#!/bin/bash
set -e

echo "🔍 Running code quality checks..."

# Run ruff check and fix
echo "Running ruff check & fix..."
ruff check --fix *.py

# Run black formatting
echo "Running black formatting..."
black *.py

# Final ruff check (no fix)
echo "Running final ruff check..."
ruff check *.py

echo "All checks passed! Code is ready for production."

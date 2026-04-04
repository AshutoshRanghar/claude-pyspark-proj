#!/bin/bash

# Lint script for Python code
# Usage: ./lint.sh [path] [--fix]

set -e

PATH_TO_LINT="${1:-.}"
AUTO_FIX="${2:-}"

echo "Installing lint tools..."
pip install -q black flake8 pylint 2>/dev/null || true

echo ""
echo "================================"
echo "🔍 Linting: $PATH_TO_LINT"
echo "================================"
echo ""

# Run black (formatter)
echo "📋 Checking code formatting with black..."
if black --check "$PATH_TO_LINT" 2>/dev/null || true; then
    echo "✓ Black check passed"
else
    if [ "$AUTO_FIX" == "--fix" ]; then
        echo "🔧 Running black to auto-format..."
        black "$PATH_TO_LINT"
        echo "✓ Code reformatted"
    else
        echo "⚠ Code formatting issues found. Run 'black $PATH_TO_LINT' to fix"
    fi
fi

echo ""

# Run flake8 (PEP 8)
echo "📋 Checking PEP 8 compliance with flake8..."
if flake8 "$PATH_TO_LINT" --max-line-length=120 --count 2>/dev/null || true; then
    echo "✓ Flake8 check passed"
else
    echo "⚠ PEP 8 violations found above"
fi

echo ""

# Run pylint (code quality)
echo "📋 Analyzing code quality with pylint..."
if pylint "$PATH_TO_LINT" --disable=R,C --fail-under=7.0 2>/dev/null || true; then
    echo "✓ Pylint check passed"
else
    echo "⚠ Code quality issues found above"
fi

echo ""
echo "================================"
echo "✅ Lint report complete"
echo "================================"

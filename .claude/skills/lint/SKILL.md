---
name: lint
description: Lint Python code for style, formatting, and quality issues using black, flake8, and pylint. Use this skill to check code style compliance, find formatting issues, and identify PEP 8 violations. Trigger on requests like "lint the code", "check code style", "run linting", "find style issues", or when you want to validate code before committing.
compatibility: Requires Python files (src/ folder); works with any Python project
---

# Lint Skill

## Overview

This skill checks Python code for style violations, formatting issues, and quality problems using industry-standard tools:
- **black** — Code formatter (opinionated formatting)
- **flake8** — Style guide enforcement (PEP 8)
- **pylint** — Code analysis (bugs, style, complexity)

## How to Use

When invoked, this skill will:

1. **Check Code Formatting** (black)
   - Detect inconsistent indentation, line length violations
   - Check quote consistency, spacing around operators

2. **Validate PEP 8 Compliance** (flake8)
   - Find unused imports, undefined variables
   - Detect unused variables, trailing whitespace
   - Check line length, complexity issues

3. **Analyze Code Quality** (pylint)
   - Identify potential bugs and design problems
   - Flag missing docstrings, overly complex functions
   - Check naming conventions

## Input Format

The skill operates on:
- `src/` folder (default, all Python files)
- Specific file paths provided by user
- Configurable line length and complexity thresholds

## Output Format

```
# Lint Report: [filename or summary]

## Summary
- Files checked: N
- Issues found: Total | Errors | Warnings | Style
- Overall grade: [pylint score or pass/fail]

## Issues by Category

### 🔴 Errors (must fix)
- [filename:line] Issue description
- Fix: [suggestion]

### 🟠 Warnings (should fix)
- [filename:line] Issue description

### 🟡 Style Issues (nice to have)
- [filename:line] Issue description

### 🟢 Formatting Issues (black)
- [filename:line] Issue description

## Suggestions

1. Format all code: `black src/`
2. Fix critical issues: [list specific items]
3. Next steps for CI/CD integration
```

## Configuration

Default settings:
- **Line length**: 120 characters (suitable for data science code)
- **Max complexity**: 10 (McCabe complexity)
- **Target Python**: 3.10+
- **Ignored patterns**: `.venv`, `__pycache__`, `.pytest_cache`

## Integration with CI/CD

This skill generates reports suitable for GitHub Actions:

```yaml
- name: Lint Code
  run: |
    pip install black flake8 pylint
    black --check src/
    flake8 src/ --max-line-length=120
    pylint src/claude_pyspark_proj/ --disable=R,C
```

## Example Interaction

**User**: "lint the src folder"

**Skill Output**:
```
# Lint Report: src/

## Summary
- Files checked: 8
- Issues found: 12 | 2 Errors | 5 Warnings | 5 Style
- Overall grade: 7.2/10 (fair)

## Issues by Category

### 🔴 Errors (must fix)
- src/claude_pyspark_proj/cli_autoloader.py:22 Hardcoded secret found
  Fix: Move to environment variable or Databricks secret

- src/claude_pyspark_proj/cli_autoloader.py:27 Hardcoded user path
  Fix: Use environment variable for workspace paths

### 🟠 Warnings (should fix)
- src/claude_pyspark_proj/streaming/get_data_from_event_hubs.py:54 Unused import 'json'
  Fix: Remove or use it

### 🟡 Style Issues
- src/claude_pyspark_proj/autoloader/cloud_file_ingester.py:99 Line too long (145 > 120)
  Fix: Break into multiple lines

### 🟢 Formatting Issues (black)
- 3 files need reformatting: Run `black src/` to fix
```

## Notes

- This skill performs **static analysis** (no code execution)
- Severity levels help prioritize what to fix first
- Some warnings may be false positives (will note when encountered)
- For complex type checking, consider adding `mypy` to the workflow
- Can be run as pre-commit hook or in CI/CD pipeline


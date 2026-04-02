# Claude Code Project Guidelines

## Project Overview
PySpark streaming pipeline with Azure Event Hubs integration. Batch and streaming job processing with pytest test coverage.

## Structure
- `src/claude_pyspark_proj/` — Main application code
- `tests/` — Pytest test suites
- `project_architecture/` — Architecture documentation

## Key Technologies
- PySpark (batch & streaming)
- Azure Event Hubs
- Python 3.10+
- pytest for testing

## Guidelines

### Code Style
- Follow PEP 8
- Use type hints where applicable
- Keep functions focused and testable

### Testing
- Write tests for all new jobs/transformations
- Use pytest and PySpark test utilities
- Aim for >80% coverage on critical paths

### Development Workflow
- Work on feature branches, PR to main
- Keep commits atomic and well-described
- Run tests before pushing

### Preferences
- Be concise in responses
- Focus on implementation, skip unnecessary refactoring
- No extra comments/docstrings unless logic is non-obvious

## Resources
- Architecture decisions in `project_architecture/`
- Streaming module at `src/claude_pyspark_proj/streaming/`

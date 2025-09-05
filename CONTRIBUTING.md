# Contributing to dagster-clickhouse

Thank you for your interest in contributing to dagster-clickhouse! This document provides guidelines for contributing to the project.

## Development Setup

### Prerequisites

- Python 3.8 or higher
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- ClickHouse server (for testing)

### Setting up the Development Environment

1. Clone the repository:
```bash
git clone https://github.com/yourusername/dagster-clickhouse.git
cd dagster-clickhouse
```

2. Install dependencies using uv (recommended):
```bash
# Install uv if you haven't already
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync --all-extras --dev
```

Or using pip:
```bash
pip install -e ".[dev,test]"
```

3. Set up pre-commit hooks:
```bash
uv run pre-commit install
# or: pre-commit install
```

4. Start ClickHouse for testing:
```bash
docker-compose up -d
```

## Development Workflow

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=dagster_clickhouse --cov-report=html

# Run specific test file
uv run pytest tests/test_clickhouse_storage.py -v
```

### Code Quality

We use several tools to maintain code quality:

- **Black**: Code formatting
- **Ruff**: Linting and import sorting
- **MyPy**: Type checking
- **Bandit**: Security scanning

Run all checks:
```bash
uv run black .
uv run ruff check . --fix
uv run mypy dagster_clickhouse/
uv run bandit -r dagster_clickhouse/
```

### Making Changes

1. Create a new branch for your feature/fix:
```bash
git checkout -b feature/your-feature-name
```

2. Make your changes following the coding standards below

3. Add tests for new functionality

4. Run the test suite and ensure all tests pass

5. Commit your changes with a descriptive message

6. Push your branch and create a pull request

## Coding Standards

### Python Code Style

- Follow PEP 8 (enforced by Black and Ruff)
- Use type hints for all public functions and methods
- Write docstrings for all public classes and functions (Google style)
- Keep functions focused and small
- Use meaningful variable and function names

### Example Function with Proper Documentation

```python
def store_event(self, event: EventLogEntry) -> None:
    """Store an event with optimized batching for balanced latency/throughput.
    
    Args:
        event: The event log entry to store
        
    Raises:
        DagsterInvariantViolationError: If ClickHouse storage fails
        ValueError: If event is invalid
        
    Example:
        >>> storage = ClickHouseEventLogStorage(url="http://localhost:8123/dagster")
        >>> event = EventLogEntry(...)
        >>> storage.store_event(event)
    """
```

### Testing Guidelines

- Write tests for all new functionality
- Use descriptive test names that explain what is being tested
- Include both positive and negative test cases
- Test error conditions and edge cases
- Use fixtures for common test setup

### Performance Considerations

- ClickHouse is optimized for batch operations - prefer batching over single operations
- Use async inserts for better single-event performance
- Consider memory usage for large batches
- Profile performance-critical code paths

## Pull Request Guidelines

### Before Submitting

- [ ] All tests pass locally
- [ ] Code follows the style guidelines
- [ ] New functionality includes tests
- [ ] Documentation is updated if needed
- [ ] CHANGELOG.md is updated for user-facing changes

### PR Description

Please include:

1. **What**: Brief description of the changes
2. **Why**: Motivation for the changes
3. **How**: Technical approach taken
4. **Testing**: How the changes were tested
5. **Breaking Changes**: Any backwards incompatible changes

### Example PR Template

```markdown
## What
Add connection pooling for improved performance under high concurrency.

## Why
The current implementation creates a new connection for each operation, which is inefficient and can lead to connection exhaustion under load.

## How
- Implemented connection pool using clickhouse-connect's built-in pooling
- Added configuration options for pool size and timeout
- Updated error handling to properly manage pool connections

## Testing
- Added unit tests for connection pool functionality
- Tested with concurrent operations (100 threads)
- Verified no connection leaks under stress testing

## Breaking Changes
None - all changes are backwards compatible.
```

## Issue Reporting

When reporting issues, please include:

1. **Environment**: Python version, dagster-clickhouse version, ClickHouse version
2. **Configuration**: Relevant configuration (sanitize sensitive data)
3. **Steps to Reproduce**: Minimal code example that reproduces the issue
4. **Expected Behavior**: What you expected to happen
5. **Actual Behavior**: What actually happened
6. **Logs**: Relevant error messages or logs

## Security

If you discover a security vulnerability, please email the maintainers directly rather than opening a public issue.

## License

By contributing to dagster-clickhouse, you agree that your contributions will be licensed under the MIT License.

## Questions?

Feel free to open an issue for questions about contributing or reach out to the maintainers.

Thank you for contributing! 🚀

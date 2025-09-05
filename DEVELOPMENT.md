# Development Guide

## Quick Start

### Prerequisites

- [uv](https://docs.astral.sh/uv/) (recommended) or Python 3.8+
- [Docker](https://www.docker.com/) and Docker Compose

### Setup

1. **Clone and install dependencies:**
   ```bash
   git clone <repository-url>
   cd dagster-clickhouse
   uv sync --all-extras --dev
   ```

2. **Start ClickHouse:**
   ```bash
   docker-compose up -d clickhouse
   ```

3. **Verify ClickHouse is running:**
   ```bash
   curl "http://dagster:dagster@localhost:8123/?query=SELECT%201"
   ```

4. **Run tests:**
   ```bash
   uv run pytest tests/ -v
   ```

## Development Workflow

### Using uv (Recommended)

```bash
# Install dependencies
uv sync

# Add new dependency
uv add "package-name>=1.0.0"

# Add development dependency
uv add --dev "dev-package>=1.0.0"

# Run tests
uv run pytest

# Run linting
uv run ruff check .
uv run black --check .
uv run mypy dagster_clickhouse/

# Format code
uv run ruff format .
uv run black .
```

### Using pip/venv

```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v
```

## ClickHouse Development

### Docker Compose Services

- **clickhouse**: Main ClickHouse server
  - HTTP: `http://localhost:8123`
  - TCP: `localhost:9000`
  - Credentials: `dagster/dagster`
  - Database: `dagster`

- **clickhouse-client**: Interactive client (optional)
  ```bash
  docker-compose run --rm clickhouse-client
  ```

### Useful ClickHouse Commands

```bash
# Connect to ClickHouse
docker-compose exec clickhouse clickhouse-client --user dagster --password dagster --database dagster

# View tables
docker-compose exec clickhouse clickhouse-client --user dagster --password dagster --query "SHOW TABLES FROM dagster"

# Check table sizes
docker-compose exec clickhouse clickhouse-client --user dagster --password dagster --query "SELECT table, formatReadableSize(sum(bytes)) as size FROM system.parts WHERE database = 'dagster' GROUP BY table"

# Monitor queries
docker-compose exec clickhouse clickhouse-client --user dagster --password dagster --query "SELECT query, query_duration_ms FROM system.query_log ORDER BY event_time DESC LIMIT 10"
```

### Configuration

The ClickHouse instance is configured for development with:
- Async inserts enabled for better performance
- Query logging for debugging
- Relaxed memory limits
- Custom user `dagster` with password `dagster`

Configuration files:
- `docker-compose.yml`: Service definition
- `docker/clickhouse-config.xml`: ClickHouse server configuration

## Testing

### Running Tests

```bash
# All tests
uv run pytest

# Specific test file
uv run pytest tests/test_clickhouse_storage.py -v

# With coverage
uv run pytest --cov=dagster_clickhouse --cov-report=html

# Integration tests only
uv run pytest -m integration

# Skip slow tests
uv run pytest -m "not slow"
```

### Test Configuration

Tests use the ClickHouse instance at `localhost:9000` by default. Make sure ClickHouse is running before running tests.

### Writing Tests

- Use the `clickhouse_storage` fixture for consistent test setup
- Tests should be independent and clean up after themselves
- Use appropriate markers (`@pytest.mark.slow`, `@pytest.mark.integration`)

## Code Quality

### Pre-commit Hooks

```bash
# Install pre-commit hooks
uv run pre-commit install

# Run hooks manually
uv run pre-commit run --all-files
```

### Linting and Formatting

The project uses:
- **ruff**: Fast Python linter and formatter
- **black**: Code formatter
- **mypy**: Type checking

Configuration is in `pyproject.toml`.

### Type Checking

```bash
# Run mypy
uv run mypy dagster_clickhouse/

# Check specific file
uv run mypy dagster_clickhouse/event_log/event_log.py
```

## Building and Publishing

### Build Package

```bash
# Using uv
uv build

# Using hatch
uv run hatch build
```

### Local Installation

```bash
# Install in development mode
uv pip install -e .

# Install from built wheel
uv pip install dist/dagster_clickhouse-*.whl
```

## Troubleshooting

### ClickHouse Connection Issues

1. **Check if ClickHouse is running:**
   ```bash
   docker-compose ps clickhouse
   ```

2. **Check ClickHouse logs:**
   ```bash
   docker-compose logs clickhouse
   ```

3. **Restart ClickHouse:**
   ```bash
   docker-compose restart clickhouse
   ```

4. **Reset ClickHouse data:**
   ```bash
   docker-compose down -v
   docker-compose up -d clickhouse
   ```

### Test Failures

1. **Ensure ClickHouse is healthy:**
   ```bash
   curl "http://dagster:dagster@localhost:8123/ping"
   ```

2. **Check for port conflicts:**
   ```bash
   lsof -i :8123
   lsof -i :9000
   ```

3. **Run tests with verbose output:**
   ```bash
   uv run pytest tests/ -v -s
   ```

### Performance Issues

1. **Monitor ClickHouse performance:**
   ```bash
   docker-compose exec clickhouse clickhouse-client --user dagster --password dagster --query "SELECT * FROM system.metrics WHERE metric LIKE '%Query%'"
   ```

2. **Check memory usage:**
   ```bash
   docker stats dagster-clickhouse-dev
   ```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass and code is formatted
6. Submit a pull request

See the main README for more information about the project structure and goals.

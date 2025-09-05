# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial ClickHouse event log storage implementation
- High-performance batch insertion with configurable batching
- Real-time event streaming support
- Async insert optimization for single events
- Connection pooling for better performance
- Comprehensive test suite

### Changed
- Migrated from setup.py to modern pyproject.toml
- Added uv support for faster dependency management
- Improved error handling and logging

### Fixed
- Thread safety issues in background flush
- Data loss on error conditions
- Connection management inefficiencies

## [0.1.0] - 2024-01-XX

### Added
- Initial release of dagster-clickhouse package
- ClickHouseEventLogStorage implementation
- Basic event storage and retrieval functionality
- Docker setup for development
- Performance benchmarks and optimization

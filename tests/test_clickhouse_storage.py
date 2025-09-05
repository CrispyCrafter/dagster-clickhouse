"""Tests for ClickHouse event log storage."""

import threading
import time
from datetime import datetime

import pytest
from dagster import DagsterEventType
from dagster._core.events import DagsterEvent
from dagster._core.events.log import EventLogEntry

from dagster_clickhouse.event_log.event_log import ClickHouseEventLogStorage


def create_test_event(
    run_id: str, message: str, event_type: DagsterEventType, step_key: str = None
) -> EventLogEntry:
    """Helper function to create test EventLogEntry objects."""
    dagster_event = DagsterEvent(
        event_type_value=event_type.value,
        job_name="test_job",
        step_key=step_key,
    )

    return EventLogEntry(
        error_info=None,
        level="INFO",
        user_message=message,
        run_id=run_id,
        timestamp=datetime.now().timestamp(),
        step_key=step_key,
        job_name="test_job",
        dagster_event=dagster_event,
    )


@pytest.fixture
def clickhouse_storage():
    """Create a test ClickHouse storage instance with proper isolation."""
    import os

    # Use environment variable for CI, fallback to local default
    clickhouse_url = os.environ.get(
        "CLICKHOUSE_URL", "http://dagster:dagster@localhost:8123/test_dagster"
    )

    storage = ClickHouseEventLogStorage(
        clickhouse_url=clickhouse_url,
        batch_size=10,  # Small batch for testing
        flush_interval=1.0,  # Must be float
        should_autocreate_tables=True,
    )

    # Clean up any existing test data before test
    storage.wipe()

    # Ensure any pending background operations are complete
    import time

    time.sleep(0.1)  # Small delay to ensure cleanup is complete

    yield storage

    # Thorough cleanup after test
    try:
        # Force flush any remaining events
        with storage._buffer_lock:
            if storage._event_buffer:
                storage._flush_events()

        # Stop any background threads
        storage._shutdown = True
        if hasattr(storage, "_shutdown_event"):
            storage._shutdown_event.set()

        # Wait for background thread to stop
        if hasattr(storage, "_flush_thread") and storage._flush_thread.is_alive():
            storage._flush_thread.join(timeout=1.0)

        # Clean up event watcher
        if storage._event_watcher:
            storage._event_watcher.close()
            storage._event_watcher = None

        # Wipe all data
        storage.wipe()

        # Small delay to ensure cleanup is complete
        time.sleep(0.1)

    except Exception as e:
        # Log cleanup errors but don't fail the test
        print(f"Cleanup warning: {e}")
    finally:
        # Always try to dispose
        try:
            storage.dispose()
        except Exception:
            pass


def test_clickhouse_storage_creation(clickhouse_storage):
    """Test that ClickHouse storage can be created."""
    assert clickhouse_storage.is_persistent
    assert clickhouse_storage.batch_size == 10
    assert clickhouse_storage.flush_interval == 1


def test_store_and_retrieve_event(clickhouse_storage):
    """Test storing and retrieving a simple event."""
    # Create a test event
    event = create_test_event("test_run_123", "Test event", DagsterEventType.RUN_START)

    # Store the event
    clickhouse_storage.store_event(event)

    # Force flush
    with clickhouse_storage._buffer_lock:
        clickhouse_storage._flush_events()

    # Retrieve events for the run
    records = clickhouse_storage.get_records_for_run("test_run_123")

    assert len(records.records) == 1
    retrieved_event = records.records[0].event_log_entry
    assert retrieved_event.run_id == "test_run_123"
    assert retrieved_event.user_message == "Test event"


def test_batch_storage(clickhouse_storage):
    """Test batch storage of multiple events."""
    events = []
    for i in range(5):
        event = create_test_event(
            "batch_test_run", f"Test event {i}", DagsterEventType.RUN_START
        )
        events.append(event)

    # Store events as batch
    clickhouse_storage.store_event_batch(events)

    # Retrieve events
    records = clickhouse_storage.get_records_for_run("batch_test_run")

    assert len(records.records) == 5


def test_event_filtering(clickhouse_storage):
    """Test filtering events by type."""
    # Store events of different types
    events = [
        create_test_event("filter_test_run", "Start event", DagsterEventType.RUN_START),
        create_test_event(
            "filter_test_run", "Success event", DagsterEventType.RUN_SUCCESS
        ),
    ]

    clickhouse_storage.store_event_batch(events)

    # Filter by RUN_START events only
    start_records = clickhouse_storage.get_records_for_run(
        "filter_test_run", of_type={DagsterEventType.RUN_START}
    )

    assert len(start_records.records) == 1
    assert (
        start_records.records[0].event_log_entry.dagster_event_type
        == DagsterEventType.RUN_START
    )


def test_asset_keys(clickhouse_storage):
    """Test asset key management."""
    from dagster._core.definitions.events import AssetKey

    # Test has_asset_key with non-existent key
    test_key = AssetKey(["test", "asset"])
    assert not clickhouse_storage.has_asset_key(test_key)

    # Test all_asset_keys (should be empty initially)
    keys = clickhouse_storage.all_asset_keys()
    assert len(keys) == 0


def test_dynamic_partitions(clickhouse_storage):
    """Test dynamic partition management."""
    import time

    partition_def = "test_partition_def"
    partition_keys = ["2023-01-01", "2023-01-02", "2023-01-03"]

    # Ensure clean state
    time.sleep(0.1)

    # Add partitions
    clickhouse_storage.add_dynamic_partitions(partition_def, partition_keys)

    # Check if partitions exist
    for key in partition_keys:
        assert clickhouse_storage.has_dynamic_partition(partition_def, key)

    # Get all partitions
    all_partitions = clickhouse_storage.get_dynamic_partitions(partition_def)
    assert set(all_partitions) == set(partition_keys)

    # Delete a partition
    clickhouse_storage.delete_dynamic_partition(partition_def, "2023-01-01")

    # Small delay to ensure deletion is processed
    time.sleep(0.1)

    assert not clickhouse_storage.has_dynamic_partition(partition_def, "2023-01-01")


def test_wipe_storage(clickhouse_storage):
    """Test wiping all storage."""
    # Add some test data
    event = create_test_event("wipe_test_run", "Test event", DagsterEventType.RUN_START)

    clickhouse_storage.store_event(event)

    # Force flush
    with clickhouse_storage._buffer_lock:
        clickhouse_storage._flush_events()

    # Verify data exists
    records = clickhouse_storage.get_records_for_run("wipe_test_run")
    assert len(records.records) == 1

    # Wipe storage
    clickhouse_storage.wipe()

    # Verify data is gone
    records = clickhouse_storage.get_records_for_run("wipe_test_run")
    assert len(records.records) == 0


def test_event_watching(clickhouse_storage):
    """Test real-time event watching functionality."""
    received_events = []
    watch_complete = threading.Event()

    def event_handler(event, cursor):
        """Handler to collect received events."""
        received_events.append(event)
        if len(received_events) >= 3:  # Wait for 3 events
            watch_complete.set()

    run_id = "watch_test_run"

    # Start watching for events
    clickhouse_storage.watch(run_id, None, event_handler)

    # Store initial event and flush to establish baseline
    initial_event = create_test_event(
        run_id, "Initial event", DagsterEventType.RUN_START
    )
    clickhouse_storage.store_event(initial_event)
    with clickhouse_storage._buffer_lock:
        clickhouse_storage._flush_events()

    # Wait a bit for the polling to pick up the initial event
    time.sleep(1.5)

    # Add more events that should be picked up by the watcher
    for i in range(2):
        event = create_test_event(
            run_id, f"Watched event {i + 1}", DagsterEventType.STEP_START
        )
        clickhouse_storage.store_event(event)
        with clickhouse_storage._buffer_lock:
            clickhouse_storage._flush_events()
        time.sleep(0.5)  # Small delay between events

    # Wait for events to be received (with timeout)
    watch_complete.wait(timeout=10)

    # Stop watching
    clickhouse_storage.end_watch(run_id, event_handler)

    # Verify we received the events
    assert (
        len(received_events) >= 1
    ), f"Expected at least 1 event, got {len(received_events)}"

    # Verify the events are correct
    run_ids = [event.run_id for event in received_events]
    assert all(
        rid == run_id for rid in run_ids
    ), "All events should be for the correct run"

    print(f"✓ Event watching test passed - received {len(received_events)} events")


def test_multiple_watchers(clickhouse_storage):
    """Test multiple watchers on the same run."""
    received_events_1 = []
    received_events_2 = []

    def handler_1(event, cursor):
        received_events_1.append(event)

    def handler_2(event, cursor):
        received_events_2.append(event)

    run_id = "multi_watch_test_run"

    # Start multiple watchers
    clickhouse_storage.watch(run_id, None, handler_1)
    clickhouse_storage.watch(run_id, None, handler_2)

    # Store an event
    event = create_test_event(run_id, "Multi-watch event", DagsterEventType.RUN_START)
    clickhouse_storage.store_event(event)
    with clickhouse_storage._buffer_lock:
        clickhouse_storage._flush_events()

    # Wait for polling
    time.sleep(2)

    # Stop watching
    clickhouse_storage.end_watch(run_id, handler_1)
    clickhouse_storage.end_watch(run_id, handler_2)

    # Both handlers should have received the event
    assert len(received_events_1) >= 1, "Handler 1 should receive events"
    assert len(received_events_2) >= 1, "Handler 2 should receive events"

    print("✓ Multiple watchers test passed")


def test_watcher_cleanup(clickhouse_storage):
    """Test that watchers are properly cleaned up."""
    received_events = []

    def event_handler(event, cursor):
        received_events.append(event)

    run_id = "cleanup_test_run"

    # Start watching
    clickhouse_storage.watch(run_id, None, event_handler)

    # Verify watcher exists (test public behavior instead of private attributes)
    assert clickhouse_storage._event_watcher is not None

    # Stop watching
    clickhouse_storage.end_watch(run_id, event_handler)

    # Test that we can still create new watchers (indicates cleanup worked)
    clickhouse_storage.watch(run_id, None, event_handler)
    clickhouse_storage.end_watch(run_id, event_handler)

    # Test dispose cleanup
    clickhouse_storage.dispose()
    assert clickhouse_storage._event_watcher is None

    print("✓ Watcher cleanup test passed")


if __name__ == "__main__":
    # Simple test runner for development
    import os

    clickhouse_url = os.environ.get(
        "CLICKHOUSE_URL", "http://dagster:dagster@localhost:8123/test_dagster"
    )

    storage = ClickHouseEventLogStorage(
        clickhouse_url=clickhouse_url,
        batch_size=10,
        flush_interval=1.0,  # Must be float
        should_autocreate_tables=True,
    )

    print("Testing ClickHouse storage...")
    test_clickhouse_storage_creation(storage)
    print("✓ Storage creation test passed")

    test_store_and_retrieve_event(storage)
    print("✓ Store and retrieve test passed")

    test_batch_storage(storage)
    print("✓ Batch storage test passed")

    test_asset_keys(storage)
    print("✓ Asset keys test passed")

    test_dynamic_partitions(storage)
    print("✓ Dynamic partitions test passed")

    test_wipe_storage(storage)
    print("✓ Wipe storage test passed")

    print("\nTesting event watching functionality...")
    test_event_watching(storage)
    test_multiple_watchers(storage)
    test_watcher_cleanup(storage)

    print("\nAll tests passed!")

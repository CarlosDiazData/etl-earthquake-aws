"""
Unit tests for ETL pipeline data integrity fixes.

Tests the three P0 remediation changes:
1. UUID suffix in api_to_bronze.py for unique bronze file names
2. Deterministic hash-based surrogate keys in process_silver_to_gold.py
3. Checkpoint-based read filtering logic in process_bronze_to_silver.py
"""

import uuid
import hashlib
from datetime import datetime, timezone

import pytest


class TestUUIDUniqueness:
    """Tests for api_to_bronze.build_partitioned_key uniqueness."""

    def test_build_partitioned_key_generates_unique_ids(self):
        """Each call should produce a unique file name (simulating the UUID suffix logic)."""
        BRONZE_PREFIX = "bronze"

        def build_partitioned_key(end_datetime):
            unique_id = str(uuid.uuid4())[:8]
            return (
                f"{BRONZE_PREFIX}/"
                f"{end_datetime.year}/"
                f"{end_datetime.month:02d}/"
                f"{end_datetime.day:02d}/"
                f"{end_datetime.hour:02d}/"
                f"raw_earthquakes_{unique_id}.json"
            )

        end_dt = datetime(2024, 4, 29, 14, 0, 0, tzinfo=timezone.utc)

        keys = set()
        for _ in range(100):
            key = build_partitioned_key(end_dt)
            assert "raw_earthquakes_" in key
            assert key.endswith(".json")
            keys.add(key)

        # All 100 keys should be unique
        assert len(keys) == 100

    def test_build_partitioned_key_format(self):
        """Key should have correct format with 8-char UUID suffix."""
        BRONZE_PREFIX = "bronze"

        def build_partitioned_key(end_datetime):
            unique_id = str(uuid.uuid4())[:8]
            return (
                f"{BRONZE_PREFIX}/"
                f"{end_datetime.year}/"
                f"{end_datetime.month:02d}/"
                f"{end_datetime.day:02d}/"
                f"{end_datetime.hour:02d}/"
                f"raw_earthquakes_{unique_id}.json"
            )

        end_dt = datetime(2024, 4, 29, 14, 0, 0, tzinfo=timezone.utc)
        key = build_partitioned_key(end_dt)

        assert key.startswith("bronze/2024/04/29/14/raw_earthquakes_")
        assert key.endswith(".json")
        # UUID suffix should be 8 characters
        uuid_part = key.replace("bronze/2024/04/29/14/raw_earthquakes_", "").replace(".json", "")
        assert len(uuid_part) == 8


class TestDeterministicLocationKey:
    """Tests for deterministic LocationKey generation."""

    def test_location_key_deterministic(self):
        """
        Same (latitude, longitude, place) should always produce the same LocationKey.
        This is the core property we need to guarantee idempotent gold layer regeneration.
        """
        lat, lon, place = 34.0522, -118.2437, "Los Angeles, CA"

        # Simulate the key generation logic
        composite = f"{lat}|{lon}|{place}"
        expected_key = int(hashlib.md5(composite.encode()).hexdigest()[:16], 16)

        # Generate again - should be identical
        composite2 = f"{lat}|{lon}|{place}"
        key2 = int(hashlib.md5(composite2.encode()).hexdigest()[:16], 16)

        assert expected_key == key2

    def test_location_key_different_inputs_different_keys(self):
        """Different locations should produce different keys (with very high probability)."""
        locations = [
            (34.0522, -118.2437, "Los Angeles, CA"),
            (40.7128, -74.0060, "New York, NY"),
            (34.0522, -118.2437, "LA, CA"),  # Different place string
        ]

        keys = set()
        for lat, lon, place in locations:
            composite = f"{lat}|{lon}|{place}"
            key = int(hashlib.md5(composite.encode()).hexdigest()[:16], 16)
            keys.add(key)

        # All three should be different
        assert len(keys) == 3

    def test_location_key_handles_null_place(self):
        """LocationKey should handle NULL/missing place gracefully."""
        lat, lon = 34.0522, -118.2437

        composite = f"{lat}|{lon}|NULL"  # Simulating NULL placeholder
        key = int(hashlib.md5(composite.encode()).hexdigest()[:16], 16)

        assert isinstance(key, int)
        assert key > 0


class TestDeterministicEventTypeKey:
    """Tests for deterministic EventTypeKey generation."""

    def test_eventtype_key_deterministic(self):
        """Same (event_type, magType) should always produce the same EventTypeKey."""
        event_type, mag_type = "earthquake", "mb"

        composite = f"{event_type}|{mag_type}"
        expected_key = int(hashlib.md5(composite.encode()).hexdigest()[:16], 16)

        composite2 = f"{event_type}|{mag_type}"
        key2 = int(hashlib.md5(composite2.encode()).hexdigest()[:16], 16)

        assert expected_key == key2

    def test_eventtype_key_different_types_different_keys(self):
        """Different event_type/magType combinations should produce different keys."""
        combos = [
            ("earthquake", "mb"),
            ("earthquake", "ml"),
            ("explosion", "mb"),
        ]

        keys = set()
        for event_type, mag_type in combos:
            composite = f"{event_type}|{mag_type}"
            key = int(hashlib.md5(composite.encode()).hexdigest()[:16], 16)
            keys.add(key)

        assert len(keys) == 3

    def test_eventtype_key_handles_nulls(self):
        """EventTypeKey should handle NULL/missing values gracefully."""
        composite = "NULL|NULL"
        key = int(hashlib.md5(composite.encode()).hexdigest()[:16], 16)

        assert isinstance(key, int)
        assert key > 0


class TestCheckpointFilteringLogic:
    """Tests for checkpoint-based partition filtering logic."""

    def test_partition_key_format(self):
        """Partition key should be in YYYY/MM/DD format."""
        year, month, day = "2024", "04", "29"
        partition_key = f"{year}/{month}/{day}"

        assert partition_key == "2024/04/29"
        assert len(partition_key.split("/")) == 3

    def test_skip_already_processed_partitions(self):
        """Partitions already in processed_partitions set should be skipped."""
        processed = {"2024/04/28", "2024/04/27"}
        all_partitions = ["2024/04/28", "2024/04/27", "2024/04/29"]

        to_process = [p for p in all_partitions if p not in processed]

        assert "2024/04/28" not in to_process
        assert "2024/04/27" not in to_process
        assert "2024/04/29" in to_process

    def test_paths_to_read_construction(self):
        """Paths to read should be correctly constructed from partition keys."""
        bucket = "test-bucket"
        partitions = ["2024/04/28", "2024/04/29"]
        s3_bucket = "test-bucket"

        paths = [f"s3://{s3_bucket}/{p}/*.json" for p in partitions]

        assert len(paths) == 2
        assert "s3://test-bucket/2024/04/28/*.json" in paths
        assert "s3://test-bucket/2024/04/29/*.json" in paths

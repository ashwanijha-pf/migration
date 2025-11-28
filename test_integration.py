"""
Integration tests for migration scripts.

These tests create real source and destination objects, run the migration logic,
and compare the results to verify correctness. Unlike unit tests which mock
everything, these tests verify actual data transformations.

Tests verify:
- Data type consistency between source and destination
- Field mapping correctness
- State type transformations
- Catalog ID prefixing
- Reference PK transformations
- Hash exclusions
- Data structure preservation
"""

import unittest
from unittest.mock import Mock, MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, MapType, IntegerType,
    ArrayType, LongType, DoubleType
)
from pyspark.sql import functions as F
import json


class TestClientReferenceIntegration(unittest.TestCase):
    """Integration tests for client reference migration."""

    @classmethod
    def setUpClass(cls):
        """Set up Spark session for all tests."""
        cls.spark = SparkSession.builder \
            .appName("IntegrationTests") \
            .master("local[2]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """Tear down Spark session."""
        cls.spark.stop()

    def test_reference_pk_transformation(self):
        """Test REFERENCE# PK transformation with real data."""
        from search import transform_reference_pk, prefixed_catalog_id

        # Create source data
        source_data = [
            {"PK": "REFERENCE#FD-S-1005", "listing_id": "12345", "data": "value1"},
            {"PK": "REFERENCE#ABC-123", "listing_id": "67890", "data": "value2"},
        ]
        source_df = self.spark.createDataFrame(source_data)

        # Apply transformations
        country_code = "AE"
        result_df = source_df.withColumn(
            "PK",
            transform_reference_pk(F.lit(country_code), F.col("PK"))
        ).withColumn(
            "catalog_id",
            prefixed_catalog_id(F.lit(country_code), F.col("listing_id"))
        )

        # Collect results
        results = result_df.collect()

        # Verify transformations
        self.assertEqual(results[0]["PK"], "AE#REFERENCE#FD-S-1005")
        self.assertEqual(results[0]["catalog_id"], "AE#12345")
        self.assertEqual(results[1]["PK"], "AE#REFERENCE#ABC-123")
        self.assertEqual(results[1]["catalog_id"], "AE#67890")

        # Verify data types match
        self.assertEqual(type(results[0]["PK"]), str)
        self.assertEqual(type(results[0]["catalog_id"]), str)
        self.assertEqual(type(results[0]["data"]), str)

    def test_reference_data_preservation(self):
        """Test that non-key fields are preserved during migration."""
        from search import transform_reference_pk

        # Create source with various data types
        source_data = [
            {
                "PK": "REFERENCE#TEST-1",
                "listing_id": "123",
                "reference_number": "REF-001",
                "created_at": 1234567890,
                "metadata": {"key": "value"}
            }
        ]
        source_df = self.spark.createDataFrame(source_data)

        # Apply transformation
        result_df = source_df.withColumn(
            "PK",
            transform_reference_pk(F.lit("US"), F.col("PK"))
        )

        result = result_df.collect()[0]

        # Verify PK transformed
        self.assertEqual(result["PK"], "US#REFERENCE#TEST-1")

        # Verify other fields preserved
        self.assertEqual(result["listing_id"], "123")
        self.assertEqual(result["reference_number"], "REF-001")
        self.assertEqual(result["created_at"], 1234567890)
        self.assertEqual(result["metadata"], {"key": "value"})


class TestStateTypeIntegration(unittest.TestCase):
    """Integration tests for state type mapping."""

    @classmethod
    def setUpClass(cls):
        """Set up Spark session for all tests."""
        cls.spark = SparkSession.builder \
            .appName("StateTypeIntegrationTests") \
            .master("local[2]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """Tear down Spark session."""
        cls.spark.stop()

    def test_state_type_mapping_transformations(self):
        """Test state type transformations with real data structures."""
        from search import map_state_type_expr, STATE_TYPE_MAP

        # Create source data with various state types
        source_data = [
            {"state_type": "pending_approval", "data": {"type": "pending_approval"}},
            {"state_type": "publishing_failed", "data": {"type": "publishing_failed"}},
            {"state_type": "live", "data": {"type": "live"}},
            {"state_type": "pending_publishing", "data": {"type": "pending_publishing"}},
        ]
        source_df = self.spark.createDataFrame(source_data)

        # Apply mapping
        result_df = map_state_type_expr(source_df)
        results = result_df.collect()

        # Verify transformations match STATE_TYPE_MAP
        self.assertEqual(results[0]["state_type"], "draft_pending_approval")
        self.assertEqual(results[0]["data"]["type"], "draft_pending_approval")

        self.assertEqual(results[1]["state_type"], "allocation_failed")
        self.assertEqual(results[1]["data"]["type"], "allocation_failed")

        self.assertEqual(results[2]["state_type"], "live")
        self.assertEqual(results[2]["data"]["type"], "live")

        self.assertEqual(results[3]["state_type"], "validation_requested")
        self.assertEqual(results[3]["data"]["type"], "validation_requested")

    def test_state_reasons_preservation(self):
        """Test that state reasons are preserved as array of structs."""
        from search import map_state_type_expr

        # Create source with reasons
        source_data = [
            {
                "state_type": "rejected",
                "data": {
                    "type": "rejected",
                    "reasons": [
                        {"ar": "سبب بالعربي", "en": "Reason in English"},
                        {"ar": "سبب آخر", "en": "Another reason"}
                    ]
                }
            }
        ]
        source_df = self.spark.createDataFrame(source_data)

        # Apply mapping
        result_df = map_state_type_expr(source_df)
        result = result_df.collect()[0]

        # Verify reasons preserved
        self.assertEqual(len(result["data"]["reasons"]), 2)
        self.assertEqual(result["data"]["reasons"][0]["ar"], "سبب بالعربي")
        self.assertEqual(result["data"]["reasons"][0]["en"], "Reason in English")
        self.assertEqual(result["data"]["reasons"][1]["ar"], "سبب آخر")
        self.assertEqual(result["data"]["reasons"][1]["en"], "Another reason")

    def test_state_type_case_insensitivity(self):
        """Test that state type mapping is case-insensitive."""
        from search import map_state_type_expr

        # Create source with mixed case
        source_data = [
            {"state_type": "LIVE", "data": {"type": "LIVE"}},
            {"state_type": "Live", "data": {"type": "Live"}},
            {"state_type": "live", "data": {"type": "live"}},
        ]
        source_df = self.spark.createDataFrame(source_data)

        # Apply mapping
        result_df = map_state_type_expr(source_df)
        results = result_df.collect()

        # All should map to lowercase "live"
        for result in results:
            self.assertEqual(result["state_type"], "live")
            self.assertEqual(result["data"]["type"], "live")


class TestMetadataHashesIntegration(unittest.TestCase):
    """Integration tests for metadata hashes filtering."""

    @classmethod
    def setUpClass(cls):
        """Set up Spark session for all tests."""
        cls.spark = SparkSession.builder \
            .appName("HashesIntegrationTests") \
            .master("local[2]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """Tear down Spark session."""
        cls.spark.stop()

    def test_hash_exclusions(self):
        """Test that excluded hashes are removed."""
        from search import strip_metadata_hashes_sql, HASH_EXCLUSIONS

        # Create source with hashes including exclusions
        source_data = [
            {
                "listing_id": "123",
                "hashes": {
                    "media": "hash1",
                    "quality_score": "hash2",
                    "compliance": "hash3",
                    "title": "hash4",
                    "description": "hash5"
                }
            }
        ]
        source_df = self.spark.createDataFrame(source_data)

        # Apply hash stripping
        result_df = strip_metadata_hashes_sql(source_df)
        result = result_df.collect()[0]

        # Verify exclusions removed
        self.assertNotIn("media", result["hashes"])
        self.assertNotIn("quality_score", result["hashes"])
        self.assertNotIn("compliance", result["hashes"])

        # Verify valid hashes preserved
        self.assertIn("title", result["hashes"])
        self.assertIn("description", result["hashes"])
        self.assertEqual(result["hashes"]["title"], "hash4")
        self.assertEqual(result["hashes"]["description"], "hash5")

    def test_empty_hashes_nulled(self):
        """Test that empty hashes map is converted to null."""
        from search import _null_out_empty_hashes

        # Create source with empty hashes
        source_data = [
            {"listing_id": "123", "hashes": {}},
            {"listing_id": "456", "hashes": {"title": "hash1"}},
        ]
        source_df = self.spark.createDataFrame(source_data)

        # Apply null conversion
        result_df = _null_out_empty_hashes(source_df)
        results = result_df.collect()

        # Verify empty map nulled
        self.assertIsNone(results[0]["hashes"])

        # Verify non-empty map preserved
        self.assertIsNotNone(results[1]["hashes"])
        self.assertEqual(results[1]["hashes"]["title"], "hash1")


class TestCatalogIdIntegration(unittest.TestCase):
    """Integration tests for catalog ID transformations."""

    @classmethod
    def setUpClass(cls):
        """Set up Spark session for all tests."""
        cls.spark = SparkSession.builder \
            .appName("CatalogIdIntegrationTests") \
            .master("local[2]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """Tear down Spark session."""
        cls.spark.stop()

    def test_catalog_id_prefixing(self):
        """Test catalog_id prefixing with country code."""
        from search import prefixed_catalog_id

        # Create source data
        source_data = [
            {"listing_id": "12345", "segment": "SEGMENT#METADATA"},
            {"listing_id": "67890", "segment": "SEGMENT#PRICE"},
            {"listing_id": "11111", "segment": "SEGMENT#STATE"},
        ]
        source_df = self.spark.createDataFrame(source_data)

        # Apply catalog_id transformation for different countries
        for country in ["AE", "US", "GB"]:
            result_df = source_df.withColumn(
                "catalog_id",
                prefixed_catalog_id(F.lit(country), F.col("listing_id"))
            )
            results = result_df.collect()

            # Verify format
            self.assertEqual(results[0]["catalog_id"], f"{country}#12345")
            self.assertEqual(results[1]["catalog_id"], f"{country}#67890")
            self.assertEqual(results[2]["catalog_id"], f"{country}#11111")

    def test_catalog_id_replaces_listing_id(self):
        """Test that catalog_id replaces listing_id in target."""
        from search import prefixed_catalog_id

        # Create source
        source_data = [
            {"listing_id": "123", "data": "value", "segment": "SEGMENT#METADATA"}
        ]
        source_df = self.spark.createDataFrame(source_data)

        # Transform
        result_df = source_df.withColumn(
            "catalog_id",
            prefixed_catalog_id(F.lit("AE"), F.col("listing_id"))
        ).drop("listing_id")

        result = result_df.collect()[0]

        # Verify listing_id removed and catalog_id present
        self.assertNotIn("listing_id", result.asDict())
        self.assertEqual(result["catalog_id"], "AE#123")


class TestSegmentDataIntegration(unittest.TestCase):
    """Integration tests for segment-specific data handling."""

    @classmethod
    def setUpClass(cls):
        """Set up Spark session for all tests."""
        cls.spark = SparkSession.builder \
            .appName("SegmentDataIntegrationTests") \
            .master("local[2]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """Tear down Spark session."""
        cls.spark.stop()

    def test_amenities_array_preservation(self):
        """Test that AMENITIES data is preserved as array."""
        # Create source with amenities
        source_data = [
            {
                "listing_id": "123",
                "segment": "SEGMENT#AMENITIES",
                "data": ["WiFi", "Pool", "Parking", "Gym"]
            }
        ]
        source_df = self.spark.createDataFrame(source_data)

        # Verify data type
        result = source_df.collect()[0]
        self.assertIsInstance(result["data"], list)
        self.assertEqual(len(result["data"]), 4)
        self.assertIn("WiFi", result["data"])
        self.assertIn("Pool", result["data"])

    def test_attributes_struct_preservation(self):
        """Test that ATTRIBUTES data is preserved as struct."""
        # Create source with attributes
        source_data = [
            {
                "listing_id": "123",
                "segment": "SEGMENT#ATTRIBUTES",
                "data": {
                    "bedrooms": 3,
                    "bathrooms": 2,
                    "area": 1500.5,
                    "furnished": "yes"
                }
            }
        ]
        source_df = self.spark.createDataFrame(source_data)

        # Verify data type
        result = source_df.collect()[0]
        self.assertIsInstance(result["data"], dict)
        self.assertEqual(result["data"]["bedrooms"], 3)
        self.assertEqual(result["data"]["bathrooms"], 2)
        self.assertEqual(result["data"]["area"], 1500.5)
        self.assertEqual(result["data"]["furnished"], "yes")

    def test_price_struct_preservation(self):
        """Test that PRICE data is preserved as struct."""
        # Create source with price
        source_data = [
            {
                "listing_id": "123",
                "segment": "SEGMENT#PRICE",
                "data": {
                    "amount": 5000,
                    "currency": "AED",
                    "period": "monthly"
                }
            }
        ]
        source_df = self.spark.createDataFrame(source_data)

        # Verify data type
        result = source_df.collect()[0]
        self.assertIsInstance(result["data"], dict)
        self.assertEqual(result["data"]["amount"], 5000)
        self.assertEqual(result["data"]["currency"], "AED")
        self.assertEqual(result["data"]["period"], "monthly")


class TestEndToEndMigration(unittest.TestCase):
    """End-to-end integration tests simulating full migration flow."""

    @classmethod
    def setUpClass(cls):
        """Set up Spark session for all tests."""
        cls.spark = SparkSession.builder \
            .appName("EndToEndIntegrationTests") \
            .master("local[2]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """Tear down Spark session."""
        cls.spark.stop()

    def test_complete_listing_migration(self):
        """Test complete migration of a listing with all segments."""
        from search import (
            prefixed_catalog_id,
            map_state_type_expr,
            strip_metadata_hashes_sql,
            _null_out_empty_hashes
        )

        # Create source data for a complete listing
        listing_id = "12345"
        country_code = "AE"

        # METADATA segment
        metadata_source = [
            {
                "listing_id": listing_id,
                "segment": "SEGMENT#METADATA",
                "client_id": "client_123",
                "hashes": {
                    "media": "hash_media",
                    "quality_score": "hash_qs",
                    "title": "hash_title",
                    "description": "hash_desc"
                }
            }
        ]

        # STATE segment
        state_source = [
            {
                "listing_id": listing_id,
                "segment": "SEGMENT#STATE",
                "state_type": "pending_approval",
                "data": {
                    "type": "pending_approval",
                    "reasons": [{"ar": "سبب", "en": "reason"}]
                }
            }
        ]

        # AMENITIES segment
        amenities_source = [
            {
                "listing_id": listing_id,
                "segment": "SEGMENT#AMENITIES",
                "data": ["WiFi", "Pool", "Gym"]
            }
        ]

        # PRICE segment
        price_source = [
            {
                "listing_id": listing_id,
                "segment": "SEGMENT#PRICE",
                "data": {
                    "amount": 5000,
                    "currency": "AED"
                }
            }
        ]

        # Process METADATA
        metadata_df = self.spark.createDataFrame(metadata_source)
        metadata_df = metadata_df.withColumn(
            "catalog_id",
            prefixed_catalog_id(F.lit(country_code), F.col("listing_id"))
        ).drop("listing_id")
        metadata_df = strip_metadata_hashes_sql(metadata_df)
        metadata_df = _null_out_empty_hashes(metadata_df)
        metadata_result = metadata_df.collect()[0]

        # Process STATE
        state_df = self.spark.createDataFrame(state_source)
        state_df = state_df.withColumn(
            "catalog_id",
            prefixed_catalog_id(F.lit(country_code), F.col("listing_id"))
        ).drop("listing_id")
        state_df = map_state_type_expr(state_df)
        state_result = state_df.collect()[0]

        # Process AMENITIES
        amenities_df = self.spark.createDataFrame(amenities_source)
        amenities_df = amenities_df.withColumn(
            "catalog_id",
            prefixed_catalog_id(F.lit(country_code), F.col("listing_id"))
        ).drop("listing_id")
        amenities_result = amenities_df.collect()[0]

        # Process PRICE
        price_df = self.spark.createDataFrame(price_source)
        price_df = price_df.withColumn(
            "catalog_id",
            prefixed_catalog_id(F.lit(country_code), F.col("listing_id"))
        ).drop("listing_id")
        price_result = price_df.collect()[0]

        # Verify METADATA transformations
        self.assertEqual(metadata_result["catalog_id"], f"{country_code}#{listing_id}")
        self.assertNotIn("media", metadata_result["hashes"])
        self.assertNotIn("quality_score", metadata_result["hashes"])
        self.assertIn("title", metadata_result["hashes"])
        self.assertIn("description", metadata_result["hashes"])

        # Verify STATE transformations
        self.assertEqual(state_result["catalog_id"], f"{country_code}#{listing_id}")
        self.assertEqual(state_result["state_type"], "draft_pending_approval")
        self.assertEqual(state_result["data"]["type"], "draft_pending_approval")
        self.assertEqual(len(state_result["data"]["reasons"]), 1)

        # Verify AMENITIES preservation
        self.assertEqual(amenities_result["catalog_id"], f"{country_code}#{listing_id}")
        self.assertEqual(len(amenities_result["data"]), 3)
        self.assertIn("WiFi", amenities_result["data"])

        # Verify PRICE preservation
        self.assertEqual(price_result["catalog_id"], f"{country_code}#{listing_id}")
        self.assertEqual(price_result["data"]["amount"], 5000)
        self.assertEqual(price_result["data"]["currency"], "AED")

    def test_source_destination_type_consistency(self):
        """Test that source and destination have consistent data types."""
        from search import prefixed_catalog_id, map_state_type_expr

        # Create source with various data types
        source_data = [
            {
                "listing_id": "123",
                "segment": "SEGMENT#STATE",
                "state_type": "live",
                "data": {"type": "live"},
                "created_at": 1234567890,
                "updated_at": 1234567900,
                "version": 1
            }
        ]
        source_df = self.spark.createDataFrame(source_data)

        # Apply transformations
        dest_df = source_df.withColumn(
            "catalog_id",
            prefixed_catalog_id(F.lit("AE"), F.col("listing_id"))
        ).drop("listing_id")
        dest_df = map_state_type_expr(dest_df)

        source_result = source_df.collect()[0]
        dest_result = dest_df.collect()[0]

        # Verify type consistency for preserved fields
        self.assertEqual(type(source_result["created_at"]), type(dest_result["created_at"]))
        self.assertEqual(type(source_result["updated_at"]), type(dest_result["updated_at"]))
        self.assertEqual(type(source_result["version"]), type(dest_result["version"]))
        self.assertEqual(type(source_result["segment"]), type(dest_result["segment"]))

        # Verify values preserved
        self.assertEqual(source_result["created_at"], dest_result["created_at"])
        self.assertEqual(source_result["updated_at"], dest_result["updated_at"])
        self.assertEqual(source_result["version"], dest_result["version"])


class TestDataComparison(unittest.TestCase):
    """Tests that compare source and destination data structures."""

    @classmethod
    def setUpClass(cls):
        """Set up Spark session for all tests."""
        cls.spark = SparkSession.builder \
            .appName("DataComparisonTests") \
            .master("local[2]") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """Tear down Spark session."""
        cls.spark.stop()

    def compare_schemas(self, source_df, dest_df, expected_changes):
        """
        Helper to compare schemas between source and destination.

        Args:
            source_df: Source DataFrame
            dest_df: Destination DataFrame
            expected_changes: Dict of expected field changes
                {
                    "added": ["field1", "field2"],
                    "removed": ["field3"],
                    "renamed": {"old_name": "new_name"}
                }
        """
        source_fields = set(source_df.columns)
        dest_fields = set(dest_df.columns)

        # Check added fields
        added = dest_fields - source_fields
        expected_added = set(expected_changes.get("added", []))
        self.assertEqual(added, expected_added, f"Added fields mismatch. Expected: {expected_added}, Got: {added}")

        # Check removed fields
        removed = source_fields - dest_fields
        expected_removed = set(expected_changes.get("removed", []))
        # Account for renamed fields
        for old, new in expected_changes.get("renamed", {}).items():
            if old in removed and new in added:
                removed.discard(old)
                added.discard(new)
        self.assertEqual(removed, expected_removed, f"Removed fields mismatch. Expected: {expected_removed}, Got: {removed}")

    def test_reference_schema_changes(self):
        """Test schema changes for reference migration."""
        from search import transform_reference_pk, prefixed_catalog_id

        # Source schema
        source_data = [
            {"PK": "REFERENCE#TEST", "listing_id": "123", "data": "value"}
        ]
        source_df = self.spark.createDataFrame(source_data)

        # Destination schema
        dest_df = source_df.withColumn(
            "PK",
            transform_reference_pk(F.lit("AE"), F.col("PK"))
        ).withColumn(
            "catalog_id",
            prefixed_catalog_id(F.lit("AE"), F.col("listing_id"))
        ).drop("listing_id")

        # Expected changes
        expected_changes = {
            "added": ["catalog_id"],
            "removed": ["listing_id"]
        }

        self.compare_schemas(source_df, dest_df, expected_changes)

    def test_state_schema_preservation(self):
        """Test that STATE segment schema is preserved (only values change)."""
        from search import map_state_type_expr

        # Source schema
        source_data = [
            {
                "listing_id": "123",
                "segment": "SEGMENT#STATE",
                "state_type": "pending_approval",
                "data": {"type": "pending_approval"}
            }
        ]
        source_df = self.spark.createDataFrame(source_data)

        # Destination schema
        dest_df = map_state_type_expr(source_df)

        # Schema should be identical (only values change)
        expected_changes = {
            "added": [],
            "removed": []
        }

        self.compare_schemas(source_df, dest_df, expected_changes)

    def test_metadata_schema_changes(self):
        """Test schema changes for metadata migration."""
        from search import strip_metadata_hashes_sql, prefixed_catalog_id

        # Source schema
        source_data = [
            {
                "listing_id": "123",
                "segment": "SEGMENT#METADATA",
                "hashes": {"media": "h1", "title": "h2"}
            }
        ]
        source_df = self.spark.createDataFrame(source_data)

        # Destination schema
        dest_df = source_df.withColumn(
            "catalog_id",
            prefixed_catalog_id(F.lit("AE"), F.col("listing_id"))
        ).drop("listing_id")
        dest_df = strip_metadata_hashes_sql(dest_df)

        # Expected changes
        expected_changes = {
            "added": ["catalog_id"],
            "removed": ["listing_id"]
        }

        self.compare_schemas(source_df, dest_df, expected_changes)


if __name__ == "__main__":
    unittest.main()

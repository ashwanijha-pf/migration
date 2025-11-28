"""
Unit tests for catalog.py migration script.

Tests cover:
- Argument parsing utilities
- DynamoDB batch deletion with retry logic
- Deletion functions for catalog and util tables
- Main orchestration logic
"""

import unittest
from unittest.mock import Mock, MagicMock, patch, call
from botocore.exceptions import ClientError
import sys


class TestResolveOptional(unittest.TestCase):
    """Test the _resolve_optional argument parsing function."""

    def test_resolve_optional_with_space_separator(self):
        """Test parsing --KEY value format."""
        from catalog import _resolve_optional
        
        argv = ["script.py", "--TEST_KEY", "test_value", "--OTHER", "other"]
        result = _resolve_optional(argv, "TEST_KEY", "default")
        self.assertEqual(result, "test_value")

    def test_resolve_optional_with_equals_separator(self):
        """Test parsing --KEY=value format."""
        from catalog import _resolve_optional
        
        argv = ["script.py", "--TEST_KEY=test_value", "--OTHER=other"]
        result = _resolve_optional(argv, "TEST_KEY", "default")
        self.assertEqual(result, "test_value")

    def test_resolve_optional_returns_default(self):
        """Test that default value is returned when key not found."""
        from catalog import _resolve_optional
        
        argv = ["script.py", "--OTHER", "other"]
        result = _resolve_optional(argv, "TEST_KEY", "default_value")
        self.assertEqual(result, "default_value")

    def test_resolve_optional_with_empty_value(self):
        """Test parsing when value is empty string."""
        from catalog import _resolve_optional
        
        argv = ["script.py", "--TEST_KEY="]
        result = _resolve_optional(argv, "TEST_KEY", "default")
        self.assertEqual(result, "")

    def test_resolve_optional_at_end_of_argv(self):
        """Test when key is at end without value."""
        from catalog import _resolve_optional
        
        argv = ["script.py", "--TEST_KEY"]
        result = _resolve_optional(argv, "TEST_KEY", "default")
        self.assertEqual(result, "default")


class TestDeleteBatch(unittest.TestCase):
    """Test the _delete_batch DynamoDB deletion function."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_ddb = Mock()
        self.table_name = "test-table"
        self.requests = [
            {"DeleteRequest": {"Key": {"pk": {"S": f"key{i}"}}}}
            for i in range(3)
        ]

    def test_delete_batch_success(self):
        """Test successful batch deletion without retries."""
        from catalog import _delete_batch
        
        # Mock successful response
        self.mock_ddb.batch_write_item.return_value = {
            "UnprocessedItems": {}
        }
        
        deleted, throttled = _delete_batch(
            self.mock_ddb, self.table_name, self.requests
        )
        
        self.assertEqual(deleted, 3)
        self.assertEqual(len(throttled), 0)
        self.mock_ddb.batch_write_item.assert_called_once()

    def test_delete_batch_with_unprocessed_items(self):
        """Test batch deletion with unprocessed items that succeed on retry."""
        from catalog import _delete_batch
        
        # First call has unprocessed items, second succeeds
        self.mock_ddb.batch_write_item.side_effect = [
            {
                "UnprocessedItems": {
                    self.table_name: [self.requests[2]]
                }
            },
            {"UnprocessedItems": {}}
        ]
        
        with patch('time.sleep'):  # Skip actual sleep
            deleted, throttled = _delete_batch(
                self.mock_ddb, self.table_name, self.requests
            )
        
        self.assertEqual(deleted, 3)
        self.assertEqual(len(throttled), 0)
        self.assertEqual(self.mock_ddb.batch_write_item.call_count, 2)

    def test_delete_batch_with_throttle_exception(self):
        """Test batch deletion with throttle exception."""
        from catalog import _delete_batch
        
        # First call raises throttle exception, second succeeds
        throttle_error = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException"}},
            "batch_write_item"
        )
        self.mock_ddb.batch_write_item.side_effect = [
            throttle_error,
            {"UnprocessedItems": {}}
        ]
        
        with patch('time.sleep'):
            deleted, throttled = _delete_batch(
                self.mock_ddb, self.table_name, self.requests
            )
        
        self.assertEqual(deleted, 3)
        self.assertEqual(len(throttled), 0)

    def test_delete_batch_max_retries_exceeded(self):
        """Test batch deletion when max retries are exceeded."""
        from catalog import _delete_batch
        
        # Always return unprocessed items
        self.mock_ddb.batch_write_item.return_value = {
            "UnprocessedItems": {
                self.table_name: [self.requests[0]]
            }
        }
        
        # Mock delete_item to fail as well
        delete_error = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException"}},
            "delete_item"
        )
        self.mock_ddb.delete_item.side_effect = delete_error
        
        with patch('time.sleep'):
            deleted, throttled = _delete_batch(
                self.mock_ddb, self.table_name, self.requests, max_retries=2
            )
        
        # Should have some throttled items
        self.assertGreater(len(throttled), 0)

    def test_delete_batch_fallback_to_individual_deletes(self):
        """Test fallback to individual delete_item calls."""
        from catalog import _delete_batch
        
        # Batch write always fails, but individual deletes succeed
        self.mock_ddb.batch_write_item.return_value = {
            "UnprocessedItems": {
                self.table_name: self.requests
            }
        }
        self.mock_ddb.delete_item.return_value = {}
        
        with patch('time.sleep'):
            deleted, throttled = _delete_batch(
                self.mock_ddb, self.table_name, self.requests, max_retries=1
            )
        
        # Should have attempted individual deletes
        self.assertGreater(self.mock_ddb.delete_item.call_count, 0)


class TestDeleteCatalogForCountry(unittest.TestCase):
    """Test the delete_catalog_for_country function."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_glue_context = Mock()
        self.mock_logger = Mock()
        self.country_code = "US"
        self.target_table = "target-catalog-table"
        self.target_region = "us-east-1"

    @patch('catalog.read_ddb_table')
    @patch('catalog.get_table_key_attrs')
    @patch('catalog.boto3.client')
    @patch('catalog._delete_batch')
    def test_delete_catalog_no_records(
        self, mock_delete_batch, mock_boto3, mock_get_keys, mock_read_ddb
    ):
        """Test deletion when no records exist."""
        from catalog import delete_catalog_for_country
        
        # Mock empty dataframe
        mock_dyf = Mock()
        mock_df = Mock()
        mock_df.rdd.isEmpty.return_value = True
        mock_dyf.toDF.return_value = mock_df
        mock_read_ddb.return_value = mock_dyf
        
        delete_catalog_for_country(
            self.mock_glue_context,
            self.mock_logger,
            self.country_code,
            self.target_table,
            self.target_region
        )
        
        # Should not attempt deletion
        mock_delete_batch.assert_not_called()

    @patch('catalog.read_ddb_table')
    @patch('catalog.get_table_key_attrs')
    @patch('catalog.boto3.client')
    @patch('catalog._delete_batch')
    def test_delete_catalog_with_test_listing_ids(
        self, mock_delete_batch, mock_boto3, mock_get_keys, mock_read_ddb
    ):
        """Test deletion with specific listing IDs."""
        from catalog import delete_catalog_for_country
        
        # Mock dataframe with records
        mock_dyf = Mock()
        mock_df = Mock()
        mock_df.rdd.isEmpty.return_value = False
        mock_df.count.return_value = 2
        mock_df.filter.return_value = mock_df
        mock_df.select.return_value = mock_df
        
        # Mock collect to return keys
        mock_row1 = Mock()
        mock_row1.__getitem__ = Mock(side_effect=lambda k: "US#123" if k == "catalog_id" else None)
        mock_row2 = Mock()
        mock_row2.__getitem__ = Mock(side_effect=lambda k: "US#456" if k == "catalog_id" else None)
        mock_df.collect.return_value = [mock_row1, mock_row2]
        
        mock_dyf.toDF.return_value = mock_df
        mock_read_ddb.return_value = mock_dyf
        
        mock_get_keys.return_value = ["catalog_id"]
        mock_delete_batch.return_value = (2, [])
        
        delete_catalog_for_country(
            self.mock_glue_context,
            self.mock_logger,
            self.country_code,
            self.target_table,
            self.target_region,
            test_listing_ids=["123", "456"]
        )
        
        # Should have filtered by listing IDs
        mock_df.filter.assert_called()
        # Should have called delete batch
        mock_delete_batch.assert_called()

    @patch('catalog.read_ddb_table')
    @patch('catalog.get_table_key_attrs')
    @patch('catalog.get_listing_ids_for_clients')
    @patch('catalog.boto3.client')
    @patch('catalog._delete_batch')
    def test_delete_catalog_with_client_ids(
        self, mock_delete_batch, mock_boto3, mock_get_listing_ids,
        mock_get_keys, mock_read_ddb
    ):
        """Test deletion with client IDs (should look up listing IDs)."""
        from catalog import delete_catalog_for_country
        
        # Mock listing ID lookup
        mock_get_listing_ids.return_value = ["123", "456"]
        
        # Mock dataframe
        mock_dyf = Mock()
        mock_df = Mock()
        mock_df.rdd.isEmpty.return_value = False
        mock_df.count.return_value = 2
        mock_df.filter.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.collect.return_value = []
        mock_dyf.toDF.return_value = mock_df
        mock_read_ddb.return_value = mock_dyf
        
        mock_get_keys.return_value = ["catalog_id"]
        mock_delete_batch.return_value = (0, [])
        
        delete_catalog_for_country(
            self.mock_glue_context,
            self.mock_logger,
            self.country_code,
            self.target_table,
            self.target_region,
            test_client_ids=["client1", "client2"],
            source_listings_table="source-table",
            source_region="us-west-2"
        )
        
        # Should have looked up listing IDs
        mock_get_listing_ids.assert_called_once_with(
            self.mock_glue_context,
            self.mock_logger,
            self.country_code,
            "source-table",
            "us-west-2",
            ["client1", "client2"]
        )


class TestDeleteUtilForCountry(unittest.TestCase):
    """Test the delete_util_for_country function."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_glue_context = Mock()
        self.mock_logger = Mock()
        self.country_code = "US"
        self.target_table = "target-util-table"
        self.target_region = "us-east-1"

    @patch('catalog.read_ddb_table')
    @patch('catalog.get_table_key_attrs')
    @patch('catalog.boto3.client')
    @patch('catalog._delete_batch')
    def test_delete_util_no_records(
        self, mock_delete_batch, mock_boto3, mock_get_keys, mock_read_ddb
    ):
        """Test util deletion when no records exist."""
        from catalog import delete_util_for_country
        
        # Mock empty dataframe
        mock_dyf = Mock()
        mock_df = Mock()
        mock_df.rdd.isEmpty.return_value = True
        mock_dyf.toDF.return_value = mock_df
        mock_read_ddb.return_value = mock_dyf
        
        delete_util_for_country(
            self.mock_glue_context,
            self.mock_logger,
            self.country_code,
            self.target_table,
            self.target_region
        )
        
        # Should not attempt deletion
        mock_delete_batch.assert_not_called()

    @patch('catalog.read_ddb_table')
    @patch('catalog.get_table_key_attrs')
    @patch('catalog.boto3.client')
    @patch('catalog._delete_batch')
    def test_delete_util_with_filters(
        self, mock_delete_batch, mock_boto3, mock_get_keys, mock_read_ddb
    ):
        """Test util deletion with listing ID and client ID filters."""
        from catalog import delete_util_for_country
        
        # Mock dataframe with records
        mock_dyf = Mock()
        mock_df = Mock()
        mock_df.rdd.isEmpty.return_value = False
        mock_df.count.return_value = 5
        mock_df.columns = ["catalog_id", "client_id", "pk"]
        mock_df.filter.return_value = mock_df
        mock_df.select.return_value = mock_df
        mock_df.collect.return_value = []
        mock_dyf.toDF.return_value = mock_df
        mock_read_ddb.return_value = mock_dyf
        
        mock_get_keys.return_value = ["pk"]
        mock_delete_batch.return_value = (0, [])
        
        delete_util_for_country(
            self.mock_glue_context,
            self.mock_logger,
            self.country_code,
            self.target_table,
            self.target_region,
            test_listing_ids=["123"],
            test_client_ids=["client1"]
        )
        
        # Should have applied both filters
        self.assertEqual(mock_df.filter.call_count, 2)


class TestMainFunction(unittest.TestCase):
    """Test the main orchestration function."""

    @patch('catalog.init_spark')
    @patch('catalog.delete_catalog_for_country')
    @patch('catalog.delete_util_for_country')
    @patch('catalog.run_migration')
    def test_main_delete_only_mode(
        self, mock_run_migration, mock_delete_util, mock_delete_catalog, mock_init
    ):
        """Test main function in DELETE_ONLY mode."""
        from catalog import main
        
        # Mock init_spark
        mock_spark = Mock()
        mock_glue_context = Mock()
        mock_job = Mock()
        mock_logger = Mock()
        mock_args = {
            "COUNTRY_CODE": "us",
            "SOURCE_LISTINGS_TABLE": "source-listings",
            "SOURCE_REGION": "us-west-2",
            "TARGET_CATALOG_TABLE": "target-catalog",
            "TARGET_UTIL_TABLE": "target-util",
            "TARGET_REGION": "us-east-1",
            "TEST_LISTING_IDS": ["123"],
            "TEST_CLIENT_IDS": [],
            "DELETE_ONLY": True,
            "DELETE_ALL": False,
            "CLEAN_FIRST": False
        }
        mock_init.return_value = (mock_spark, mock_glue_context, mock_job, mock_args, mock_logger)
        
        main()
        
        # Should call delete functions
        mock_delete_catalog.assert_called_once()
        mock_delete_util.assert_called_once()
        # Should NOT run migration
        mock_run_migration.assert_not_called()
        # Should commit job
        mock_job.commit.assert_called_once()

    @patch('catalog.init_spark')
    @patch('catalog.delete_catalog_for_country')
    @patch('catalog.delete_util_for_country')
    @patch('catalog.run_migration')
    def test_main_delete_all_mode(
        self, mock_run_migration, mock_delete_util, mock_delete_catalog, mock_init
    ):
        """Test main function in DELETE_ALL mode."""
        from catalog import main
        
        # Mock init_spark
        mock_spark = Mock()
        mock_glue_context = Mock()
        mock_job = Mock()
        mock_logger = Mock()
        mock_args = {
            "COUNTRY_CODE": "us",
            "SOURCE_LISTINGS_TABLE": "source-listings",
            "SOURCE_REGION": "us-west-2",
            "TARGET_CATALOG_TABLE": "target-catalog",
            "TARGET_UTIL_TABLE": "target-util",
            "TARGET_REGION": "us-east-1",
            "TEST_LISTING_IDS": ["123"],  # Should be ignored
            "TEST_CLIENT_IDS": ["client1"],  # Should be ignored
            "DELETE_ONLY": True,
            "DELETE_ALL": True,
            "CLEAN_FIRST": False
        }
        mock_init.return_value = (mock_spark, mock_glue_context, mock_job, mock_args, mock_logger)
        
        main()
        
        # Should call delete functions with empty test IDs
        delete_catalog_call = mock_delete_catalog.call_args
        self.assertEqual(delete_catalog_call[1]["test_listing_ids"], [])
        self.assertEqual(delete_catalog_call[1]["test_client_ids"], [])

    @patch('catalog.init_spark')
    @patch('catalog.delete_catalog_for_country')
    @patch('catalog.delete_util_for_country')
    @patch('catalog.run_migration')
    def test_main_delete_only_no_test_ids_aborts(
        self, mock_run_migration, mock_delete_util, mock_delete_catalog, mock_init
    ):
        """Test main function aborts when DELETE_ONLY without test IDs or DELETE_ALL."""
        from catalog import main
        
        # Mock init_spark
        mock_spark = Mock()
        mock_glue_context = Mock()
        mock_job = Mock()
        mock_logger = Mock()
        mock_args = {
            "COUNTRY_CODE": "us",
            "SOURCE_LISTINGS_TABLE": "source-listings",
            "SOURCE_REGION": "us-west-2",
            "TARGET_CATALOG_TABLE": "target-catalog",
            "TARGET_UTIL_TABLE": "target-util",
            "TARGET_REGION": "us-east-1",
            "TEST_LISTING_IDS": [],
            "TEST_CLIENT_IDS": [],
            "DELETE_ONLY": True,
            "DELETE_ALL": False,
            "CLEAN_FIRST": False
        }
        mock_init.return_value = (mock_spark, mock_glue_context, mock_job, mock_args, mock_logger)
        
        main()
        
        # Should NOT call delete functions
        mock_delete_catalog.assert_not_called()
        mock_delete_util.assert_not_called()
        # Should NOT run migration
        mock_run_migration.assert_not_called()
        # Should still commit job
        mock_job.commit.assert_called_once()

    @patch('catalog.init_spark')
    @patch('catalog.delete_catalog_for_country')
    @patch('catalog.delete_util_for_country')
    @patch('catalog.run_migration')
    def test_main_clean_first_mode(
        self, mock_run_migration, mock_delete_util, mock_delete_catalog, mock_init
    ):
        """Test main function in CLEAN_FIRST mode."""
        from catalog import main
        
        # Mock init_spark
        mock_spark = Mock()
        mock_glue_context = Mock()
        mock_job = Mock()
        mock_logger = Mock()
        mock_args = {
            "COUNTRY_CODE": "us",
            "SOURCE_LISTINGS_TABLE": "source-listings",
            "SOURCE_REGION": "us-west-2",
            "TARGET_CATALOG_TABLE": "target-catalog",
            "TARGET_UTIL_TABLE": "target-util",
            "TARGET_REGION": "us-east-1",
            "TEST_LISTING_IDS": ["123"],
            "TEST_CLIENT_IDS": [],
            "DELETE_ONLY": False,
            "DELETE_ALL": False,
            "CLEAN_FIRST": True
        }
        mock_init.return_value = (mock_spark, mock_glue_context, mock_job, mock_args, mock_logger)
        
        main()
        
        # Should call delete functions
        mock_delete_catalog.assert_called_once()
        mock_delete_util.assert_called_once()
        # Should ALSO run migration
        mock_run_migration.assert_called_once()
        # Should commit job
        mock_job.commit.assert_called_once()

    @patch('catalog.init_spark')
    @patch('catalog.run_migration')
    def test_main_normal_migration(self, mock_run_migration, mock_init):
        """Test main function in normal migration mode."""
        from catalog import main
        
        # Mock init_spark
        mock_spark = Mock()
        mock_glue_context = Mock()
        mock_job = Mock()
        mock_logger = Mock()
        mock_args = {
            "COUNTRY_CODE": "us",
            "SOURCE_LISTINGS_TABLE": "source-listings",
            "SOURCE_REGION": "us-west-2",
            "TARGET_CATALOG_TABLE": "target-catalog",
            "TARGET_UTIL_TABLE": "target-util",
            "TARGET_REGION": "us-east-1",
            "TEST_LISTING_IDS": ["123"],
            "TEST_CLIENT_IDS": [],
            "DELETE_ONLY": False,
            "DELETE_ALL": False,
            "CLEAN_FIRST": False
        }
        mock_init.return_value = (mock_spark, mock_glue_context, mock_job, mock_args, mock_logger)
        
        main()
        
        # Should run migration
        mock_run_migration.assert_called_once()
        # Should commit job
        mock_job.commit.assert_called_once()


class TestInitSpark(unittest.TestCase):
    """Test the init_spark initialization function."""

    @patch('catalog.SparkContext')
    @patch('catalog.GlueContext')
    @patch('catalog.Job')
    @patch('catalog.getResolvedOptions')
    @patch('catalog._resolve_optional')
    def test_init_spark_with_test_listing_ids(
        self, mock_resolve_optional, mock_get_resolved, mock_job_class,
        mock_glue_context_class, mock_spark_context_class
    ):
        """Test init_spark with comma-separated test listing IDs."""
        from catalog import init_spark
        
        # Mock resolved options
        mock_get_resolved.return_value = {
            "JOB_NAME": "test-job",
            "COUNTRY_CODE": "US",
            "SOURCE_REGION": "us-west-2",
            "TARGET_REGION": "us-east-1",
            "SOURCE_UTIL_TABLE": "source-util",
            "SOURCE_LISTINGS_TABLE": "source-listings",
            "TARGET_UTIL_TABLE": "target-util",
            "TARGET_CATALOG_TABLE": "target-catalog"
        }
        
        # Mock optional parameters
        def resolve_optional_side_effect(argv, key, default):
            if key == "TEST_LISTING_IDS":
                return "123,456,789"
            elif key == "TEST_CLIENT_IDS":
                return "client1,client2"
            elif key == "CLEAN_FIRST":
                return "true"
            elif key == "DELETE_ONLY":
                return "false"
            elif key == "DELETE_ALL":
                return "false"
            return default
        
        mock_resolve_optional.side_effect = resolve_optional_side_effect
        
        # Mock Spark/Glue objects
        mock_sc = Mock()
        mock_spark_context_class.return_value = mock_sc
        mock_gc = Mock()
        mock_logger = Mock()
        mock_gc.get_logger.return_value = mock_logger
        mock_spark = Mock()
        mock_gc.spark_session = mock_spark
        mock_glue_context_class.return_value = mock_gc
        mock_job = Mock()
        mock_job_class.return_value = mock_job
        
        spark, glue_context, job, args, logger = init_spark()
        
        # Verify test IDs were parsed correctly
        self.assertEqual(args["TEST_LISTING_IDS"], ["123", "456", "789"])
        self.assertEqual(args["TEST_CLIENT_IDS"], ["client1", "client2"])
        self.assertTrue(args["CLEAN_FIRST"])
        self.assertFalse(args["DELETE_ONLY"])
        self.assertFalse(args["DELETE_ALL"])

    @patch('catalog.SparkContext')
    @patch('catalog.GlueContext')
    @patch('catalog.Job')
    @patch('catalog.getResolvedOptions')
    @patch('catalog._resolve_optional')
    def test_init_spark_with_empty_test_ids(
        self, mock_resolve_optional, mock_get_resolved, mock_job_class,
        mock_glue_context_class, mock_spark_context_class
    ):
        """Test init_spark with empty test IDs."""
        from catalog import init_spark
        
        # Mock resolved options
        mock_get_resolved.return_value = {
            "JOB_NAME": "test-job",
            "COUNTRY_CODE": "US",
            "SOURCE_REGION": "us-west-2",
            "TARGET_REGION": "us-east-1",
            "SOURCE_UTIL_TABLE": "source-util",
            "SOURCE_LISTINGS_TABLE": "source-listings",
            "TARGET_UTIL_TABLE": "target-util",
            "TARGET_CATALOG_TABLE": "target-catalog"
        }
        
        # Mock optional parameters - empty strings
        def resolve_optional_side_effect(argv, key, default):
            return ""
        
        mock_resolve_optional.side_effect = resolve_optional_side_effect
        
        # Mock Spark/Glue objects
        mock_sc = Mock()
        mock_spark_context_class.return_value = mock_sc
        mock_gc = Mock()
        mock_logger = Mock()
        mock_gc.get_logger.return_value = mock_logger
        mock_spark = Mock()
        mock_gc.spark_session = mock_spark
        mock_glue_context_class.return_value = mock_gc
        mock_job = Mock()
        mock_job_class.return_value = mock_job
        
        spark, glue_context, job, args, logger = init_spark()
        
        # Verify empty test IDs result in empty lists
        self.assertEqual(args["TEST_LISTING_IDS"], [])
        self.assertEqual(args["TEST_CLIENT_IDS"], [])


if __name__ == "__main__":
    unittest.main()

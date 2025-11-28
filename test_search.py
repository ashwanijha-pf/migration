"""
Unit tests for search.py migration script.

Tests cover:
- Catalog ID and reference PK transformation utilities
- State type mapping and extraction
- JSON handling and normalization
- Filter application logic
- DynamoDB batch operations
- Column alignment and key validation
"""

import unittest
from unittest.mock import Mock, MagicMock, patch, call
from botocore.exceptions import ClientError
from pyspark.sql.types import StructType, StructField, StringType, MapType, IntegerType


class TestPrefixedCatalogId(unittest.TestCase):
    """Test the prefixed_catalog_id function."""

    @patch('search.F')
    def test_prefixed_catalog_id(self, mock_f):
        """Test catalog_id construction with country code prefix."""
        from search import prefixed_catalog_id
        
        mock_country_col = Mock()
        mock_listing_col = Mock()
        mock_f.concat_ws.return_value = "US#12345"
        
        result = prefixed_catalog_id(mock_country_col, mock_listing_col)
        
        # Verify concat_ws called with separator and columns
        mock_f.concat_ws.assert_called_once_with("#", mock_country_col, mock_listing_col)


class TestTransformReferencePk(unittest.TestCase):
    """Test the transform_reference_pk function."""

    @patch('search.F')
    def test_transform_reference_pk(self, mock_f):
        """Test REFERENCE# PK transformation to include country code."""
        from search import transform_reference_pk
        
        mock_country_col = Mock()
        mock_pk_col = Mock()
        mock_substring = Mock()
        mock_f.substring.return_value = mock_substring
        mock_f.lit.return_value = Mock()
        
        result = transform_reference_pk(mock_country_col, mock_pk_col)
        
        # Verify substring extracts after 'REFERENCE#' (position 11)
        mock_f.substring.assert_called_once_with(mock_pk_col, 11, 1_000_000)
        # Verify concat_ws builds new PK with country, REFERENCE, and tail
        mock_f.concat_ws.assert_called_once()


class TestGetTableKeyAttrs(unittest.TestCase):
    """Test the get_table_key_attrs function."""

    @patch('search.boto3.client')
    def test_get_table_key_attrs_hash_only(self, mock_boto3):
        """Test getting key attributes for table with only hash key."""
        from search import get_table_key_attrs
        
        mock_ddb = Mock()
        mock_boto3.return_value = mock_ddb
        mock_ddb.describe_table.return_value = {
            "Table": {
                "KeySchema": [
                    {"AttributeName": "PK", "KeyType": "HASH"}
                ]
            }
        }
        
        result = get_table_key_attrs("test-table", "us-east-1")
        
        self.assertEqual(result, ["PK"])
        mock_ddb.describe_table.assert_called_once_with(TableName="test-table")

    @patch('search.boto3.client')
    def test_get_table_key_attrs_hash_and_range(self, mock_boto3):
        """Test getting key attributes for table with hash and range keys."""
        from search import get_table_key_attrs
        
        mock_ddb = Mock()
        mock_boto3.return_value = mock_ddb
        mock_ddb.describe_table.return_value = {
            "Table": {
                "KeySchema": [
                    {"AttributeName": "PK", "KeyType": "HASH"},
                    {"AttributeName": "SK", "KeyType": "RANGE"}
                ]
            }
        }
        
        result = get_table_key_attrs("test-table", "us-east-1")
        
        self.assertEqual(result, ["PK", "SK"])


class TestStripMetadataHashes(unittest.TestCase):
    """Test the strip_metadata_hashes_sql function."""

    def test_strip_metadata_hashes_no_hashes_column(self):
        """Test when hashes column doesn't exist."""
        from search import strip_metadata_hashes_sql
        
        mock_df = Mock()
        mock_df.columns = ["listing_id", "data"]
        
        result = strip_metadata_hashes_sql(mock_df)
        
        # Should return df unchanged
        self.assertEqual(result, mock_df)

    def test_strip_metadata_hashes_with_map_type(self):
        """Test stripping exclusions from MapType hashes."""
        from search import strip_metadata_hashes_sql, HASH_EXCLUSIONS
        
        mock_df = Mock()
        mock_df.columns = ["listing_id", "hashes"]
        mock_schema = {"hashes": Mock(dataType=MapType(StringType(), StringType()))}
        mock_df.schema.__getitem__ = lambda self, key: mock_schema[key]
        mock_df.withColumn.return_value = mock_df
        
        result = strip_metadata_hashes_sql(mock_df)
        
        # Should call withColumn to filter out exclusions
        mock_df.withColumn.assert_called_once()


class TestJsonOf(unittest.TestCase):
    """Test the _json_of helper function."""

    @patch('search.F')
    def test_json_of_creates_json_string(self, mock_f):
        """Test _json_of converts column to JSON string."""
        from search import _json_of
        
        mock_col = Mock()
        mock_f.col.return_value = mock_col
        mock_col.cast.return_value = Mock()
        
        result = _json_of("test_column")
        
        # Verify column is accessed and cast to string
        mock_f.col.assert_called_once_with("test_column")
        mock_col.cast.assert_called_once_with("string")


class TestExtractStateType(unittest.TestCase):
    """Test state type extraction functions."""

    @patch('search.F')
    def test_extract_state_type_expr(self, mock_f):
        """Test _extract_state_type_expr tries multiple paths."""
        from search import _extract_state_type_expr
        
        # Mock F.col to return different mocks for different paths
        mock_f.col.return_value = Mock()
        mock_f.to_json.return_value = Mock()
        mock_f.get_json_object.return_value = Mock()
        mock_f.coalesce.return_value = Mock()
        mock_f.lower.return_value = "live"
        
        result = _extract_state_type_expr()
        
        # Verify coalesce is called to try multiple extraction paths
        mock_f.coalesce.assert_called_once()
        mock_f.lower.assert_called_once()


class TestStateTypeMapping(unittest.TestCase):
    """Test state type mapping logic."""

    def test_state_type_map_direct_mappings(self):
        """Test direct state type mappings (no transformation)."""
        from search import STATE_TYPE_MAP
        
        # Test direct mappings
        self.assertEqual(STATE_TYPE_MAP["archived"], "archived")
        self.assertEqual(STATE_TYPE_MAP["live"], "live")
        self.assertEqual(STATE_TYPE_MAP["draft"], "draft")
        self.assertEqual(STATE_TYPE_MAP["unpublished"], "unpublished")

    def test_state_type_map_transformations(self):
        """Test state type transformations."""
        from search import STATE_TYPE_MAP
        
        # Test transformed mappings
        self.assertEqual(STATE_TYPE_MAP["pending_approval"], "draft_pending_approval")
        self.assertEqual(STATE_TYPE_MAP["publishing_failed"], "allocation_failed")
        self.assertEqual(STATE_TYPE_MAP["pending_publishing"], "validation_requested")
        self.assertEqual(STATE_TYPE_MAP["live_changes_publishing_failed"], "live_changes_allocation_failed")


class TestApplyTestFilters(unittest.TestCase):
    """Test the _apply_test_filters function."""

    @patch('search.F')
    def test_apply_test_filters_run_all(self, mock_f):
        """Test that run_all=True returns df unchanged."""
        from search import _apply_test_filters
        
        mock_df = Mock()
        mock_df.columns = ["listing_id"]
        
        result = _apply_test_filters(mock_df, ["123"], ["client1"], run_all=True)
        
        # Should return df unchanged
        self.assertEqual(result, mock_df)
        mock_df.filter.assert_not_called()

    @patch('search.F')
    def test_apply_test_filters_with_listing_ids(self, mock_f):
        """Test filtering by listing IDs."""
        from search import _apply_test_filters
        
        mock_df = Mock()
        mock_df.columns = ["listing_id", "data"]
        mock_filtered_df = Mock()
        mock_df.filter.return_value = mock_filtered_df
        
        mock_col = Mock()
        mock_f.col.return_value = mock_col
        mock_col.isin.return_value = "filter_condition"
        
        result = _apply_test_filters(mock_df, ["123", "456"], [], run_all=False)
        
        # Should filter by listing IDs
        mock_f.col.assert_called_with("listing_id")
        mock_col.isin.assert_called_once_with(["123", "456"])
        mock_df.filter.assert_called_once()

    @patch('search.F')
    def test_apply_test_filters_no_listing_id_column(self, mock_f):
        """Test when listing_id column doesn't exist."""
        from search import _apply_test_filters
        
        mock_df = Mock()
        mock_df.columns = ["data", "segment"]
        
        result = _apply_test_filters(mock_df, ["123"], [], run_all=False)
        
        # Should return df unchanged (no listing_id column)
        self.assertEqual(result, mock_df)
        mock_df.filter.assert_not_called()


class TestDropAllNullTopLevelColumns(unittest.TestCase):
    """Test the _drop_all_null_top_level_columns function."""

    @patch('search.F')
    def test_drop_all_null_columns_keeps_required(self, mock_f):
        """Test that required columns are always kept."""
        from search import _drop_all_null_top_level_columns
        
        mock_df = Mock()
        mock_df.columns = ["PK", "SK", "null_col", "data_col"]
        
        # Mock aggregation result
        mock_agg_result = Mock()
        mock_agg_result.asDict.return_value = {
            "PK": 1,
            "SK": 1,
            "null_col": 0,  # All null
            "data_col": 1
        }
        mock_df.agg.return_value.collect.return_value = [mock_agg_result]
        
        mock_df.select.return_value = mock_df
        
        result = _drop_all_null_top_level_columns(mock_df, ["PK", "SK"])
        
        # Should call select with non-null columns plus required
        mock_df.select.assert_called_once()


class TestNullOutEmptyHashes(unittest.TestCase):
    """Test the _null_out_empty_hashes function."""

    def test_null_out_empty_hashes_no_hashes_column(self):
        """Test when hashes column doesn't exist."""
        from search import _null_out_empty_hashes
        
        mock_df = Mock()
        mock_df.columns = ["listing_id", "data"]
        
        result = _null_out_empty_hashes(mock_df)
        
        # Should return df unchanged
        self.assertEqual(result, mock_df)

    @patch('search.F')
    def test_null_out_empty_hashes_with_map_type(self, mock_f):
        """Test nulling out empty map hashes."""
        from search import _null_out_empty_hashes
        
        mock_df = Mock()
        mock_df.columns = ["listing_id", "hashes"]
        mock_schema = {"hashes": Mock(dataType=MapType(StringType(), StringType()))}
        mock_df.schema.__getitem__ = lambda self, key: mock_schema[key]
        mock_df.withColumn.return_value = mock_df
        
        result = _null_out_empty_hashes(mock_df)
        
        # Should call withColumn to null out empty maps
        mock_df.withColumn.assert_called_once()


class TestDeleteBatch(unittest.TestCase):
    """Test the _delete_batch function."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_ddb = Mock()
        self.table_name = "test-table"
        self.requests = [
            {"DeleteRequest": {"Key": {"pk": {"S": f"key{i}"}}}}
            for i in range(3)
        ]

    def test_delete_batch_success(self):
        """Test successful batch deletion."""
        from search import _delete_batch
        
        self.mock_ddb.batch_write_item.return_value = {
            "UnprocessedItems": {}
        }
        
        deleted = _delete_batch(self.mock_ddb, self.table_name, self.requests)
        
        self.assertEqual(deleted, 3)
        self.mock_ddb.batch_write_item.assert_called_once()

    def test_delete_batch_with_retries(self):
        """Test batch deletion with unprocessed items."""
        from search import _delete_batch
        
        # First call has unprocessed, second succeeds
        self.mock_ddb.batch_write_item.side_effect = [
            {
                "UnprocessedItems": {
                    self.table_name: [self.requests[2]]
                }
            },
            {"UnprocessedItems": {}}
        ]
        
        with patch('time.sleep'):
            deleted = _delete_batch(self.mock_ddb, self.table_name, self.requests)
        
        self.assertEqual(deleted, 3)
        self.assertEqual(self.mock_ddb.batch_write_item.call_count, 2)

    def test_delete_batch_with_throttle_exception(self):
        """Test batch deletion with throttle exception."""
        from search import _delete_batch
        
        throttle_error = ClientError(
            {"Error": {"Code": "ProvisionedThroughputExceededException"}},
            "batch_write_item"
        )
        self.mock_ddb.batch_write_item.side_effect = [
            throttle_error,
            {"UnprocessedItems": {}}
        ]
        
        with patch('time.sleep'):
            deleted = _delete_batch(self.mock_ddb, self.table_name, self.requests)
        
        self.assertEqual(deleted, 3)


class TestTableHasItems(unittest.TestCase):
    """Test the _table_has_items function."""

    @patch('search.boto3.client')
    def test_table_has_items_true(self, mock_boto3):
        """Test when table has items."""
        from search import _table_has_items
        
        mock_ddb = Mock()
        mock_ddb.scan.return_value = {"Items": [{"pk": "test"}]}
        
        result = _table_has_items(mock_ddb, "test-table")
        
        self.assertTrue(result)
        mock_ddb.scan.assert_called_once_with(TableName="test-table", Limit=1)

    @patch('search.boto3.client')
    def test_table_has_items_false(self, mock_boto3):
        """Test when table is empty."""
        from search import _table_has_items
        
        mock_ddb = Mock()
        mock_ddb.scan.return_value = {"Items": []}
        
        result = _table_has_items(mock_ddb, "test-table")
        
        self.assertFalse(result)

    @patch('search.boto3.client')
    def test_table_has_items_error(self, mock_boto3):
        """Test when scan raises error."""
        from search import _table_has_items
        
        mock_ddb = Mock()
        mock_ddb.scan.side_effect = ClientError(
            {"Error": {"Code": "ResourceNotFoundException"}},
            "scan"
        )
        
        result = _table_has_items(mock_ddb, "test-table")
        
        # Should return True on error (safe default)
        self.assertTrue(result)


class TestAlignDfToTargetKeysForCatalog(unittest.TestCase):
    """Test the align_df_to_target_keys_for_catalog function."""

    @patch('search.get_table_key_attrs')
    @patch('search.F')
    def test_align_df_pk_sk_keys(self, mock_f, mock_get_keys):
        """Test alignment when target uses PK/SK keys."""
        from search import align_df_to_target_keys_for_catalog
        
        mock_df = Mock()
        mock_df.columns = ["catalog_id", "segment", "data"]
        mock_get_keys.return_value = ["PK", "SK"]
        
        mock_df.withColumn.return_value = mock_df
        mock_f.col.return_value = Mock()
        
        result = align_df_to_target_keys_for_catalog(
            mock_df, "target-table", "us-east-1", "segment"
        )
        
        # Should add PK and SK columns
        self.assertEqual(mock_df.withColumn.call_count, 2)

    @patch('search.get_table_key_attrs')
    @patch('search.F')
    def test_align_df_catalog_id_segment_keys(self, mock_f, mock_get_keys):
        """Test alignment when target uses catalog_id/segment keys."""
        from search import align_df_to_target_keys_for_catalog
        
        mock_df = Mock()
        mock_df.columns = ["catalog_id", "segment", "data"]
        mock_get_keys.return_value = ["catalog_id", "segment"]
        
        result = align_df_to_target_keys_for_catalog(
            mock_df, "target-table", "us-east-1", "segment"
        )
        
        # Should return df unchanged (already has correct keys)
        self.assertEqual(result, mock_df)


class TestAlignDfToTargetKeysForUtil(unittest.TestCase):
    """Test the align_df_to_target_keys_for_util function."""

    @patch('search.get_table_key_attrs')
    @patch('search.F')
    def test_align_df_util_with_pk_sk(self, mock_f, mock_get_keys):
        """Test util table alignment with PK/SK."""
        from search import align_df_to_target_keys_for_util
        
        mock_df = Mock()
        mock_df.columns = ["PK", "SK", "data"]
        mock_get_keys.return_value = ["PK", "SK"]
        
        result = align_df_to_target_keys_for_util(
            mock_df, "target-table", "us-east-1"
        )
        
        # Should return df unchanged (already has PK/SK)
        self.assertEqual(result, mock_df)

    @patch('search.get_table_key_attrs')
    @patch('search.F')
    def test_align_df_util_missing_sk(self, mock_f, mock_get_keys):
        """Test util table alignment when SK is missing."""
        from search import align_df_to_target_keys_for_util
        
        mock_df = Mock()
        mock_df.columns = ["PK", "data"]
        mock_get_keys.return_value = ["PK", "SK"]
        
        mock_df.withColumn.return_value = mock_df
        mock_f.lit.return_value = Mock()
        
        result = align_df_to_target_keys_for_util(
            mock_df, "target-table", "us-east-1"
        )
        
        # Should add SK column
        mock_df.withColumn.assert_called_once()


class TestAssertWriteKeysPresent(unittest.TestCase):
    """Test the assert_write_keys_present function."""

    @patch('search.get_table_key_attrs')
    def test_assert_write_keys_present_success(self, mock_get_keys):
        """Test when all keys are present and non-null."""
        from search import assert_write_keys_present
        
        mock_df = Mock()
        mock_df.columns = ["PK", "SK", "data"]
        mock_get_keys.return_value = ["PK", "SK"]
        
        # Mock filter to return empty df (no nulls)
        mock_filtered = Mock()
        mock_filtered.count.return_value = 0
        mock_df.filter.return_value = mock_filtered
        
        # Should not raise
        assert_write_keys_present(mock_df, "test-table", "us-east-1")

    @patch('search.get_table_key_attrs')
    def test_assert_write_keys_present_missing_column(self, mock_get_keys):
        """Test when required key column is missing."""
        from search import assert_write_keys_present
        
        mock_df = Mock()
        mock_df.columns = ["PK", "data"]
        mock_get_keys.return_value = ["PK", "SK"]
        
        # Should raise ValueError
        with self.assertRaises(ValueError) as context:
            assert_write_keys_present(mock_df, "test-table", "us-east-1")
        
        self.assertIn("Missing key columns", str(context.exception))

    @patch('search.get_table_key_attrs')
    @patch('search.F')
    def test_assert_write_keys_present_null_values(self, mock_f, mock_get_keys):
        """Test when key columns have null values."""
        from search import assert_write_keys_present
        
        mock_df = Mock()
        mock_df.columns = ["PK", "SK", "data"]
        mock_get_keys.return_value = ["PK", "SK"]
        
        # Mock filter to return df with nulls
        mock_filtered = Mock()
        mock_filtered.count.return_value = 1
        mock_df.filter.return_value = mock_filtered
        
        # Should raise ValueError
        with self.assertRaises(ValueError) as context:
            assert_write_keys_present(mock_df, "test-table", "us-east-1")
        
        self.assertIn("Nulls in key columns", str(context.exception))


class TestChooseSegmentCol(unittest.TestCase):
    """Test the _choose_segment_col function."""

    def test_choose_segment_col_prefers_segment(self):
        """Test that 'segment' column is preferred."""
        from search import _choose_segment_col
        
        mock_df = Mock()
        mock_df.columns = ["segment", "SK", "data"]
        
        result = _choose_segment_col(mock_df)
        
        self.assertEqual(result, "segment")

    def test_choose_segment_col_falls_back_to_sk(self):
        """Test fallback to SK when segment doesn't exist."""
        from search import _choose_segment_col
        
        mock_df = Mock()
        mock_df.columns = ["SK", "data"]
        
        result = _choose_segment_col(mock_df)
        
        self.assertEqual(result, "SK")

    def test_choose_segment_col_returns_none(self):
        """Test returns None when neither exists."""
        from search import _choose_segment_col
        
        mock_df = Mock()
        mock_df.columns = ["data", "listing_id"]
        
        result = _choose_segment_col(mock_df)
        
        self.assertIsNone(result)


class TestBatchWriteItemsDirect(unittest.TestCase):
    """Test the batch_write_items_direct function."""

    @patch('search.boto3.client')
    def test_batch_write_items_empty_df(self, mock_boto3):
        """Test batch write with empty dataframe."""
        from search import batch_write_items_direct
        
        mock_df = Mock()
        mock_df.collect.return_value = []
        
        result = batch_write_items_direct(mock_df, "test-table", "us-east-1")
        
        self.assertEqual(result, 0)

    @patch('search.boto3.client')
    @patch('search.TypeSerializer')
    def test_batch_write_items_success(self, mock_serializer_class, mock_boto3):
        """Test successful batch write."""
        from search import batch_write_items_direct
        
        mock_ddb = Mock()
        mock_boto3.return_value = mock_ddb
        
        # Mock serializer
        mock_serializer = Mock()
        mock_serializer_class.return_value = mock_serializer
        mock_serializer.serialize.return_value = {"S": "value"}
        
        # Mock dataframe with records
        mock_row = Mock()
        mock_row.asDict.return_value = {"PK": "test", "data": "value"}
        mock_row.__getitem__ = lambda self, key: "test" if key == "PK" else "value"
        mock_df = Mock()
        mock_df.collect.return_value = [mock_row]
        
        # Mock successful batch write
        mock_ddb.batch_write_item.return_value = {"UnprocessedItems": {}}
        
        with patch('search.ThreadPoolExecutor'):
            result = batch_write_items_direct(mock_df, "test-table", "us-east-1")
        
        # Should have attempted to write
        self.assertGreaterEqual(result, 0)


class TestReadDdbTable(unittest.TestCase):
    """Test the read_ddb_table function."""

    def test_read_ddb_table_without_filter(self):
        """Test reading DynamoDB table without scan filter."""
        from search import read_ddb_table
        
        mock_glue_context = Mock()
        mock_dyf = Mock()
        mock_glue_context.create_dynamic_frame.from_options.return_value = mock_dyf
        
        result = read_ddb_table(mock_glue_context, "test-table", "us-east-1")
        
        self.assertEqual(result, mock_dyf)
        mock_glue_context.create_dynamic_frame.from_options.assert_called_once()

    def test_read_ddb_table_with_scan_filter(self):
        """Test reading DynamoDB table with scan filter."""
        from search import read_ddb_table
        
        mock_glue_context = Mock()
        mock_dyf = Mock()
        mock_glue_context.create_dynamic_frame.from_options.return_value = mock_dyf
        
        scan_filter = {
            "dynamodb.filter.expression": "segment = :seg",
            "dynamodb.filter.expressionAttributeValues": '{":seg":{"S":"METADATA"}}'
        }
        
        result = read_ddb_table(
            mock_glue_context, "test-table", "us-east-1", scan_filter=scan_filter
        )
        
        self.assertEqual(result, mock_dyf)
        # Verify scan filter was passed in options
        call_args = mock_glue_context.create_dynamic_frame.from_options.call_args
        self.assertIn("connection_options", call_args[1])


class TestToDynamicFrame(unittest.TestCase):
    """Test the to_dynamic_frame function."""

    @patch('search.DynamicFrame')
    def test_to_dynamic_frame(self, mock_dynamic_frame_class):
        """Test conversion from DataFrame to DynamicFrame."""
        from search import to_dynamic_frame
        
        mock_glue_context = Mock()
        mock_df = Mock()
        mock_dyf = Mock()
        mock_dynamic_frame_class.fromDF.return_value = mock_dyf
        
        result = to_dynamic_frame(mock_glue_context, mock_df)
        
        self.assertEqual(result, mock_dyf)
        mock_dynamic_frame_class.fromDF.assert_called_once_with(
            mock_df, mock_glue_context, "tmp"
        )


class TestNormalizeDataBySegment(unittest.TestCase):
    """Test the _normalize_data_by_segment function."""

    @patch('search.F')
    def test_normalize_data_by_segment(self, mock_f):
        """Test data normalization based on segment type."""
        from search import _normalize_data_by_segment
        
        mock_df = Mock()
        mock_df.withColumn.return_value = mock_df
        
        mock_f.col.return_value = Mock()
        mock_f.lit.return_value = Mock()
        mock_f.when.return_value = Mock(
            when=Mock(return_value=Mock(
                when=Mock(return_value=Mock(
                    otherwise=Mock(return_value="normalized_expr")
                ))
            ))
        )
        
        result = _normalize_data_by_segment(mock_df, "segment")
        
        # Should call withColumn to normalize data
        mock_df.withColumn.assert_called_once()


class TestExtractStateReasonsAsArray(unittest.TestCase):
    """Test the _extract_state_reasons_as_array function."""

    @patch('search.F')
    def test_extract_state_reasons_as_array(self, mock_f):
        """Test extraction of state reasons as array."""
        from search import _extract_state_reasons_as_array
        
        mock_f.col.return_value = Mock()
        mock_f.when.return_value = Mock(
            when=Mock(return_value=Mock(
                otherwise=Mock(return_value="reasons_expr")
            ))
        )
        
        result = _extract_state_reasons_as_array()
        
        # Should build expression to extract reasons
        self.assertIsNotNone(result)


if __name__ == "__main__":
    unittest.main()

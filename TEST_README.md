# Unit Tests for Migration Scripts

This directory contains comprehensive unit tests for the migration scripts: `catalog.py` and `search.py`.

## Test Coverage

### test_catalog.py

Tests for the `catalog.py` migration script:

### 1. **Argument Parsing** (`TestResolveOptional`)
- Tests for `_resolve_optional` function
- Validates parsing of command-line arguments in both `--KEY value` and `--KEY=value` formats
- Tests default value handling

### 2. **DynamoDB Batch Deletion** (`TestDeleteBatch`)
- Tests for `_delete_batch` function
- Validates retry logic with exponential backoff
- Tests handling of throttle exceptions
- Tests fallback to individual delete operations
- Validates tracking of throttled items

### 3. **Catalog Deletion** (`TestDeleteCatalogForCountry`)
- Tests for `delete_catalog_for_country` function
- Validates deletion with no records
- Tests filtering by listing IDs
- Tests client ID lookup and filtering
- Validates DynamoDB scan filters

### 4. **Util Table Deletion** (`TestDeleteUtilForCountry`)
- Tests for `delete_util_for_country` function
- Validates deletion with no records
- Tests filtering by both listing IDs and client IDs
- Validates proper filter application

### 5. **Main Orchestration** (`TestMainFunction`)
- Tests for `main` function
- Validates DELETE_ONLY mode
- Tests DELETE_ALL mode (clears test IDs)
- Tests safety abort when DELETE_ONLY without test IDs
- Validates CLEAN_FIRST mode (delete then migrate)
- Tests normal migration flow

### 6. **Spark Initialization** (`TestInitSpark`)
- Tests for `init_spark` function
- Validates parsing of comma-separated test IDs
- Tests boolean flag parsing (CLEAN_FIRST, DELETE_ONLY, DELETE_ALL)
- Validates empty test ID handling

### test_search.py

Tests for the `search.py` migration script:

### 1. **Catalog ID Transformation** (`TestPrefixedCatalogId`, `TestTransformReferencePk`)
- Tests for `prefixed_catalog_id` function (country code prefixing)
- Tests for `transform_reference_pk` function (REFERENCE# PK transformation)
- Validates proper string concatenation and formatting

### 2. **Table Key Management** (`TestGetTableKeyAttrs`)
- Tests for `get_table_key_attrs` function
- Validates extraction of hash and range keys from DynamoDB tables
- Tests both single-key and composite-key tables

### 3. **Hash Stripping** (`TestStripMetadataHashes`)
- Tests for `strip_metadata_hashes_sql` function
- Validates removal of excluded hash keys (media, quality_score, compliance)
- Tests MapType and StructType handling

### 4. **JSON Utilities** (`TestJsonOf`, `TestNormalizeDataBySegment`)
- Tests for `_json_of` helper function
- Tests for `_normalize_data_by_segment` function
- Validates JSON string creation and segment-based normalization

### 5. **State Type Handling** (`TestExtractStateType`, `TestStateTypeMapping`)
- Tests for state type extraction functions
- Validates state type mapping transformations
- Tests multiple extraction paths for different data shapes

### 6. **Filter Application** (`TestApplyTestFilters`)
- Tests for `_apply_test_filters` function
- Validates run_all mode (no filtering)
- Tests filtering by listing IDs
- Validates behavior when listing_id column is missing

### 7. **Column Management** (`TestDropAllNullTopLevelColumns`, `TestNullOutEmptyHashes`)
- Tests for `_drop_all_null_top_level_columns` function
- Tests for `_null_out_empty_hashes` function
- Validates required column preservation
- Tests empty map/struct handling

### 8. **DynamoDB Operations** (`TestDeleteBatch`, `TestTableHasItems`, `TestBatchWriteItemsDirect`)
- Tests for batch deletion with retry logic
- Tests for table emptiness checking
- Tests for optimized batch write operations
- Validates throttle handling and error recovery

### 9. **Key Alignment** (`TestAlignDfToTargetKeysForCatalog`, `TestAlignDfToTargetKeysForUtil`)
- Tests for catalog table key alignment
- Tests for util table key alignment
- Validates PK/SK vs catalog_id/segment handling

### 10. **Validation** (`TestAssertWriteKeysPresent`, `TestChooseSegmentCol`)
- Tests for key presence validation
- Tests for segment column selection logic
- Validates error handling for missing/null keys

### 11. **Data I/O** (`TestReadDdbTable`, `TestToDynamicFrame`)
- Tests for DynamoDB table reading with/without filters
- Tests for DataFrame to DynamicFrame conversion
- Validates scan filter application

## Running the Tests

### Prerequisites

Install test dependencies:

```bash
pip install -r test_requirements.txt
```

**Note:** AWS Glue libraries are not available for local installation. The tests use mocking to simulate Glue context and related objects.

### Run All Tests

```bash
# Using unittest - run all test files
python -m unittest discover -s . -p "test_*.py"

# Using pytest (recommended) - run all tests
pytest test_catalog.py test_search.py -v

# Run just catalog tests
pytest test_catalog.py -v

# Run just search tests
pytest test_search.py -v
```

### Run Specific Test Classes

```bash
# Catalog tests - argument parsing
pytest test_catalog.py::TestResolveOptional -v

# Catalog tests - deletion functions
pytest test_catalog.py::TestDeleteBatch -v
pytest test_catalog.py::TestDeleteCatalogForCountry -v
pytest test_catalog.py::TestDeleteUtilForCountry -v

# Catalog tests - main orchestration
pytest test_catalog.py::TestMainFunction -v

# Search tests - transformation functions
pytest test_search.py::TestPrefixedCatalogId -v
pytest test_search.py::TestTransformReferencePk -v

# Search tests - state handling
pytest test_search.py::TestStateTypeMapping -v
pytest test_search.py::TestExtractStateType -v

# Search tests - filter application
pytest test_search.py::TestApplyTestFilters -v

# Search tests - key alignment
pytest test_search.py::TestAlignDfToTargetKeysForCatalog -v
pytest test_search.py::TestAlignDfToTargetKeysForUtil -v
```

### Run with Coverage

```bash
# Coverage for catalog.py
pytest test_catalog.py --cov=catalog --cov-report=html --cov-report=term

# Coverage for search.py
pytest test_search.py --cov=search --cov-report=html --cov-report=term

# Combined coverage for both
pytest test_catalog.py test_search.py --cov=catalog --cov=search --cov-report=html --cov-report=term
```

This will generate:
- Terminal coverage report
- HTML coverage report in `htmlcov/index.html`

### Run Specific Tests

```bash
# Run a specific test method from catalog tests
pytest test_catalog.py::TestDeleteBatch::test_delete_batch_success -v

# Run a specific test method from search tests
pytest test_search.py::TestStateTypeMapping::test_state_type_map_transformations -v

# Run tests matching a pattern across all files
pytest -k "delete_batch" -v
pytest -k "state_type" -v
pytest -k "align" -v
```

## Test Structure

Each test class follows this pattern:

1. **Setup** (`setUp` method): Initializes mock objects and test fixtures
2. **Test Methods**: Each tests a specific scenario
3. **Assertions**: Validates expected behavior using `assert` statements
4. **Mocking**: Uses `@patch` decorators to mock external dependencies

## Key Testing Patterns Used

### 1. Mocking AWS Services
```python
@patch('catalog.boto3.client')
def test_example(self, mock_boto3):
    mock_ddb = Mock()
    mock_boto3.return_value = mock_ddb
    # Test code here
```

### 2. Mocking Spark DataFrames
```python
mock_df = Mock()
mock_df.rdd.isEmpty.return_value = False
mock_df.count.return_value = 10
```

### 3. Testing Retry Logic
```python
with patch('time.sleep'):  # Skip actual sleep in tests
    result = function_with_retries()
```

### 4. Verifying Function Calls
```python
mock_function.assert_called_once()
mock_function.assert_called_with(expected_arg1, expected_arg2)
mock_function.assert_not_called()
```

## Continuous Integration

To integrate with CI/CD pipelines:

```yaml
# Example GitHub Actions workflow
- name: Run tests
  run: |
    pip install -r test_requirements.txt
    pytest test_catalog.py --cov=catalog --cov-report=xml
    
- name: Upload coverage
  uses: codecov/codecov-action@v3
  with:
    file: ./coverage.xml
```

## Troubleshooting

### Import Errors

If you encounter import errors for AWS Glue modules:
- The tests use mocking, so actual Glue libraries are not required
- Ensure you're running tests with the mocked imports

### PySpark Version Conflicts

If PySpark version conflicts occur:
- Use the same PySpark version as your AWS Glue environment
- AWS Glue 3.0 uses PySpark 3.1.x
- AWS Glue 4.0 uses PySpark 3.3.x

### Mock Not Working

If mocks aren't being applied:
- Ensure the patch path matches the import path in `catalog.py`
- Use `@patch('catalog.function_name')` not `@patch('module.function_name')`

## Extending the Tests

To add new tests:

1. Create a new test class inheriting from `unittest.TestCase`
2. Add a `setUp` method to initialize fixtures
3. Write test methods starting with `test_`
4. Use appropriate mocks and assertions
5. Run tests to verify

Example:
```python
class TestNewFunction(unittest.TestCase):
    def setUp(self):
        self.mock_logger = Mock()
    
    def test_new_functionality(self):
        # Arrange
        expected = "result"
        
        # Act
        actual = new_function()
        
        # Assert
        self.assertEqual(actual, expected)
```

## Best Practices

1. **Isolation**: Each test should be independent
2. **Mocking**: Mock external dependencies (AWS, Spark, etc.)
3. **Clarity**: Use descriptive test names
4. **Coverage**: Aim for >80% code coverage
5. **Fast**: Tests should run quickly (mock sleeps, network calls)
6. **Assertions**: Test both success and failure cases

## Contact

For questions or issues with the tests, please contact the migration team.

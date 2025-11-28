# Integration Tests for Migration Scripts

## Overview

The integration tests (`test_integration.py`) complement the unit tests by creating **real source and destination objects** using PySpark, running actual migration transformations, and comparing the results to verify correctness.

Unlike unit tests which mock everything, integration tests:
- ✅ Create real PySpark DataFrames with actual data
- ✅ Execute actual transformation logic (not mocked)
- ✅ Compare source and destination data types and values
- ✅ Verify schema changes match expectations
- ✅ Test end-to-end migration flows

## Test Structure

### Test Classes

| Test Class | Purpose | Test Count |
|------------|---------|------------|
| `TestClientReferenceIntegration` | Reference PK transformations | 2 |
| `TestStateTypeIntegration` | State type mapping and reasons | 3 |
| `TestMetadataHashesIntegration` | Hash filtering and nulling | 2 |
| `TestCatalogIdIntegration` | Catalog ID prefixing | 2 |
| `TestSegmentDataIntegration` | Segment-specific data handling | 3 |
| `TestEndToEndMigration` | Complete listing migration | 2 |
| `TestDataComparison` | Schema comparison utilities | 3 |

**Total: 17 integration tests**

## What Gets Tested

### 1. Client Reference Migration

**Test:** `test_reference_pk_transformation`
- Creates source data with `REFERENCE#` PKs
- Applies `transform_reference_pk()` transformation
- Verifies PK format: `REFERENCE#ABC` → `AE#REFERENCE#ABC`
- Verifies catalog_id format: `listing_id` → `AE#listing_id`
- **Comparison:** Source vs destination PK and catalog_id types and values

**Test:** `test_reference_data_preservation`
- Creates source with various data types (string, int, dict)
- Applies transformations
- Verifies non-key fields are preserved unchanged
- **Comparison:** All non-key fields match exactly

### 2. State Type Mapping

**Test:** `test_state_type_mapping_transformations`
- Creates source with various state types
- Applies `map_state_type_expr()` transformation
- Verifies mappings:
  - `pending_approval` → `draft_pending_approval`
  - `publishing_failed` → `allocation_failed`
  - `live` → `live` (no change)
  - `pending_publishing` → `validation_requested`
- **Comparison:** Source state types vs destination mapped types

**Test:** `test_state_reasons_preservation`
- Creates source with state reasons array
- Applies transformation
- Verifies reasons array structure preserved
- Verifies Arabic and English text preserved
- **Comparison:** Source reasons vs destination reasons (structure and values)

**Test:** `test_state_type_case_insensitivity`
- Creates source with mixed case state types
- Applies transformation
- Verifies all normalized to lowercase
- **Comparison:** Different case inputs produce same output

### 3. Metadata Hashes

**Test:** `test_hash_exclusions`
- Creates source with hashes including exclusions
- Applies `strip_metadata_hashes_sql()` transformation
- Verifies excluded hashes removed: `media`, `quality_score`, `compliance`
- Verifies valid hashes preserved: `title`, `description`
- **Comparison:** Source hashes vs destination hashes (exclusions removed)

**Test:** `test_empty_hashes_nulled`
- Creates source with empty and non-empty hashes
- Applies `_null_out_empty_hashes()` transformation
- Verifies empty maps converted to null
- Verifies non-empty maps preserved
- **Comparison:** Source empty map vs destination null

### 4. Catalog ID Prefixing

**Test:** `test_catalog_id_prefixing`
- Creates source with listing_id
- Applies `prefixed_catalog_id()` for multiple countries
- Verifies format: `listing_id` → `{country}#listing_id`
- **Comparison:** Source listing_id vs destination catalog_id format

**Test:** `test_catalog_id_replaces_listing_id`
- Creates source with listing_id
- Applies transformation and drops listing_id
- Verifies listing_id removed, catalog_id added
- **Comparison:** Source has listing_id, destination has catalog_id

### 5. Segment Data Handling

**Test:** `test_amenities_array_preservation`
- Creates AMENITIES segment with array data
- Verifies array type and contents preserved
- **Comparison:** Source array vs destination array (type and values)

**Test:** `test_attributes_struct_preservation`
- Creates ATTRIBUTES segment with struct data
- Verifies struct type and fields preserved
- **Comparison:** Source struct vs destination struct (type and values)

**Test:** `test_price_struct_preservation`
- Creates PRICE segment with struct data
- Verifies struct type and fields preserved
- **Comparison:** Source struct vs destination struct (type and values)

### 6. End-to-End Migration

**Test:** `test_complete_listing_migration`
- Creates complete listing with all segments:
  - METADATA (with hashes)
  - STATE (with type and reasons)
  - AMENITIES (array)
  - PRICE (struct)
- Applies all transformations
- Verifies each segment transformed correctly
- **Comparison:** Complete source listing vs complete destination listing

**Test:** `test_source_destination_type_consistency`
- Creates source with various data types
- Applies transformations
- Verifies preserved fields have consistent types
- **Comparison:** Source field types vs destination field types

### 7. Schema Comparison

**Test:** `test_reference_schema_changes`
- Compares source and destination schemas
- Verifies expected additions: `catalog_id`
- Verifies expected removals: `listing_id`
- **Comparison:** Source schema vs destination schema

**Test:** `test_state_schema_preservation`
- Compares source and destination schemas
- Verifies schema unchanged (only values change)
- **Comparison:** Source schema vs destination schema (identical)

**Test:** `test_metadata_schema_changes`
- Compares source and destination schemas
- Verifies expected additions: `catalog_id`
- Verifies expected removals: `listing_id`
- **Comparison:** Source schema vs destination schema

## Running Integration Tests

### Prerequisites

```bash
# Install dependencies (includes PySpark)
pip install -r test_requirements.txt
```

### Run All Integration Tests

```bash
# Using pytest (recommended)
pytest test_integration.py -v

# Using unittest
python -m unittest test_integration.py
```

### Run Specific Test Classes

```bash
# Reference migration tests
pytest test_integration.py::TestClientReferenceIntegration -v

# State type tests
pytest test_integration.py::TestStateTypeIntegration -v

# Hash filtering tests
pytest test_integration.py::TestMetadataHashesIntegration -v

# Catalog ID tests
pytest test_integration.py::TestCatalogIdIntegration -v

# Segment data tests
pytest test_integration.py::TestSegmentDataIntegration -v

# End-to-end tests
pytest test_integration.py::TestEndToEndMigration -v

# Schema comparison tests
pytest test_integration.py::TestDataComparison -v
```

### Run Specific Tests

```bash
# Test reference PK transformation
pytest test_integration.py::TestClientReferenceIntegration::test_reference_pk_transformation -v

# Test state type mapping
pytest test_integration.py::TestStateTypeIntegration::test_state_type_mapping_transformations -v

# Test hash exclusions
pytest test_integration.py::TestMetadataHashesIntegration::test_hash_exclusions -v

# Test complete migration
pytest test_integration.py::TestEndToEndMigration::test_complete_listing_migration -v
```

### Run with Coverage

```bash
# Integration test coverage
pytest test_integration.py --cov=search --cov-report=html --cov-report=term

# Combined unit + integration coverage
pytest test_catalog.py test_search.py test_integration.py \
    --cov=catalog --cov=search \
    --cov-report=html --cov-report=term
```

## Key Differences: Unit Tests vs Integration Tests

| Aspect | Unit Tests | Integration Tests |
|--------|-----------|-------------------|
| **Data** | Mocked objects | Real PySpark DataFrames |
| **Execution** | Mocked function calls | Actual transformations |
| **Verification** | Mock call assertions | Data value comparisons |
| **Speed** | Very fast (~seconds) | Slower (~minutes) |
| **Dependencies** | Minimal (mocks) | PySpark required |
| **Purpose** | Test function logic | Test data correctness |

## What Gets Compared

### 1. Data Types

For each transformation, we verify:
```python
# Source type
type(source_result["field"]) == int

# Destination type
type(dest_result["field"]) == int

# Types match
assert type(source_result["field"]) == type(dest_result["field"])
```

### 2. Data Values

For each transformation, we verify:
```python
# Expected transformation
assert dest_result["PK"] == "AE#REFERENCE#TEST"

# Value preservation
assert source_result["data"] == dest_result["data"]

# Value mapping
assert dest_result["state_type"] == STATE_TYPE_MAP[source_result["state_type"]]
```

### 3. Schema Changes

For each migration, we verify:
```python
# Fields added
added = set(dest_df.columns) - set(source_df.columns)
assert added == {"catalog_id"}

# Fields removed
removed = set(source_df.columns) - set(dest_df.columns)
assert removed == {"listing_id"}

# Fields preserved
preserved = set(source_df.columns) & set(dest_df.columns)
assert "segment" in preserved
```

## Expected Differences

The tests verify that source and destination match **except for expected changes**:

### Reference Migration
- ✅ **Changed:** `PK` format (adds country prefix)
- ✅ **Added:** `catalog_id` field
- ✅ **Removed:** `listing_id` field
- ✅ **Preserved:** All other fields (data, metadata, timestamps, etc.)

### State Migration
- ✅ **Changed:** `state_type` value (mapped per STATE_TYPE_MAP)
- ✅ **Changed:** `data.type` value (mapped per STATE_TYPE_MAP)
- ✅ **Preserved:** `data.reasons` array
- ✅ **Preserved:** All other fields

### Metadata Migration
- ✅ **Changed:** `hashes` map (exclusions removed)
- ✅ **Added:** `catalog_id` field
- ✅ **Removed:** `listing_id` field
- ✅ **Preserved:** All other fields

### Segment Migrations
- ✅ **Added:** `catalog_id` field
- ✅ **Removed:** `listing_id` field
- ✅ **Preserved:** `data` field (array or struct)
- ✅ **Preserved:** All other fields

## Test Data Examples

### Reference Source
```python
{
    "PK": "REFERENCE#FD-S-1005",
    "listing_id": "12345",
    "data": "value1"
}
```

### Reference Destination
```python
{
    "PK": "AE#REFERENCE#FD-S-1005",  # Changed
    "catalog_id": "AE#12345",        # Added
    # listing_id removed
    "data": "value1"                 # Preserved
}
```

### State Source
```python
{
    "listing_id": "123",
    "segment": "SEGMENT#STATE",
    "state_type": "pending_approval",
    "data": {
        "type": "pending_approval",
        "reasons": [{"ar": "سبب", "en": "reason"}]
    }
}
```

### State Destination
```python
{
    "catalog_id": "AE#123",          # Added (listing_id removed)
    "segment": "SEGMENT#STATE",      # Preserved
    "state_type": "draft_pending_approval",  # Changed (mapped)
    "data": {
        "type": "draft_pending_approval",    # Changed (mapped)
        "reasons": [{"ar": "سبب", "en": "reason"}]  # Preserved
    }
}
```

### Metadata Source
```python
{
    "listing_id": "123",
    "segment": "SEGMENT#METADATA",
    "hashes": {
        "media": "hash1",           # Will be removed
        "quality_score": "hash2",   # Will be removed
        "compliance": "hash3",      # Will be removed
        "title": "hash4",           # Will be preserved
        "description": "hash5"      # Will be preserved
    }
}
```

### Metadata Destination
```python
{
    "catalog_id": "AE#123",         # Added (listing_id removed)
    "segment": "SEGMENT#METADATA",  # Preserved
    "hashes": {
        # media, quality_score, compliance removed
        "title": "hash4",           # Preserved
        "description": "hash5"      # Preserved
    }
}
```

## Debugging Failed Tests

### View Actual vs Expected

```python
# Add print statements to see data
print("Source:", source_result.asDict())
print("Destination:", dest_result.asDict())
```

### View Schema Differences

```python
# Add schema printing
print("Source schema:", source_df.schema)
print("Destination schema:", dest_df.schema)
```

### View Specific Field

```python
# Debug specific field
print(f"Source PK: {source_result['PK']}")
print(f"Dest PK: {dest_result['PK']}")
```

## CI/CD Integration

```yaml
# Example GitHub Actions
- name: Run integration tests
  run: |
    pip install -r test_requirements.txt
    pytest test_integration.py -v --cov=search --cov-report=xml
    
- name: Upload coverage
  uses: codecov/codecov-action@v3
  with:
    file: ./coverage.xml
```

## Performance Considerations

Integration tests are slower than unit tests because they:
- Create real Spark sessions
- Process actual DataFrames
- Execute real transformations

**Typical execution times:**
- Unit tests: ~5-10 seconds
- Integration tests: ~30-60 seconds

To speed up integration tests:
- Use `local[2]` Spark master (2 cores)
- Keep test data small (1-10 records)
- Reuse Spark session across tests (setUpClass/tearDownClass)

## Future Enhancements

Potential additions:
- [ ] Test with larger datasets (100s-1000s of records)
- [ ] Test with real DynamoDB Local
- [ ] Performance benchmarking
- [ ] Data validation rules
- [ ] Schema evolution scenarios
- [ ] Multi-country migrations
- [ ] Error handling and recovery

## Maintenance

When adding new transformations:
1. Add integration test in appropriate test class
2. Create source data with real values
3. Apply transformation
4. Compare source vs destination
5. Document expected differences
6. Update this README

## Contact

For questions about integration tests, refer to this README or the main TEST_README.md.

# Integration Test Examples: Source vs Destination Comparison

This document provides concrete examples of how integration tests compare source and destination objects.

## Example 1: Reference PK Transformation

### Source Object
```python
{
    "PK": "REFERENCE#FD-S-1005",
    "listing_id": "12345",
    "reference_number": "REF-001",
    "created_at": 1234567890,
    "metadata": {"key": "value"}
}
```

### Transformation Applied
```python
from search import transform_reference_pk, prefixed_catalog_id

result_df = source_df.withColumn(
    "PK",
    transform_reference_pk(F.lit("AE"), F.col("PK"))
).withColumn(
    "catalog_id",
    prefixed_catalog_id(F.lit("AE"), F.col("listing_id"))
).drop("listing_id")
```

### Destination Object
```python
{
    "PK": "AE#REFERENCE#FD-S-1005",      # ✅ CHANGED (country prefix added)
    "catalog_id": "AE#12345",             # ✅ ADDED (new field)
    # listing_id removed                  # ✅ REMOVED
    "reference_number": "REF-001",        # ✅ PRESERVED (unchanged)
    "created_at": 1234567890,             # ✅ PRESERVED (unchanged)
    "metadata": {"key": "value"}          # ✅ PRESERVED (unchanged)
}
```

### Comparison Assertions
```python
# Verify PK transformation
assert result["PK"] == "AE#REFERENCE#FD-S-1005"

# Verify catalog_id added
assert result["catalog_id"] == "AE#12345"

# Verify listing_id removed
assert "listing_id" not in result.asDict()

# Verify other fields preserved
assert result["reference_number"] == source["reference_number"]
assert result["created_at"] == source["created_at"]
assert result["metadata"] == source["metadata"]

# Verify types match
assert type(result["PK"]) == str
assert type(result["catalog_id"]) == str
assert type(result["created_at"]) == int
```

---

## Example 2: State Type Mapping

### Source Object
```python
{
    "listing_id": "123",
    "segment": "SEGMENT#STATE",
    "state_type": "pending_approval",
    "data": {
        "type": "pending_approval",
        "reasons": [
            {"ar": "سبب بالعربي", "en": "Reason in English"},
            {"ar": "سبب آخر", "en": "Another reason"}
        ]
    },
    "created_at": 1234567890
}
```

### Transformation Applied
```python
from search import map_state_type_expr, prefixed_catalog_id

result_df = source_df.withColumn(
    "catalog_id",
    prefixed_catalog_id(F.lit("AE"), F.col("listing_id"))
).drop("listing_id")

result_df = map_state_type_expr(result_df)
```

### Destination Object
```python
{
    "catalog_id": "AE#123",                    # ✅ ADDED (listing_id removed)
    "segment": "SEGMENT#STATE",                # ✅ PRESERVED
    "state_type": "draft_pending_approval",    # ✅ CHANGED (mapped)
    "data": {
        "type": "draft_pending_approval",      # ✅ CHANGED (mapped)
        "reasons": [                           # ✅ PRESERVED (structure and values)
            {"ar": "سبب بالعربي", "en": "Reason in English"},
            {"ar": "سبب آخر", "en": "Another reason"}
        ]
    },
    "created_at": 1234567890                   # ✅ PRESERVED
}
```

### Comparison Assertions
```python
from search import STATE_TYPE_MAP

# Verify state_type mapping
assert result["state_type"] == STATE_TYPE_MAP["pending_approval"]
assert result["state_type"] == "draft_pending_approval"

# Verify data.type mapping
assert result["data"]["type"] == STATE_TYPE_MAP["pending_approval"]
assert result["data"]["type"] == "draft_pending_approval"

# Verify reasons preserved
assert len(result["data"]["reasons"]) == 2
assert result["data"]["reasons"][0]["ar"] == "سبب بالعربي"
assert result["data"]["reasons"][0]["en"] == "Reason in English"
assert result["data"]["reasons"][1]["ar"] == "سبب آخر"
assert result["data"]["reasons"][1]["en"] == "Another reason"

# Verify catalog_id transformation
assert result["catalog_id"] == "AE#123"
assert "listing_id" not in result.asDict()

# Verify other fields preserved
assert result["segment"] == source["segment"]
assert result["created_at"] == source["created_at"]
```

---

## Example 3: Metadata Hash Filtering

### Source Object
```python
{
    "listing_id": "123",
    "segment": "SEGMENT#METADATA",
    "client_id": "client_456",
    "hashes": {
        "media": "hash_media_123",           # Will be REMOVED
        "quality_score": "hash_qs_456",      # Will be REMOVED
        "compliance": "hash_comp_789",       # Will be REMOVED
        "title": "hash_title_abc",           # Will be PRESERVED
        "description": "hash_desc_def",      # Will be PRESERVED
        "location": "hash_loc_ghi"           # Will be PRESERVED
    },
    "title": "Beautiful Apartment",
    "created_at": 1234567890
}
```

### Transformation Applied
```python
from search import strip_metadata_hashes_sql, _null_out_empty_hashes, prefixed_catalog_id

result_df = source_df.withColumn(
    "catalog_id",
    prefixed_catalog_id(F.lit("AE"), F.col("listing_id"))
).drop("listing_id")

result_df = strip_metadata_hashes_sql(result_df)
result_df = _null_out_empty_hashes(result_df)
```

### Destination Object
```python
{
    "catalog_id": "AE#123",                  # ✅ ADDED (listing_id removed)
    "segment": "SEGMENT#METADATA",           # ✅ PRESERVED
    "client_id": "client_456",               # ✅ PRESERVED
    "hashes": {
        # media REMOVED
        # quality_score REMOVED
        # compliance REMOVED
        "title": "hash_title_abc",           # ✅ PRESERVED
        "description": "hash_desc_def",      # ✅ PRESERVED
        "location": "hash_loc_ghi"           # ✅ PRESERVED
    },
    "title": "Beautiful Apartment",          # ✅ PRESERVED
    "created_at": 1234567890                 # ✅ PRESERVED
}
```

### Comparison Assertions
```python
from search import HASH_EXCLUSIONS

# Verify excluded hashes removed
for excluded in HASH_EXCLUSIONS:  # {"media", "quality_score", "compliance"}
    assert excluded not in result["hashes"]

# Verify valid hashes preserved
assert "title" in result["hashes"]
assert "description" in result["hashes"]
assert "location" in result["hashes"]
assert result["hashes"]["title"] == "hash_title_abc"
assert result["hashes"]["description"] == "hash_desc_def"
assert result["hashes"]["location"] == "hash_loc_ghi"

# Verify catalog_id transformation
assert result["catalog_id"] == "AE#123"
assert "listing_id" not in result.asDict()

# Verify other fields preserved
assert result["client_id"] == source["client_id"]
assert result["title"] == source["title"]
assert result["created_at"] == source["created_at"]
```

---

## Example 4: Empty Hashes Nulling

### Source Object
```python
{
    "listing_id": "123",
    "segment": "SEGMENT#METADATA",
    "hashes": {
        "media": "hash1",
        "quality_score": "hash2",
        "compliance": "hash3"
        # Only exclusions present
    }
}
```

### Transformation Applied
```python
from search import strip_metadata_hashes_sql, _null_out_empty_hashes

result_df = strip_metadata_hashes_sql(source_df)  # Removes all exclusions
result_df = _null_out_empty_hashes(result_df)     # Nulls empty map
```

### Destination Object
```python
{
    "listing_id": "123",
    "segment": "SEGMENT#METADATA",
    "hashes": None                           # ✅ CHANGED (empty map → null)
}
```

### Comparison Assertions
```python
# Verify empty hashes nulled
assert result["hashes"] is None

# Compare with source
assert source["hashes"] == {"media": "hash1", "quality_score": "hash2", "compliance": "hash3"}
assert result["hashes"] is None  # All exclusions removed, then nulled
```

---

## Example 5: Segment Data Preservation

### AMENITIES Source
```python
{
    "listing_id": "123",
    "segment": "SEGMENT#AMENITIES",
    "data": ["WiFi", "Pool", "Parking", "Gym", "Security"]
}
```

### AMENITIES Destination
```python
{
    "catalog_id": "AE#123",                  # ✅ ADDED (listing_id removed)
    "segment": "SEGMENT#AMENITIES",          # ✅ PRESERVED
    "data": ["WiFi", "Pool", "Parking", "Gym", "Security"]  # ✅ PRESERVED (array)
}
```

### AMENITIES Comparison
```python
# Verify data type preserved
assert isinstance(result["data"], list)
assert isinstance(source["data"], list)

# Verify array contents preserved
assert len(result["data"]) == len(source["data"])
assert result["data"] == source["data"]
assert "WiFi" in result["data"]
assert "Pool" in result["data"]
```

### ATTRIBUTES Source
```python
{
    "listing_id": "456",
    "segment": "SEGMENT#ATTRIBUTES",
    "data": {
        "bedrooms": 3,
        "bathrooms": 2,
        "area": 1500.5,
        "furnished": "yes",
        "parking_spaces": 2
    }
}
```

### ATTRIBUTES Destination
```python
{
    "catalog_id": "AE#456",                  # ✅ ADDED (listing_id removed)
    "segment": "SEGMENT#ATTRIBUTES",         # ✅ PRESERVED
    "data": {                                # ✅ PRESERVED (struct)
        "bedrooms": 3,
        "bathrooms": 2,
        "area": 1500.5,
        "furnished": "yes",
        "parking_spaces": 2
    }
}
```

### ATTRIBUTES Comparison
```python
# Verify data type preserved
assert isinstance(result["data"], dict)
assert isinstance(source["data"], dict)

# Verify struct contents preserved
assert result["data"]["bedrooms"] == source["data"]["bedrooms"]
assert result["data"]["bathrooms"] == source["data"]["bathrooms"]
assert result["data"]["area"] == source["data"]["area"]
assert result["data"]["furnished"] == source["data"]["furnished"]
assert result["data"]["parking_spaces"] == source["data"]["parking_spaces"]

# Verify field types preserved
assert type(result["data"]["bedrooms"]) == int
assert type(result["data"]["area"]) == float
assert type(result["data"]["furnished"]) == str
```

---

## Example 6: Complete Listing Migration

### Complete Source (All Segments)

**METADATA Segment:**
```python
{
    "listing_id": "12345",
    "segment": "SEGMENT#METADATA",
    "client_id": "client_123",
    "title": "Luxury Villa",
    "hashes": {"media": "h1", "title": "h2", "description": "h3"}
}
```

**STATE Segment:**
```python
{
    "listing_id": "12345",
    "segment": "SEGMENT#STATE",
    "state_type": "pending_approval",
    "data": {"type": "pending_approval", "reasons": [{"ar": "سبب", "en": "reason"}]}
}
```

**AMENITIES Segment:**
```python
{
    "listing_id": "12345",
    "segment": "SEGMENT#AMENITIES",
    "data": ["WiFi", "Pool", "Gym"]
}
```

**PRICE Segment:**
```python
{
    "listing_id": "12345",
    "segment": "SEGMENT#PRICE",
    "data": {"amount": 5000, "currency": "AED", "period": "monthly"}
}
```

### Complete Destination (All Segments)

**METADATA Segment:**
```python
{
    "catalog_id": "AE#12345",               # ✅ CHANGED
    "segment": "SEGMENT#METADATA",          # ✅ PRESERVED
    "client_id": "client_123",              # ✅ PRESERVED
    "title": "Luxury Villa",                # ✅ PRESERVED
    "hashes": {"title": "h2", "description": "h3"}  # ✅ CHANGED (media removed)
}
```

**STATE Segment:**
```python
{
    "catalog_id": "AE#12345",               # ✅ CHANGED
    "segment": "SEGMENT#STATE",             # ✅ PRESERVED
    "state_type": "draft_pending_approval", # ✅ CHANGED (mapped)
    "data": {
        "type": "draft_pending_approval",   # ✅ CHANGED (mapped)
        "reasons": [{"ar": "سبب", "en": "reason"}]  # ✅ PRESERVED
    }
}
```

**AMENITIES Segment:**
```python
{
    "catalog_id": "AE#12345",               # ✅ CHANGED
    "segment": "SEGMENT#AMENITIES",         # ✅ PRESERVED
    "data": ["WiFi", "Pool", "Gym"]         # ✅ PRESERVED
}
```

**PRICE Segment:**
```python
{
    "catalog_id": "AE#12345",               # ✅ CHANGED
    "segment": "SEGMENT#PRICE",             # ✅ PRESERVED
    "data": {"amount": 5000, "currency": "AED", "period": "monthly"}  # ✅ PRESERVED
}
```

### Complete Listing Comparison
```python
# Verify all segments have catalog_id
assert metadata_result["catalog_id"] == "AE#12345"
assert state_result["catalog_id"] == "AE#12345"
assert amenities_result["catalog_id"] == "AE#12345"
assert price_result["catalog_id"] == "AE#12345"

# Verify all segments removed listing_id
assert "listing_id" not in metadata_result.asDict()
assert "listing_id" not in state_result.asDict()
assert "listing_id" not in amenities_result.asDict()
assert "listing_id" not in price_result.asDict()

# Verify METADATA transformations
assert "media" not in metadata_result["hashes"]
assert "title" in metadata_result["hashes"]

# Verify STATE transformations
assert state_result["state_type"] == "draft_pending_approval"
assert state_result["data"]["type"] == "draft_pending_approval"

# Verify AMENITIES preservation
assert amenities_result["data"] == ["WiFi", "Pool", "Gym"]

# Verify PRICE preservation
assert price_result["data"]["amount"] == 5000
assert price_result["data"]["currency"] == "AED"
```

---

## Summary: What Gets Compared

### For Every Transformation

1. **Field Additions**
   - Verify new fields exist in destination
   - Verify new field values are correct
   - Example: `catalog_id` added

2. **Field Removals**
   - Verify removed fields don't exist in destination
   - Example: `listing_id` removed

3. **Field Transformations**
   - Verify transformed values match expectations
   - Example: `PK` format changed, `state_type` mapped

4. **Field Preservation**
   - Verify unchanged fields have same values
   - Verify unchanged fields have same types
   - Example: `created_at`, `segment`, `client_id`

5. **Data Type Consistency**
   - Verify source and destination types match for preserved fields
   - Example: `int` → `int`, `str` → `str`, `dict` → `dict`

6. **Schema Changes**
   - Verify expected schema additions
   - Verify expected schema removals
   - Verify schema preservation for unchanged fields

### Comparison Pattern

```python
# 1. Create source data
source_data = [{"field": "value"}]
source_df = spark.createDataFrame(source_data)

# 2. Apply transformation
dest_df = apply_transformation(source_df)

# 3. Collect results
source_result = source_df.collect()[0]
dest_result = dest_df.collect()[0]

# 4. Compare
assert dest_result["transformed_field"] == expected_value
assert dest_result["preserved_field"] == source_result["preserved_field"]
assert type(dest_result["field"]) == type(source_result["field"])
```

This pattern ensures that transformations work correctly and data integrity is maintained.

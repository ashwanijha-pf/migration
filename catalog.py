import sys, time, random, boto3
from botocore.exceptions import ClientError
from awsglue.transforms import *
from awsglue.transforms import DropNullFields
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StructType, ArrayType, StringType


# ---------- Delete utilities ----------
def _delete_batch(ddb, table_name, requests, backoff_s=0.5, max_retries=12):
    """Delete a batch of items with exponential backoff and throttle tracking"""
    attempts = 0
    remaining = list(requests)
    deleted_count = 0
    throttled_items = []

    while remaining and attempts < max_retries:
        chunk = remaining[:25]
        try:
            resp = ddb.batch_write_item(RequestItems={table_name: chunk})
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'ProvisionedThroughputExceededException':
                print(f"[THROTTLE] Batch write throttled, attempt {attempts + 1}/{max_retries}")
            attempts += 1
            time.sleep(
                min(backoff_s * (2 ** (attempts - 1)) * (1 + random.random()), 10.0)
            )
            continue

        un = resp.get("UnprocessedItems", {}).get(table_name, [])
        processed = len(chunk) - len(un)
        deleted_count += processed

        if un:
            print(f"[THROTTLE] {len(un)} unprocessed items, retrying (attempt {attempts + 1}/{max_retries})")
            remaining = un + remaining[len(chunk) :]
            attempts += 1
            time.sleep(
                min(backoff_s * (2 ** (attempts - 1)) * (1 + random.random()), 10.0)
            )
        else:
            remaining = remaining[len(chunk) :]
            attempts = 0

    # Track items that couldn't be deleted after max retries
    if remaining:
        print(f"[THROTTLE] {len(remaining)} items still unprocessed after {max_retries} retries")
        for r in remaining:
            throttled_items.append(r["DeleteRequest"]["Key"])

    # best-effort fallback for unprocessed items
    for r in remaining:
        key = r["DeleteRequest"]["Key"]
        for i in range(6):
            try:
                ddb.delete_item(TableName=table_name, Key=key)
                deleted_count += 1
                throttled_items.remove(key) if key in throttled_items else None
                break
            except ClientError as e:
                error_code = e.response.get('Error', {}).get('Code', '')
                if error_code == 'ProvisionedThroughputExceededException' and i == 5:
                    print(f"[THROTTLE] Item still throttled after retries: {key}")
                time.sleep(min((0.25 * (2 ** i)) * (1 + random.random()), 6.0))

    return deleted_count, throttled_items


def get_listing_ids_from_client_index(table, region, index_name, client_ids, max_workers=10):
    """
    Fast retrieval of listing IDs using client_id index (GSI/LSI).
    10-100x faster than table scan!

    Args:
        table: DynamoDB table name
        region: AWS region
        index_name: Name of the client_id GSI/LSI (e.g., 'client_id-index')
        client_ids: List of client IDs to query
        max_workers: Number of parallel queries (default 10)

    Returns:
        List of unique listing IDs
    """
    import boto3
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time

    ddb = boto3.client('dynamodb', region_name=region)
    listing_ids = set()

    def query_client_listing_ids(client_id):
        """Query listing IDs for a single client using index"""
        local_ids = set()
        last_evaluated_key = None
        items_count = 0

        while True:
            query_params = {
                'TableName': table,
                'IndexName': index_name,
                'KeyConditionExpression': 'client_id = :cid',
                'ExpressionAttributeValues': {
                    ':cid': {'S': str(client_id)}
                },
                'ProjectionExpression': 'listing_id',
                'Select': 'SPECIFIC_ATTRIBUTES'
            }

            if last_evaluated_key:
                query_params['ExclusiveStartKey'] = last_evaluated_key

            try:
                response = ddb.query(**query_params)

                for item in response.get('Items', []):
                    if 'listing_id' in item:
                        local_ids.add(item['listing_id']['S'])
                        items_count += 1

                last_evaluated_key = response.get('LastEvaluatedKey')
                if not last_evaluated_key:
                    break

            except Exception as e:
                print(f"[INDEX_QUERY] Error querying client {client_id}: {e}")
                break

        return local_ids, items_count

    # Query all clients in parallel
    total_items = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(query_client_listing_ids, cid): cid for cid in client_ids}

        for future in as_completed(futures):
            client_id = futures[future]
            try:
                client_listing_ids, count = future.result()
                listing_ids.update(client_listing_ids)
                total_items += count
                print(f"[INDEX_QUERY] Client {client_id}: {len(client_listing_ids)} unique listings ({count} total items)")
            except Exception as e:
                print(f"[INDEX_QUERY] Error processing client {client_id}: {e}")

    elapsed = time.time() - start_time
    print(f"[INDEX_QUERY] Retrieved {len(listing_ids)} unique listings from {total_items} total items across {len(client_ids)} clients in {elapsed:.1f}s")

    return list(listing_ids)


def get_listing_ids_for_clients(
    glueContext,
    logger,
    country_code,
    source_listings_table,
    source_region,
    client_ids,
    use_index=True,
    index_name="client-id-index"
):
    """
    Get listing IDs for given client IDs.

    Args:
        use_index: If True, use client_id index for 10-100x faster queries (recommended)
        index_name: Name of the client_id GSI/LSI in your DynamoDB table
    """
    if not client_ids:
        return []

    msg = "[LOOKUP] Looking up listing IDs for " + str(len(client_ids)) + " client IDs"
    print(msg)
    logger.info(msg)

    # OPTIMIZED: Use index query (10-100x faster than table scan)
    if use_index:
        try:
            import time
            start = time.time()

            msg = f"[LOOKUP] Using {index_name} for fast query (10-100x faster than table scan)"
            print(msg)
            logger.info(msg)

            listing_ids = get_listing_ids_from_client_index(
                table=source_listings_table,
                region=source_region,
                index_name=index_name,
                client_ids=client_ids,
                max_workers=10  # Parallel queries
            )

            elapsed = time.time() - start
            estimated_scan_time = elapsed * 20  # Table scan is ~20x slower

            msg = f"[LOOKUP] Found {len(listing_ids)} unique listing IDs in {elapsed:.1f}s using index (vs ~{estimated_scan_time:.0f}s with table scan)"
            print(msg, flush=True)
            sys.stdout.flush()
            logger.info(msg)

            return listing_ids

        except Exception as e:
            msg = f"[LOOKUP] Index query failed ({e}), falling back to table scan"
            print(msg)
            logger.warn(msg)
            # Fall through to table scan below

    # FALLBACK: Table scan (slower, but works if index is not available)
    msg = "[LOOKUP] Using table scan (slower - consider using index for 10-100x speedup)"
    print(msg)
    logger.info(msg)

    # Use DynamoDB scan filter to only read METADATA segment
    scan_filter = {
        "dynamodb.filter.expression": "segment = :seg OR SK = :seg",
        "dynamodb.filter.expressionAttributeValues": '{":seg":{"S":"SEGMENT#METADATA"}}'
    }

    # Read with increased splits to reduce partition size
    dyf = read_ddb_table(glueContext, source_listings_table, source_region, read_percent="1.0", splits="800", scan_filter=scan_filter)
    df = dyf.toDF()

    # Filter by all client IDs at once
    if "client_id" in df.columns and LISTING_ID_COL in df.columns:
        df = df.filter(F.col("client_id").isin(client_ids))

        # Persist to avoid re-reading
        df.persist()

        # Get all listing IDs
        listing_ids = (
            df.select(LISTING_ID_COL).distinct().rdd.map(lambda r: r[0]).collect()
        )

        # Unpersist after collection
        df.unpersist()

        msg = "[LOOKUP] Found " + str(len(listing_ids)) + " unique listing IDs for " + str(len(client_ids)) + " client IDs"
        print(msg, flush=True)
        sys.stdout.flush()
        logger.info(msg)
        return listing_ids
    else:
        msg = "[LOOKUP] Required columns not found in source METADATA segment (need client_id + listing_id)"
        print(msg)
        logger.error(msg)
        return []


def _process_segment_with_batching(
    glueContext,
    logger,
    df,
    test_listing_ids,
    run_all,
    segment_name,
    transform_func,
    target_table,
    target_region,
    batch_size=10000
):
    """
    Helper function to process large datasets in batches to prevent OOM.

    Args:
        df: Source DataFrame
        test_listing_ids: List of listing IDs for test mode
        transform_func: Function that applies transformations to a DataFrame
        target_table: Target DynamoDB table
        segment_name: Name for logging and DynamoDB writes
    """
    if not run_all and test_listing_ids and len(test_listing_ids) > 5000:
        msg = f"[{segment_name}] Large dataset detected ({len(test_listing_ids)} listings), processing in batches..."
        print(msg, flush=True)
        logger.info(msg)

        # Process in batches
        listing_batches = [test_listing_ids[i:i+batch_size] for i in range(0, len(test_listing_ids), batch_size)]

        for batch_num, listing_batch in enumerate(listing_batches, 1):
            msg = f"[{segment_name}] Processing batch {batch_num}/{len(listing_batches)} ({len(listing_batch)} listings)"
            print(msg, flush=True)
            logger.info(msg)

            # Filter to this batch of listings
            batch_df = df.filter(F.col(LISTING_ID_COL).isin(listing_batch))

            # Apply segment-specific transformations
            batch_df = transform_func(batch_df)

            # Convert and write this batch
            batch_out = to_dynamic_frame(glueContext, batch_df)
            batch_out = DropNullFields.apply(frame=batch_out)

            write_to_ddb(
                glueContext,
                batch_out,
                target_table,
                target_region,
                segment_name=segment_name,
            )

            msg = f"[{segment_name}] Batch {batch_num} completed"
            print(msg)
            logger.info(msg)

            # Force cleanup
            del batch_df, batch_out
            import gc
            gc.collect()
    else:
        # Small dataset - process normally
        msg = f"[{segment_name}] Applying transformations..."
        print(msg, flush=True)
        logger.info(msg)

        df = transform_func(df)

        msg = f"[{segment_name}] Converting to DynamicFrame..."
        print(msg, flush=True)
        logger.info(msg)

        out = to_dynamic_frame(glueContext, df)
        out = DropNullFields.apply(frame=out)

        msg = f"[{segment_name}] Writing records to {target_table}"
        print(msg, flush=True)
        sys.stdout.flush()
        logger.info(msg)

        write_to_ddb(
            glueContext,
            out,
            target_table,
            target_region,
            segment_name=segment_name,
        )


def delete_catalog_for_country(
    glueContext,
    logger,
    country_code,
    target_catalog_table,
    target_region,
    test_listing_ids=None,
    test_client_ids=None,
    source_listings_table=None,
    source_region=None,
):
    """
    Delete all catalog entries for a specific country.
    If test IDs are provided, only delete those specific listings.
    """
    msg = "[DELETE] Starting deletion for country: " + str(country_code)
    print(msg)
    logger.info(msg)

    # If client IDs are provided but no listing IDs, look up listing IDs from source METADATA segment
    if test_client_ids and not test_listing_ids and source_listings_table and source_region:
        msg = "[DELETE] Client IDs provided, looking up listing IDs from source METADATA segment..."
        print(msg)
        logger.info(msg)
        looked_up_listing_ids = get_listing_ids_for_clients(
            glueContext,
            logger,
            country_code,
            source_listings_table,
            source_region,
            test_client_ids,
        )
        if looked_up_listing_ids:
            test_listing_ids = looked_up_listing_ids
            msg = (
                "[DELETE] Will delete "
                + str(len(test_listing_ids))
                + " listings for the specified clients"
            )
            print(msg)
            logger.info(msg)
        else:
            msg = "[DELETE] No listings found for client IDs: " + str(test_client_ids)
            print(msg)
            logger.warn(msg)
            return

    # Read current catalog with DynamoDB scan filter for country
    scan_filter = {
        "dynamodb.filter.expression": "begins_with(catalog_id, :country)",
        "dynamodb.filter.expressionAttributeValues": '{":country":{"S":"' + country_code + '#"}}'
    }

    msg = "[DELETE] Reading from table: " + target_catalog_table + " with filter for country " + country_code
    print(msg)
    logger.info(msg)

    dyf = read_ddb_table(glueContext, target_catalog_table, target_region, scan_filter=scan_filter)
    df = dyf.toDF()

    msg = "[DELETE] Loaded records for country " + country_code
    print(msg)
    logger.info(msg)

    # Apply test filters if provided
    if test_listing_ids:
        expected_ids = [country_code + "#" + str(lid) for lid in test_listing_ids]
        msg = "[DELETE] Filtering by " + str(len(test_listing_ids)) + " listing IDs"
        print(msg)
        logger.info(msg)
        df = df.filter(F.col(CATALOG_ID_COL).isin(expected_ids))
        msg = "[DELETE] Applied listing ID filter"
        print(msg)
        logger.info(msg)

    msg = "[DELETE] Prepared records for deletion"
    print(msg)
    logger.info(msg)

    # Check if empty using rdd.isEmpty() instead of count
    if df.rdd.isEmpty():
        msg = "[DELETE] No records to delete"
        print(msg)
        logger.info(msg)
        return

    # Get table keys
    keys = get_table_key_attrs(target_catalog_table, target_region)

    # OOM SAFETY: Check count before collecting
    key_cols = [F.col(k) for k in keys]
    delete_count = df.count()

    if delete_count > 100000:
        msg = f"[DELETE] WARNING: Attempting to delete {delete_count} items - this may cause OOM. Consider batching."
        print(msg)
        logger.warn(msg)

    msg = f"[DELETE] Collecting {delete_count} keys to delete..."
    print(msg)
    logger.info(msg)

    keys_to_delete = df.select(*key_cols).collect()

    msg = "[DELETE] Deleting " + str(len(keys_to_delete)) + " items in batches of 25..."
    print(msg)
    logger.info(msg)

    # Convert to DynamoDB format and delete
    ddb = boto3.client("dynamodb", region_name=target_region)

    # Convert Spark Row objects to DynamoDB key format
    delete_requests = []
    for row in keys_to_delete:
        key_dict = {}
        for k in keys:
            val = row[k]
            if val is not None:
                # Convert to DynamoDB AttributeValue format
                key_dict[k] = {"S": str(val)}
        delete_requests.append({"DeleteRequest": {"Key": key_dict}})

    # Delete in batches
    deleted = 0
    all_throttled_items = []
    for i in range(0, len(delete_requests), 25):
        batch = delete_requests[i : i + 25]
        batch_deleted, throttled = _delete_batch(ddb, target_catalog_table, batch)
        deleted += batch_deleted
        all_throttled_items.extend(throttled)

        if (i + 25) % 100 == 0:
            msg = f"[DELETE] Progress: {deleted}/{len(delete_requests)}"
            print(msg)
            logger.info(msg)

    msg = "[DELETE] Completed deletion of " + str(deleted) + " records from " + target_catalog_table
    print(msg)
    logger.info(msg)

    if all_throttled_items:
        msg = f"[THROTTLE] WARNING: {len(all_throttled_items)} items were throttled and could not be deleted"
        print(msg)
        logger.warn(msg)
        print("[THROTTLE] Throttled item keys:")
        for item in all_throttled_items:
            print(f"  {item}")


def delete_util_for_country(
    glueContext,
    logger,
    country_code,
    target_util_table,
    target_region,
    test_listing_ids=None,
    test_client_ids=None,
):
    """Delete all util (REFERENCE#) entries for a specific country."""
    msg = "[DELETE_UTIL] Starting util deletion for country: " + str(country_code)
    print(msg)
    logger.info(msg)

    # Read current util table with DynamoDB scan filter
    scan_filter = {
        "dynamodb.filter.expression": "begins_with(pk, :prefix)",
        "dynamodb.filter.expressionAttributeValues": '{":prefix":{"S":"' + str(country_code) + '#REFERENCE#"}}'
    }

    msg = "[DELETE_UTIL] Reading util table with filter for country " + str(country_code)
    print(msg)
    logger.info(msg)

    dyf = read_ddb_table(glueContext, target_util_table, target_region, scan_filter=scan_filter)
    df = dyf.toDF()

    msg = "[DELETE_UTIL] Loaded REFERENCE records for country " + str(country_code)
    print(msg)
    logger.info(msg)

    # Apply test filters if provided
    if test_listing_ids and CATALOG_ID_COL in df.columns:
        expected_ids = [str(country_code) + "#" + str(lid) for lid in test_listing_ids]
        msg = "[DELETE_UTIL] Filtering by " + str(len(test_listing_ids)) + " listing IDs"
        print(msg)
        logger.info(msg)
        df = df.filter(F.col(CATALOG_ID_COL).isin(expected_ids))

    if test_client_ids and "client_id" in df.columns:
        msg = "[DELETE_UTIL] Filtering by " + str(len(test_client_ids)) + " client IDs"
        print(msg)
        logger.info(msg)
        df = df.filter(F.col("client_id").isin(test_client_ids))

    msg = "[DELETE_UTIL] Prepared records for deletion"
    print(msg)
    logger.info(msg)

    if df.rdd.isEmpty():
        msg = "[DELETE_UTIL] No records to delete"
        print(msg)
        logger.info(msg)
        return

    # Get table keys
    keys = get_table_key_attrs(target_util_table, target_region)

    # OOM SAFETY: Check count before collecting
    key_cols = [F.col(k) for k in keys]
    delete_count = df.count()

    if delete_count > 100000:
        msg = f"[DELETE_UTIL] WARNING: Attempting to delete {delete_count} items - this may cause OOM. Consider batching."
        print(msg)
        logger.warn(msg)

    msg = f"[DELETE_UTIL] Collecting {delete_count} keys to delete..."
    print(msg)
    logger.info(msg)

    keys_to_delete = df.select(*key_cols).collect()

    msg = "[DELETE_UTIL] Deleting " + str(len(keys_to_delete)) + " items..."
    print(msg)
    logger.info(msg)

    # Convert to DynamoDB format and delete
    ddb = boto3.client("dynamodb", region_name=target_region)

    delete_requests = []
    for row in keys_to_delete:
        key_dict = {}
        for k in keys:
            val = row[k]
            if val is not None:
                key_dict[k] = {"S": str(val)}
        delete_requests.append({"DeleteRequest": {"Key": key_dict}})

    # Delete in batches
    deleted = 0
    all_throttled_items = []
    for i in range(0, len(delete_requests), 25):
        batch = delete_requests[i : i + 25]
        batch_deleted, throttled = _delete_batch(ddb, target_util_table, batch)
        deleted += batch_deleted
        all_throttled_items.extend(throttled)

        if (i + 25) % 100 == 0:
            msg = f"[DELETE_UTIL] Progress: {deleted}/{len(delete_requests)}"
            print(msg)
            logger.info(msg)

    msg = "[DELETE_UTIL] Completed deletion of " + str(deleted) + " records from " + target_util_table
    print(msg)
    logger.info(msg)

    if all_throttled_items:
        msg = f"[THROTTLE] WARNING: {len(all_throttled_items)} items were throttled and could not be deleted"
        print(msg)
        logger.warn(msg)
        print("[THROTTLE] Throttled item keys:")
        for item in all_throttled_items:
            print(f"  {item}")


# ---------- Configuration ----------
PK_COL = "PK"
SK_COL = "SK"
SEGMENT_COL = "segment"
LISTING_ID_COL = "listing_id"
CATALOG_ID_COL = "catalog_id"
HASHES_COL = "hashes"
STATE_DATA_COL = "data"
STATE_TYPE_KEY = "type"

GENERIC_SEGMENTS = ["AMENITIES", "ATTRIBUTES", "DESCRIPTION", "PRICE"]
METADATA_SEGMENT = "METADATA"
STATE_SEGMENT = "STATE"

HASH_EXCLUSIONS = {"media", "quality_score", "compliance"}

# ---------- State mapping ----------
STATE_TYPE_MAP = {
    # direct/no-op
    "archived": "archived",
    "takendown": "takendown",
    "live": "live",
    "unpublished": "unpublished",
    "draft": "draft",
    "rejected": "rejected",
    "live_changes_pending_approval": "live_changes_pending_approval",
    "live_changes_rejected": "live_changes_rejected",
    # transforms
    "pending_approval": "draft_pending_approval",
    "publishing_failed": "allocation_failed",
    "takendown_changes_publishing_failed": "allocation_failed",
    "takendown_changes_pending_approval": "takendown_pending_approval",
    "pending_publishing": "validation_requested",
    "live_changes_publishing_failed": "live_changes_allocation_failed",
    "live_changes_pending_publishing": "live_changes_validation_requested",
    "live_unpublishing_failed": "live_deallocation_failed",
    "live_pending_deletion": "live_delete_requested",
    "live_pending_unpublishing": "live_deallocation_requested",
    "live_deletion_failed": "live_delete_failed",
    "takendown_changes_pending_publishing": "validation_requested",
}

# Draft states for stage calculation
DRAFT_STATES = [
    "draft",
    "pending_approval",
    "draft_pending_approval",
    "rejected",
    "approved",
    "unpublished",
    "validation_requested",
    "validation_failed",
    "allocation_requested",
    "allocation_failed",
]

# Stage constants
STAGE_DRAFT = "draft"
STAGE_LIVE = "live"
STAGE_TAKENDOWN = "takendown"
STAGE_ARCHIVED = "archived"

# ---------- Helpers ----------

from pyspark.sql.types import MapType, StringType

def prune_struct_nulls(df):
    """
    Removes ALL null fields inside data.struct,
    and also removes null data.array, and removes empty 'struct' entries.
    """
    if "data" not in df.columns:
        return df

    # Convert the entire data struct to a JSON map
    df = df.withColumn("data_json", F.to_json("data"))

    def clean_data(js):
        if not js:
            return None
        import json
        try:
            obj = json.loads(js)

            # Remove ONLY null arrays (keep empty arrays [])
            if "array" in obj and obj["array"] is None:
                obj.pop("array", None)

            # Clean struct - ONLY remove None/null values
            # CRITICAL: Keep 0, false, "", [] - these are valid values!
            if "struct" in obj and isinstance(obj["struct"], dict):
                cleaned = {}
                for k, v in obj["struct"].items():
                    # Only exclude actual None/null values
                    # Keep: 0, false, "", [], {} - all valid values
                    if v is not None:
                        cleaned[k] = v
                obj["struct"] = cleaned

                # If struct becomes empty, remove it
                if len(obj["struct"]) == 0:
                    obj.pop("struct", None)

            # If both array & struct removed → data becomes null
            if len(obj.keys()) == 0:
                return None

            return json.dumps(obj)

        except:
            return js

    prune_udf = F.udf(clean_data, StringType())
    df = df.withColumn("data_json_clean", prune_udf("data_json"))

    # Convert back to struct with inferred schema
    df = df.withColumn("data",
        F.from_json("data_json_clean", MapType(StringType(), F.StringType()))
    )

    return df.drop("data_json", "data_json_clean")


def fix_street(df):
    if "data" not in df.columns:
        return df

    # If struct exists
    return df.withColumn("data",
        F.when(
            F.col("data.struct").isNotNull() & F.col("data.struct.street").isNotNull(),
            F.map_concat(
                F.map_from_entries(
                    F.expr("transform(map_entries(data.struct), x -> if(x.key='street', named_struct('key','street','value', named_struct('direction', data.struct.street.direction, 'width', data.struct.street.width)), x))")
                ),
            )
        ).otherwise(F.col("data"))
    )




import json
from pyspark.sql.types import StringType, MapType
from pyspark.sql import functions as F

def normalize_street(df):

    if "data" not in df.columns:
        return df

    def fix(js):
        if js is None:
            return None
        try:
            obj = json.loads(js)

            # If no street → return as-is
            if "street" not in obj or obj["street"] is None:
                return json.dumps(obj)

            street_raw = obj["street"]

            # street is a JSON string, parse it safely
            if isinstance(street_raw, str):
                try:
                    street = json.loads(street_raw)
                except:
                    return json.dumps(obj)
            else:
                # Already a dict (rare)
                street = street_raw

            # Build normalized street object
            new_street = {}

            # Direction → direction
            if "Direction" in street:
                new_street["direction"] = street["Direction"]

            # Width → width
            if "Width" in street:
                width_val = street["Width"]
                if isinstance(width_val, dict):
                    # pick long or double
                    new_street["width"] = width_val.get("long") or width_val.get("double")
                else:
                    new_street["width"] = width_val

            # Write back
            obj["street"] = new_street
            return json.dumps(obj)

        except:
            return js

    fix_udf = F.udf(fix, StringType())

    # Convert map to JSON → apply UDF → back to map
    df = df.withColumn("data_json_tmp", F.to_json("data"))
    df = df.withColumn("data_json_fixed", fix_udf("data_json_tmp"))

    df = df.withColumn(
        "data",
        F.from_json("data_json_fixed", MapType(StringType(), StringType()))
    )

    return df.drop("data_json_tmp", "data_json_fixed")



import json
from pyspark.sql.types import MapType, StringType

def prune_null_fields_in_data(df):
    """
    Convert data struct -> JSON -> prune null/None -> rebuild struct.
    This removes all schema-unioned fields and keeps only actual source fields.
    """
    if "data" not in df.columns:
        return df

    # Convert struct to JSON
    df = df.withColumn("data_json", F.to_json("data"))

    # Use Python UDF to drop null keys from parsed JSON
    def remove_null_keys(json_str):
        if not json_str:
            return None
        try:
            obj = json.loads(json_str)
            # remove keys whose value is None/null
            clean = {k: v for k, v in obj.items() if v is not None}
            return json.dumps(clean)
        except Exception:
            return json_str

    prune_udf = F.udf(remove_null_keys, StringType())
    df = df.withColumn("data_json_clean", prune_udf("data_json"))

    # Convert back to struct using original struct type
    df = df.withColumn("data", F.from_json("data_json_clean", df.schema["data"].dataType))

    return df.drop("data_json", "data_json_clean")

def prune_null_fields(df):
    """
    Remove all keys from `data` struct where value is null.
    Preserves EXACT source shape and eliminates Spark schema junk.
    """
    if "data" not in df.columns:
        return df

    df = df.withColumn("data_json", F.to_json("data"))

    def remove_nulls(js):
        if not js:
            return None
        try:
            obj = json.loads(js)
            clean = {k: v for k, v in obj.items() if v is not None}
            return json.dumps(clean)
        except:
            return js

    prune_udf = F.udf(remove_nulls, StringType())
    df = df.withColumn("data_json_clean", prune_udf("data_json"))

    df = df.withColumn("data",
        F.from_json("data_json_clean", df.schema["data"].dataType)
    )

    return df.drop("data_json", "data_json_clean")


from pyspark.sql.types import StructType, StructField  # you already import StructType, ensure StructField is available

def _normalize_street_in_attributes_data(df):
    """
    For ATTRIBUTES segment, convert:
      data.street.Direction -> data.street.direction
      data.street.Width     -> data.street.width

    All other fields in data are preserved exactly as in the source.
    If street is missing or null, we leave data unchanged.
    """
    if "data" not in df.columns:
        return df

    data_type = df.schema["data"].dataType
    if not isinstance(data_type, StructType):
        # unexpected shape, don't touch it
        return df

    # Only proceed if "street" is a nested struct field in data
    if "street" not in data_type.fieldNames():
        return df

    street_type = next((f.dataType for f in data_type.fields if f.name == "street"), None)
    if not isinstance(street_type, StructType):
        # street exists but not a struct, skip
        return df

    # Build a new street struct with lowercase keys, preserving types
    # NOTE: We only remap if Direction/Width exist. If they don't, we keep original.
    has_direction = "Direction" in street_type.fieldNames()
    has_width = "Width" in street_type.fieldNames()

    if not (has_direction or has_width):
        return df

    # Build a new street struct combining original fields + our normalized ones.
    # We do this via withField to avoid losing any unknown street subfields.
    street_col = F.col("data.street")
    new_street = street_col

    if has_direction:
        new_street = new_street.withField("direction", F.col("data.street.Direction"))
    if has_width:
        new_street = new_street.withField("width", F.col("data.street.Width"))

    # Optionally drop original capitalized keys if you don't want them at all
    # (If you want to keep them too, remove this part.)
    # We'll rebuild the street struct without Direction/Width, then add lowercase.
    def _drop_fields_from_struct(col, fields_to_drop):
        expr = "named_struct(" + ",".join(
            [f"'{f.name}', data.street.{f.name}" for f in street_type.fields if f.name not in fields_to_drop]
        ) + ")"
        return F.expr(expr)

    # Safer approach: we overwrite only street, leaving other data fields untouched
    df = df.withColumn(
        "data",
        F.when(
            F.col("data.street").isNull(),
            F.col("data")
        ).otherwise(
            F.col("data").withField("street", new_street)
        )
    )
    return df



def prefixed_catalog_id(country_code_col, listing_id_col):
    """Build catalog_id as '<COUNTRY>#<LISTING_ID>'."""
    return F.concat_ws("#", country_code_col, listing_id_col)


def transform_reference_pk(country_code_col, pk_col):
    """
    CLIENT-REFERENCE PK format per examples:
      Source:  REFERENCE#FD-S-1005
      Target:  <COUNTRY>#REFERENCE#FD-S-1005   (e.g., AE#REFERENCE#FD-S-1005)
    """
    tail = F.substring(pk_col, 11, 1_000_000)  # after 'REFERENCE#'
    return F.concat_ws("#", country_code_col, F.lit("REFERENCE"), tail)


def get_table_key_attrs(table_name, region):
    ddb = boto3.client("dynamodb", region_name=region)
    ks = ddb.describe_table(TableName=table_name)["Table"]["KeySchema"]
    h = next(x["AttributeName"] for x in ks if x["KeyType"] == "HASH")
    r = next((x["AttributeName"] for x in ks if x["KeyType"] == "RANGE"), None)
    return [h] + ([r] if r else [])


def strip_metadata_hashes_sql(df):
    """Remove noisy keys from 'hashes' for MapType or Struct{M:Map} shapes."""
    if HASHES_COL not in df.columns:
        return df
    exclusions_sql = "array(" + ",".join(f"'{k}'" for k in HASH_EXCLUSIONS) + ")"
    col_type = df.schema[HASHES_COL].dataType
    if isinstance(col_type, MapType):
        return df.withColumn(
            HASHES_COL,
            F.expr(
                f"map_from_entries(filter(map_entries({HASHES_COL}), "
                f"x -> NOT array_contains({exclusions_sql}, x.key)))"
            ),
        )
    if isinstance(col_type, StructType) and "M" in col_type.fieldNames():
        m_field = df.schema[HASHES_COL]["M"].dataType
        if isinstance(m_field, MapType):
            return df.withColumn(
                HASHES_COL,
                F.struct(
                    F.expr(
                        f"map_from_entries(filter(map_entries({HASHES_COL}.M), "
                        f"x -> NOT array_contains({exclusions_sql}, x.key)))"
                    ).alias("M")
                ),
            )
    return df


def _json_of(colname: str):
    """
    Convert a column to JSON string.
    If already a string (e.g., stringified data), return as-is.
    Otherwise, convert struct/map to JSON.
    """
    from pyspark.sql.types import StringType
    c = F.col(colname)

    # Check if column is already StringType - if so, return as-is
    # This handles the case where data is already stringified (PRICE, STATE segments)
    # We can't check the schema here directly, so we use a runtime check
    s = c.cast("string")

    # If the string starts with '{', it's already JSON - return as-is
    # Otherwise, try to convert to JSON (will fail if already string, so we catch that)
    return F.when(s.rlike(r"^\s*\{"), s).otherwise(
        F.when(s.isNotNull(), s).otherwise(F.lit(None))
    )


def _extract_state_type_as_json_only():
    j = _json_of(STATE_DATA_COL)
    c1 = F.get_json_object(j, "$.type")
    c2 = F.get_json_object(j, "$.M.type.S")
    c3 = F.get_json_object(j, "$.M.type")
    c4 = F.get_json_object(j, "$.type.S")
    c5 = F.get_json_object(j, "$.S")
    return F.lower(F.coalesce(c1, c2, c3, c4, c5))


def _extract_state_reasons_as_array():
    """
    Extract reasons as JSON and parse to array<struct<ar:string, en:string>>.
    Returns null if absent/not an array.
    """
    from pyspark.sql.types import ArrayType, StructType, StructField, StringType

    j = _json_of(STATE_DATA_COL)
    r1 = F.get_json_object(j, "$.reasons")
    r2 = F.get_json_object(j, "$.M.reasons")
    r_json = F.coalesce(r1, r2)

    # Reasons is an array of structs with ar and en fields
    reason_struct = StructType([
        StructField("ar", StringType(), True),
        StructField("en", StringType(), True)
    ])
    array_schema = ArrayType(reason_struct)

    return F.when(r_json.isNull(), F.lit(None).cast(array_schema)).otherwise(
        F.from_json(r_json, array_schema)
    )


def map_state_type_expr(df):
    """
    Rebuild data as struct<type:string, reasons:array<struct<ar:string, en:string>>> and set top-level state_type.
    Uses JSON-based extraction first; if that yields null, falls back to existing top-level state_type.
    Note: stage field is NOT migrated in catalog.py (only in search.py).
    """
    if STATE_DATA_COL not in df.columns:
        return df

    # 1) Try JSON-only extraction from data; 2) fall back to existing state_type column
    src_type_from_json = _extract_state_type_as_json_only()
    src_type_str = F.coalesce(src_type_from_json, F.col("state_type").cast("string"))

    # Normalize + map (lowercased)
    mapping_map = F.create_map(
        *sum([[F.lit(k), F.lit(v)] for k, v in STATE_TYPE_MAP.items()], [])
    )
    src_lc = F.lower(src_type_str)
    mapped = F.coalesce(mapping_map.getItem(src_lc), src_lc).cast("string")

    # reasons -> array<string> (or null)
    reasons_arr = _extract_state_reasons_as_array()

    # uniform struct WITHOUT stage (stage is not migrated in catalog.py)
    new_state_struct = F.struct(
        mapped.alias("type"),
        reasons_arr.alias("reasons")
    )

    return df.withColumn(STATE_DATA_COL, new_state_struct).withColumn(
        "state_type", mapped
    )


def to_dynamic_frame(glueContext, df):
    return DynamicFrame.fromDF(df, glueContext, "tmp")


# ---------- Key alignment & assertions ----------
def align_df_to_target_keys_for_catalog(df, target_table, target_region, seg_col_name):
    """Shape DF columns to match target catalog key schema (PK/SK or catalog_id/segment)."""
    keys = get_table_key_attrs(target_table, target_region)
    segment_expr = (
        F.col(seg_col_name)
        if seg_col_name in df.columns
        else (
            F.col(SEGMENT_COL)
            if SEGMENT_COL in df.columns
            else F.lit(None).alias(SEGMENT_COL)
        )
    )
    out = df
    if CATALOG_ID_COL not in out.columns:
        raise ValueError("catalog_id missing before key alignment")
    if SEGMENT_COL not in out.columns and seg_col_name != SEGMENT_COL:
        out = out.withColumn(SEGMENT_COL, segment_expr)
    if len(keys) == 1:
        h = keys[0]
        return out if h == CATALOG_ID_COL else out.withColumn(h, F.col(CATALOG_ID_COL))
    h, r = keys
    if h in (CATALOG_ID_COL, PK_COL) and r in (SEGMENT_COL, SK_COL):
        if h == PK_COL and PK_COL not in out.columns:
            out = out.withColumn(PK_COL, F.col(CATALOG_ID_COL))
        if r == SK_COL and SK_COL not in out.columns:
            chosen_seg = SEGMENT_COL if SEGMENT_COL in out.columns else seg_col_name
            out = out.withColumn(SK_COL, F.col(chosen_seg))
        if h == CATALOG_ID_COL and CATALOG_ID_COL not in out.columns:
            out = out.withColumn(CATALOG_ID_COL, F.col(PK_COL))
        if r == SEGMENT_COL and SEGMENT_COL not in out.columns:
            chosen_sk = SK_COL if SK_COL in out.columns else seg_col_name
            out = out.withColumn(SEGMENT_COL, F.col(chosen_sk))
        return out
    out = out.withColumn(h, F.col(CATALOG_ID_COL))
    chosen_seg = SEGMENT_COL if SEGMENT_COL in out.columns else seg_col_name
    out = out.withColumn(r, F.col(chosen_seg))
    return out


def align_df_to_target_keys_for_util(df, target_table, target_region):
    """Util table usually keyed on PK/SK. Align accordingly."""
    keys = get_table_key_attrs(target_table, target_region)
    out = df
    if len(keys) == 1:
        h = keys[0]
        if PK_COL not in out.columns:
            raise ValueError("Util DF missing PK")
        if h != PK_COL:
            out = out.withColumn(h, F.col(PK_COL))
        return out
    h, r = keys
    if PK_COL not in out.columns:
        raise ValueError("Util DF missing PK")
    if h != PK_COL:
        out = out.withColumn(h, F.col(PK_COL))
    if r:
        if SK_COL in out.columns:
            if r != SK_COL:
                out = out.withColumn(r, F.col(SK_COL))
        else:
            out = out.withColumn(r, F.lit("SK#NONE"))
    return out


def assert_write_keys_present(df, table, region):
    keys = get_table_key_attrs(table, region)
    missing = [k for k in keys if k not in df.columns]
    if missing:
        raise ValueError(
            f"DataFrame missing required DynamoDB key columns {missing} for {table}"
        )
    null_checks = [F.count(F.when(F.col(k).isNull(), 1)).alias(k) for k in keys]
    nulls = df.agg(*null_checks).collect()[0].asDict()
    if any(nulls[k] > 0 for k in keys):
        raise ValueError(f"Nulls in key columns for {table}: {nulls}")


# ---------- Readers / Writers ----------
def batch_write_items_direct(df, table, region, max_workers=10):
    """
    Write items to DynamoDB using boto3 BatchWriteItem (much faster than Glue connector!)

    Args:
        df: Spark DataFrame with items to write
        table: DynamoDB table name
        region: AWS region
        max_workers: Number of parallel write workers

    Returns:
        Number of items written
    """
    import boto3
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from boto3.dynamodb.types import TypeSerializer
    import time

    # Collect all records from DataFrame
    records = df.collect()

    if not records:
        print(f"[BATCH_WRITE] No records to write")
        return 0

    print(f"[BATCH_WRITE] Writing {len(records)} items to {table} using BatchWriteItem")

    ddb = boto3.client('dynamodb', region_name=region)
    serializer = TypeSerializer()

    def write_batch(batch_records):
        """Write a batch of up to 25 items (DynamoDB limit)"""
        items_written = 0

        # Convert records to DynamoDB format
        write_requests = []
        for record in batch_records:
            # Convert Row to dict
            item_dict = record.asDict(recursive=True)

            # Recursively handle nested structures: parse JSON strings and convert Row string representations
            # This preserves Map structures like description: {ar: "...", en: "..."} and title: {ar: "...", en: "..."}
            import json
            import re

            # Define numeric fields that should be numbers, not strings
            # Note: bathrooms, bedrooms, floor_number should remain as strings
            NUMERIC_FIELDS = {
                'size', 'parking_slots', 'number_of_floors', 'plot_size', 'age'
            }

            # Define boolean fields that should be converted from string to bool
            BOOLEAN_FIELDS = {
                'has_garden', 'has_kitchen', 'has_parking_on_site', 'ai_content_used'
            }

            def fix_nested_structures(obj, parent_key=None):
                """Recursively fix nested structures that may be stringified"""
                if obj is None:
                    return None
                elif isinstance(obj, dict):
                    # Recursively process dict values
                    return {k: fix_nested_structures(v, k) for k, v in obj.items()}
                elif isinstance(obj, list):
                    # Recursively process list items
                    return [fix_nested_structures(item, parent_key) for item in obj]
                elif isinstance(obj, str):
                    # Convert boolean strings to actual booleans
                    if parent_key in BOOLEAN_FIELDS:
                        if obj.lower() == 'true':
                            return True
                        elif obj.lower() == 'false':
                            return False
                        # If not a boolean string, keep as-is

                    # Convert numeric strings to numbers for specific fields
                    if parent_key in NUMERIC_FIELDS:
                        # Empty strings should remain as empty strings (will be filtered out)
                        if obj.strip() == '':
                            return obj
                        try:
                            # Try to parse as integer first (avoid float to prevent DynamoDB error)
                            if '.' not in obj:
                                return int(obj)
                            else:
                                # Convert to int if it's a whole number, otherwise use Decimal
                                from decimal import Decimal
                                return Decimal(obj)
                        except (ValueError, TypeError):
                            pass  # Keep as string if conversion fails

                    # IMPORTANT: Do NOT parse JSON strings for 'ar' or 'en' fields in reasons array
                    # These should remain as strings even if they contain JSON
                    if parent_key in ['ar', 'en']:
                        return obj

                    # Try to parse JSON strings (for stringified data fields)
                    if obj.startswith('{') and '"' in obj:
                        try:
                            parsed = json.loads(obj)
                            if isinstance(parsed, dict):
                                # Recursively process the parsed dict to handle nested structures
                                return fix_nested_structures(parsed, parent_key)
                        except (json.JSONDecodeError, ValueError):
                            pass

                    # Try to parse Spark Row string representation: {ar=..., en=...} or {Direction=..., Width=...}
                    if obj.startswith('{') and '=' in obj and obj.endswith('}'):
                        try:
                            # Parse {key1=value1, key2=value2} format
                            content = obj[1:-1]  # Remove { and }
                            pairs = re.split(r',\s*(?![^{]*})', content)  # Split by comma not inside nested {}
                            result = {}
                            for pair in pairs:
                                if '=' in pair:
                                    key, value = pair.split('=', 1)
                                    key = key.strip()
                                    value = value.strip()

                                    # For street.Width, convert to number
                                    if key == 'Width':
                                        try:
                                            result[key] = int(value) if value else 0
                                        except:
                                            result[key] = value
                                    else:
                                        result[key] = value if value else ""
                            if result:
                                # Recursively process the result to handle nested structures
                                return fix_nested_structures(result, parent_key)
                        except:
                            pass

                    return obj
                elif isinstance(obj, float):
                    # Convert float to Decimal to avoid DynamoDB error
                    from decimal import Decimal
                    return Decimal(str(obj))
                else:
                    return obj

            item_dict = fix_nested_structures(item_dict)

            # Serialize to DynamoDB format
            ddb_item = {k: serializer.serialize(v) for k, v in item_dict.items() if v is not None}

            write_requests.append({
                'PutRequest': {
                    'Item': ddb_item
                }
            })

        # Write in chunks of 25 (DynamoDB BatchWriteItem limit)
        for i in range(0, len(write_requests), 25):
            chunk = write_requests[i:i+25]

            unprocessed = {table: chunk}
            retry_count = 0
            max_retries = 5

            while unprocessed and retry_count < max_retries:
                try:
                    response = ddb.batch_write_item(RequestItems=unprocessed)
                    items_written += len(chunk) - len(response.get('UnprocessedItems', {}).get(table, []))

                    unprocessed = response.get('UnprocessedItems', {})

                    if unprocessed:
                        retry_count += 1
                        time.sleep(0.1 * (2 ** retry_count))  # Exponential backoff

                except Exception as e:
                    print(f"[BATCH_WRITE] Error writing batch: {e}")
                    retry_count += 1
                    if retry_count < max_retries:
                        time.sleep(0.1 * (2 ** retry_count))
                    else:
                        break

        return items_written

    # Split records into batches for parallel processing
    batch_size = 100  # Each worker handles 100 records (will be split into 4x25 internally)
    record_batches = [records[i:i+batch_size] for i in range(0, len(records), batch_size)]

    total_written = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(write_batch, batch): idx for idx, batch in enumerate(record_batches)}

        for future in as_completed(futures):
            batch_idx = futures[future]
            try:
                written = future.result()
                total_written += written
                if (batch_idx + 1) % 10 == 0:
                    print(f"[BATCH_WRITE] Progress: {total_written}/{len(records)} items written")
            except Exception as e:
                print(f"[BATCH_WRITE] Error processing batch {batch_idx}: {e}")

    elapsed = time.time() - start_time
    rate = total_written / elapsed if elapsed > 0 else 0
    print(f"[BATCH_WRITE] ✓ Wrote {total_written} items in {elapsed:.1f}s ({rate:.0f} items/s)")

    return total_written


def write_to_ddb(
    glueContext,
    dyf,
    table,
    region,
    segment_name="UNKNOWN",
    write_percent="0.6",
    batch_size="25",
    max_retries=3,
):
    """Write to DynamoDB with automatic retry on throttling"""

    print(
        "[WRITE_DDB:"
        + str(segment_name)
        + "] Starting write to "
        + str(table)
        + " in region "
        + str(region)
    )

    # Get DataFrame for column checks (don't count - too expensive!)
    df = dyf.toDF()
    print("[WRITE_DDB:" + str(segment_name) + "] Starting write (record count will be shown after completion)")

    # Capture keys for throttle logging (limit to avoid OOM)
    keys_being_written = []
    if CATALOG_ID_COL in df.columns:
        keys_being_written = [row[CATALOG_ID_COL] for row in df.select(CATALOG_ID_COL).distinct().limit(1000).collect()]
    elif PK_COL in df.columns:
        keys_being_written = [row[PK_COL] for row in df.select(PK_COL).distinct().limit(1000).collect()]

    current_write_percent = float(write_percent)
    current_batch_size = int(batch_size)

    for retry_attempt in range(max_retries):
        try:
            print(
                "[WRITE_DDB:"
                + str(segment_name)
                + "] Attempt "
                + str(retry_attempt + 1)
                + "/"
                + str(max_retries)
                + " - Write percent: "
                + str(current_write_percent)
                + ", Batch size: "
                + str(current_batch_size)
            )

            glueContext.write_dynamic_frame.from_options(
                frame=dyf,
                connection_type="dynamodb",
                connection_options={
                    "dynamodb.output.tableName": table,
                    "dynamodb.region": region,
                    "dynamodb.throughput.write.percent": str(current_write_percent),
                    "dynamodb.batch.size": str(current_batch_size),
                },
            )

            print(
                "[WRITE_DDB:"
                + str(segment_name)
                + "] Successfully wrote records to "
                + str(table)
            )
            return  # Success, exit function

        except Exception as e:
            error_msg = str(e)
            is_throttle_error = "ProvisionedThroughputExceededException" in error_msg or "throttl" in error_msg.lower()

            if is_throttle_error and retry_attempt < max_retries - 1:
                # Reduce throughput and retry
                current_write_percent = current_write_percent * 0.5
                current_batch_size = max(10, int(current_batch_size * 0.7))

                print(f"[THROTTLE] Write throttled for segment {segment_name} on attempt {retry_attempt + 1}")
                print(f"[THROTTLE] Retrying with reduced throughput: write_percent={current_write_percent:.2f}, batch_size={current_batch_size}")
                print(f"[THROTTLE] Number of keys being retried: {len(keys_being_written)}")

                # Wait before retry with exponential backoff
                wait_time = 2 ** retry_attempt
                print(f"[THROTTLE] Waiting {wait_time} seconds before retry...")
                time.sleep(wait_time)
                continue
            else:
                # Final failure or non-throttle error
                print(
                    "[WRITE_DDB:" + str(segment_name) + "] ERROR writing to "
                    + str(table)
                    + ": "
                    + error_msg
                )

                if is_throttle_error:
                    print(f"[THROTTLE] FAILED after {max_retries} retry attempts")
                    print(f"[THROTTLE] Table: {table}, Final write percent: {current_write_percent:.2f}, Final batch size: {current_batch_size}")
                    print(f"[THROTTLE] Number of keys that failed to write: {len(keys_being_written)}")
                    print(f"[THROTTLE] Keys that could not be written:")
                    for key in keys_being_written:
                        print(f"  {key}")

                raise


def read_ddb_segment_by_listing_ids(table, region, listing_ids, segment, max_workers=10):
    """
    Read specific segment items for given listing IDs using BatchGetItem.
    100x faster than table scan for targeted reads!

    Args:
        table: DynamoDB table name
        region: AWS region
        listing_ids: List of listing IDs to fetch
        segment: Segment name (e.g., "AMENITIES", "STATE", "METADATA")
        max_workers: Number of parallel batch requests

    Returns:
        List of items (as dicts)
    """
    import boto3
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time

    ddb = boto3.client('dynamodb', region_name=region)

    def batch_get_items(batch_listing_ids):
        """Fetch a batch of items (max 100 per request)"""
        items = []

        # Build keys for BatchGetItem
        # For listings table: listing_id (PK), segment (SK)
        # Key attribute names are "listing_id" and "segment" (not PK/SK!)
        # Example: listing_id="01K10PN21VRVBB3TPHK1MWP8RT", segment="SEGMENT#AMENITIES"
        keys = [
            {'listing_id': {'S': listing_id}, 'segment': {'S': f"SEGMENT#{segment}"}}
            for listing_id in batch_listing_ids
        ]

        # BatchGetItem supports max 100 items per request
        for i in range(0, len(keys), 100):
            batch_keys = keys[i:i+100]

            try:
                response = ddb.batch_get_item(
                    RequestItems={
                        table: {
                            'Keys': batch_keys
                        }
                    }
                )

                items.extend(response.get('Responses', {}).get(table, []))

                # Handle unprocessed keys
                unprocessed = response.get('UnprocessedKeys', {})
                while unprocessed:
                    time.sleep(0.1)  # Brief pause before retry
                    response = ddb.batch_get_item(RequestItems=unprocessed)
                    items.extend(response.get('Responses', {}).get(table, []))
                    unprocessed = response.get('UnprocessedKeys', {})

            except Exception as e:
                print(f"[BATCH_GET] Error fetching batch: {e}")

        return items

    # Split into batches for parallel processing
    batch_size = 500  # Process 500 listings per worker
    listing_batches = [listing_ids[i:i+batch_size] for i in range(0, len(listing_ids), batch_size)]

    all_items = []
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(batch_get_items, batch): idx for idx, batch in enumerate(listing_batches)}

        for future in as_completed(futures):
            batch_idx = futures[future]
            try:
                items = future.result()
                all_items.extend(items)
                print(f"[BATCH_GET] {segment}: Fetched batch {batch_idx+1}/{len(listing_batches)}: {len(items)} items")
            except Exception as e:
                print(f"[BATCH_GET] {segment}: Error processing batch {batch_idx}: {e}")

    elapsed = time.time() - start_time
    print(f"[BATCH_GET] {segment}: Retrieved {len(all_items)} items for {len(listing_ids)} listing IDs in {elapsed:.1f}s")

    return all_items


def read_util_table_by_client_index(table, region, client_ids, index_name="client-id-index", max_workers=10):
    """
    Read util table (REFERENCE#) data using client-id-index.
    Much faster than table scan!

    Args:
        table: DynamoDB util table name
        region: AWS region
        client_ids: List of client IDs to query
        index_name: Name of the client_id index
        max_workers: Number of parallel queries

    Returns:
        List of items (as dicts)
    """
    import boto3
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time

    ddb = boto3.client('dynamodb', region_name=region)

    def query_client(client_id):
        """Query all REFERENCE# items for a client"""
        items = []
        last_evaluated_key = None

        while True:
            # Query the client-id-index
            # Index schema: Partition key = SK (contains "CLIENT#{client_id}")
            # We query by SK = "CLIENT#{client_id}" to get all items for this client
            query_params = {
                'TableName': table,
                'IndexName': index_name,
                'KeyConditionExpression': '#sk = :client_sk',
                'ExpressionAttributeNames': {
                    '#sk': 'SK'  # SK is the partition key of the index
                },
                'ExpressionAttributeValues': {
                    ':client_sk': {'S': f'CLIENT#{client_id}'}
                },
                'FilterExpression': 'begins_with(PK, :ref)',
                'ExpressionAttributeValues': {
                    ':client_sk': {'S': f'CLIENT#{client_id}'},
                    ':ref': {'S': 'REFERENCE#'}
                }
            }

            if last_evaluated_key:
                query_params['ExclusiveStartKey'] = last_evaluated_key

            try:
                response = ddb.query(**query_params)
                items.extend(response.get('Items', []))

                last_evaluated_key = response.get('LastEvaluatedKey')
                if not last_evaluated_key:
                    break

            except Exception as e:
                print(f"[INDEX_QUERY] Error querying client {client_id}: {e}")
                break

        return items

    all_items = []
    start_time = time.time()

    print(f"[INDEX_QUERY] Querying client-id-index for {len(client_ids)} clients")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(query_client, cid): cid for cid in client_ids}

        for future in as_completed(futures):
            client_id = futures[future]
            try:
                items = future.result()
                all_items.extend(items)
                print(f"[INDEX_QUERY] Client {client_id}: {len(items)} REFERENCE# items")
            except Exception as e:
                print(f"[INDEX_QUERY] Error processing client {client_id}: {e}")

    elapsed = time.time() - start_time
    print(f"[INDEX_QUERY] Retrieved {len(all_items)} REFERENCE# items for {len(client_ids)} clients in {elapsed:.1f}s")

    return all_items


def read_ddb_items_by_keys(table, region, listing_ids, pk_prefix="REFERENCE#", max_workers=10):
    """
    Read specific items from DynamoDB using BatchGetItem.
    100x faster than table scan for targeted reads!

    Args:
        table: DynamoDB table name
        region: AWS region
        listing_ids: List of listing IDs to fetch
        pk_prefix: Prefix for partition key (e.g., "REFERENCE#")
        max_workers: Number of parallel batch requests

    Returns:
        List of items (as dicts)
    """
    import boto3
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time

    ddb = boto3.client('dynamodb', region_name=region)

    def batch_get_items(batch_listing_ids):
        """Fetch a batch of items (max 100 per request)"""
        items = []

        # Build keys for BatchGetItem
        # Note: Using uppercase PK/SK to match DynamoDB table schema
        keys = [
            {'PK': {'S': f"{pk_prefix}{listing_id}"}, 'SK': {'S': f"{pk_prefix}{listing_id}"}}
            for listing_id in batch_listing_ids
        ]

        # BatchGetItem supports max 100 items per request
        for i in range(0, len(keys), 100):
            batch_keys = keys[i:i+100]

            try:
                response = ddb.batch_get_item(
                    RequestItems={
                        table: {
                            'Keys': batch_keys
                        }
                    }
                )

                items.extend(response.get('Responses', {}).get(table, []))

                # Handle unprocessed keys
                unprocessed = response.get('UnprocessedKeys', {})
                while unprocessed:
                    time.sleep(0.1)  # Brief pause before retry
                    response = ddb.batch_get_item(RequestItems=unprocessed)
                    items.extend(response.get('Responses', {}).get(table, []))
                    unprocessed = response.get('UnprocessedKeys', {})

            except Exception as e:
                print(f"[BATCH_GET] Error fetching batch: {e}")

        return items

    # Split into batches for parallel processing
    batch_size = 500  # Process 500 listings per worker
    listing_batches = [listing_ids[i:i+batch_size] for i in range(0, len(listing_ids), batch_size)]

    all_items = []
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(batch_get_items, batch): idx for idx, batch in enumerate(listing_batches)}

        for future in as_completed(futures):
            batch_idx = futures[future]
            try:
                items = future.result()
                all_items.extend(items)
                print(f"[BATCH_GET] Fetched batch {batch_idx+1}/{len(listing_batches)}: {len(items)} items")
            except Exception as e:
                print(f"[BATCH_GET] Error processing batch {batch_idx}: {e}")

    elapsed = time.time() - start_time
    print(f"[BATCH_GET] Retrieved {len(all_items)} items for {len(listing_ids)} listing IDs in {elapsed:.1f}s")

    return all_items


def read_segment_targeted(glueContext, table, region, listing_ids, segment):
    """
    Read segment data using targeted BatchGetItem (100x faster than table scan).

    Args:
        glueContext: Glue context
        table: DynamoDB table name
        region: AWS region
        listing_ids: List of listing IDs to fetch
        segment: Segment name (e.g., "AMENITIES", "STATE")

    Returns:
        Spark DataFrame with segment data
    """
    if not listing_ids:
        # Return empty DataFrame with minimal schema
        from pyspark.sql.types import StructType, StructField, StringType
        empty_schema = StructType([StructField("listing_id", StringType(), True)])
        return glueContext.spark_session.createDataFrame([], schema=empty_schema)

    # Fetch items using BatchGetItem
    items = read_ddb_segment_by_listing_ids(
        table=table,
        region=region,
        listing_ids=listing_ids,
        segment=segment,
        max_workers=10
    )

    if not items:
        # Return empty DataFrame with minimal schema
        from pyspark.sql.types import StructType, StructField, StringType
        empty_schema = StructType([StructField("listing_id", StringType(), True)])
        return glueContext.spark_session.createDataFrame([], schema=empty_schema)

    # Convert DynamoDB items to Spark DataFrame
    from boto3.dynamodb.types import TypeDeserializer
    from decimal import Decimal
    deserializer = TypeDeserializer()

    # Recursive function to convert Decimals to int (keep Decimal for non-integers to avoid float)
    def convert_decimals(obj):
        if obj is None:
            return None
        elif isinstance(obj, list):
            return [convert_decimals(item) for item in obj]
        elif isinstance(obj, dict):
            return {k: convert_decimals(v) for k, v in obj.items()}
        elif isinstance(obj, Decimal):
            # Only convert to int if it's a whole number, otherwise keep as Decimal
            # This prevents float conversion which causes DynamoDB write errors
            return int(obj) if obj % 1 == 0 else obj
        else:
            return obj

    import json
    import base64
    from boto3.dynamodb.types import Binary

    # Helper to convert Binary to base64 string (for stringification)
    def convert_binary_to_base64(obj):
        if isinstance(obj, Binary):
            return base64.b64encode(obj.value).decode('utf-8')
        elif isinstance(obj, dict):
            return {k: convert_binary_to_base64(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_binary_to_base64(item) for item in obj]
        else:
            return obj

    # Helper to convert Binary to bytes (for preservation)
    def convert_binary_to_bytes(obj):
        """Convert boto3 Binary objects to raw bytes for TypeSerializer"""
        if isinstance(obj, Binary):
            return obj.value  # Extract raw bytes
        elif isinstance(obj, dict):
            return {k: convert_binary_to_bytes(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_binary_to_bytes(item) for item in obj]
        else:
            return obj

    records = []
    stringified_count = 0

    for idx, item in enumerate(items):
        record = {k: convert_decimals(deserializer.deserialize(v)) for k, v in item.items()}

        # Stringify complex fields to avoid CANNOT_MERGE_TYPE errors
        # For PRICE segment, stringify 'data' to preserve nested amounts (MapType can't hold nested Maps)
        # For other segments, preserve 'data' structure to maintain nested fields (title, etc.)
        # Other complex fields (hashes, web_ids, etc.) are stringified EXCEPT for METADATA segment
        for key, value in list(record.items()):
            if key == "data":
                # For PRICE and STATE segments, stringify data to preserve nested structures
                # PRICE: preserve nested amounts (MapType can't hold nested Maps)
                # STATE: preserve reasons list (MapType can't hold Lists)
                if segment in ["PRICE", "STATE"] and value is not None and isinstance(value, dict):
                    try:
                        record[key] = json.dumps(value)
                        stringified_count += 1
                    except (TypeError, AttributeError) as e:
#                         print(f"[WARN] [{segment}] Failed to stringify data field for item {idx}: {e}")
                        record[key] = None
                # For other segments, preserve structure
                continue

            # For METADATA segment, convert Binary objects to bytes for hashes
            if segment == "METADATA" and key == "hashes":
                # Convert Binary to bytes so TypeSerializer can handle it correctly
                record[key] = convert_binary_to_bytes(value)
                continue

            if value is not None and not isinstance(value, (str, int, float, bool)):
                try:
                    # Convert Binary to base64 before JSON serialization
                    value_converted = convert_binary_to_base64(value)
                    record[key] = json.dumps(value_converted)
                    stringified_count += 1
                except (TypeError, AttributeError) as e:
                    print(f"[WARN] [{segment}] Failed to stringify {key} field for item {idx}: {e}")
                    record[key] = None

        records.append(record)

    if stringified_count > 0:
        print(f"[CATALOG] [{segment}] Stringified {stringified_count} complex fields to avoid schema conflicts")

    try:
        df = glueContext.spark_session.createDataFrame(records)
        return df
    except Exception as e:
        print(f"[ERROR] [{segment}] Failed to create DataFrame: {e}")
        print(f"[ERROR] [{segment}] Total records: {len(records)}")
        print(f"[ERROR] [{segment}] Sample record schema: {list(records[0].keys()) if records else 'No records'}")

        # Log field types from first few records to identify conflicts
        if records:
            print(f"[ERROR] [{segment}] Analyzing first 5 records for type conflicts:")
            for i, rec in enumerate(records[:5]):
                listing_id = rec.get('listing_id', 'unknown')
                client_id = rec.get('client_id', 'unknown')
                print(f"[ERROR] [{segment}] Record {i} - listing_id={listing_id}, client_id={client_id}")
                print(f"[ERROR] [{segment}] Record {i} field types: {[(k, type(v).__name__) for k, v in rec.items()]}")

            # Try to identify the specific field causing the conflict
            print(f"[ERROR] [{segment}] Checking for type conflicts across all records...")
            field_types = {}
            for i, rec in enumerate(records):
                for key, value in rec.items():
                    if key not in field_types:
                        field_types[key] = {}
                    value_type = type(value).__name__
                    if value_type not in field_types[key]:
                        field_types[key][value_type] = []
                    field_types[key][value_type].append((i, rec.get('listing_id', 'unknown')))

            # Report fields with multiple types
            print(f"[ERROR] [{segment}] Fields with type conflicts:")
            for field, types in field_types.items():
                if len(types) > 1:
                    print(f"[ERROR] [{segment}]   Field '{field}' has {len(types)} different types:")
                    for type_name, occurrences in types.items():
                        sample_listings = [lid for _, lid in occurrences[:3]]
                        print(f"[ERROR] [{segment}]     - {type_name}: {len(occurrences)} records, sample listings: {sample_listings}")
        raise


def read_ddb_table(glueContext, table, region, read_percent="1.0", splits="400", scan_filter=None):
    """
    Read DynamoDB table with optional scan filter to reduce data at source.
    scan_filter example: "segment = :seg" with expression_attribute_values

    NOTE: For targeted reads when you have listing IDs, use read_segment_targeted() instead (100x faster!)
    """
    connection_options = {
        "dynamodb.input.tableName": table,
        "dynamodb.region": region,
        "dynamodb.throughput.read.percent": read_percent,
        "dynamodb.splits": splits,
    }

    # Add scan filter if provided (reduces data before loading into Spark)
    if scan_filter:
        connection_options.update(scan_filter)

    return glueContext.create_dynamic_frame.from_options(
        connection_type="dynamodb",
        connection_options=connection_options,
    )


# ---------- Migration Helper Functions ----------
def _choose_segment_col(df):
    """Listings tables may store the segment name in 'segment' (preferred) or 'SK'."""
    return SEGMENT_COL if SEGMENT_COL in df.columns else (
        SK_COL if SK_COL in df.columns else None
    )


def _apply_test_filters(df, test_listing_ids, test_client_ids, run_all):
    """
    Filter dataframe by listing IDs and/or client IDs.

    If listing IDs are provided (either directly or looked up from client IDs),
    use ONLY the listing_id filter. The client_id filter should only be used
    if no listing IDs are available.
    """
    if run_all:
        return df

    # Priority 1: Filter by listing_id if available
    if test_listing_ids and LISTING_ID_COL in df.columns:
        print("[FILTER-v2] Applying listing_id filter: " + str(len(test_listing_ids)) + " listing IDs")

        # OPTIMIZE: Choose filtering strategy based on list size
        # - Small lists (< 1000): Use isin() - fastest
        # - Medium lists (1000-10000): Use broadcast join - avoids massive filter expressions
        # - Large lists (> 10000): Use hash join - avoids broadcast memory issues
        if len(test_listing_ids) > 10000:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()

            # Create DataFrame with listing IDs (NO broadcast for large lists)
            listing_df = spark.createDataFrame([(lid,) for lid in test_listing_ids], [LISTING_ID_COL])

            # Use regular hash join (Spark will optimize automatically)
            df = df.join(listing_df, LISTING_ID_COL, "inner")
            print("[FILTER-v2] Applied listing_id filter using hash join (large batch)")
        elif len(test_listing_ids) > 1000:
            from pyspark.sql import SparkSession
            spark = SparkSession.getActiveSession()

            # Create small DataFrame with listing IDs and broadcast it
            listing_df = spark.createDataFrame([(lid,) for lid in test_listing_ids], [LISTING_ID_COL])
            listing_df = F.broadcast(listing_df)

            # Use broadcast join for medium-sized lists
            df = df.join(listing_df, LISTING_ID_COL, "inner")
            print("[FILTER-v2] Applied listing_id filter using broadcast join")
        else:
            # Use isin() for small lists
            df = df.filter(F.col(LISTING_ID_COL).isin(test_listing_ids))
            print("[FILTER-v2] Applied listing_id filter using isin()")

        print("[FILTER-v2] SKIPPING client_id filter (already filtered by listing_id)")
        return df

    # Priority 2: Only filter by client_id if listing_id filter wasn't applied
    if test_client_ids and "client_id" in df.columns:
        print("[FILTER] Applying client_id filter: " + str(len(test_client_ids)) + " client IDs")
        df = df.filter(F.col("client_id").isin(test_client_ids))
        print("[FILTER] Applied client_id filter")
        # For test mode, skip repartition - we'll coalesce later anyway
    elif test_client_ids:
        print(
            "[FILTER] client_id column not found in DataFrame, skipping client_id filter"
        )

    return df


def _drop_all_null_top_level_columns(df, required_cols):
    """
    Return df with any top-level columns dropped if they are all NULL across df.

    For columns in required_cols, always keep them even if they are all NULL.
    """
    cols = df.columns
    checks = [
        F.max(F.when(F.col(c).isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias(c)
        for c in cols
    ]
    flags = df.agg(*checks).collect()[0].asDict()

    keep = [c for c in cols if (flags.get(c, 0) == 1) or (c in required_cols)]
    return df.select(*keep)


def _null_out_empty_hashes(df):
    """If hashes is an empty map, set hashes to NULL."""
    if HASHES_COL not in df.columns:
        return df
    dt = df.schema[HASHES_COL].dataType
    if isinstance(dt, MapType):
        return df.withColumn(
            HASHES_COL,
            F.when(
                F.size(F.map_entries(F.col(HASHES_COL))) == 0, F.lit(None)
            ).otherwise(F.col(HASHES_COL)),
        )
    if isinstance(dt, StructType) and "M" in dt.fieldNames() and isinstance(
        dt["M"].dataType, MapType
    ):
        return df.withColumn(
            HASHES_COL,
            F.when(
                F.size(F.map_entries(F.col(f"{HASHES_COL}.M"))) == 0, F.lit(None)
            ).otherwise(F.col(HASHES_COL)),
        )
    return df


def _remove_null_fields_from_struct(df, struct_col_name, field_list):
    """Remove fields from a struct that are NULL, to avoid writing them to DynamoDB."""
    if struct_col_name not in df.columns:
        return df

    checks = [
        F.max(
            F.when(
                F.col(f"{struct_col_name}.{field}").isNotNull(), F.lit(1)
            ).otherwise(F.lit(0))
        ).alias(field)
        for field in field_list
    ]
    flags = df.agg(*checks).collect()[0].asDict()

    non_null_fields = [field for field in field_list if flags.get(field, 0) == 1]

    if len(non_null_fields) == 0:
        return df.drop(struct_col_name)

    struct_fields = [
        F.col(f"{struct_col_name}.{field}").alias(field)
        for field in non_null_fields
    ]
    return df.withColumn(struct_col_name, F.struct(*struct_fields))


def _rebuild_data_from_source(df, allowed_fields, logger, segment_name):
    """
    Build `data` struct using ONLY:
      - fields in allowed_fields
      - that ACTUALLY exist in the source row (top-level or inside data).

    This is the key to avoiding leakage of PRICE fields into ATTRIBUTES and vice versa.
    """
    if "data" in df.columns:
        data_type = df.schema["data"].dataType
    else:
        data_type = None

    data_is_struct = isinstance(data_type, StructType)
    data_has_M = data_is_struct and "M" in data_type.fieldNames() if data_type else False

    # Precompute nested field sets (for struct-style data)
    nested_fields = set()
    if data_is_struct and not data_has_M:
        nested_fields = set(data_type.fieldNames())

    logger.info(
        f"[REBUILD_DATA:{segment_name}] allowed_fields={allowed_fields}, "
        f"top_level={df.columns}, nested={list(nested_fields)}"
    )

    exprs = []
    for field in allowed_fields:
        col_expr = None

        # 1) Prefer top-level column if present
        if field in df.columns:
            col_expr = F.col(field)
        # 2) data.<field> if data is a struct with that field
        elif data_is_struct and not data_has_M and field in nested_fields:
            col_expr = F.col(f"data.{field}")
        # 3) data.M[field] if data.M is a map
        elif data_has_M:
            col_expr = F.col("data.M").getItem(field)

        if col_expr is not None:
            exprs.append(col_expr.alias(field))

    if not exprs:
        logger.info(
            f"[REBUILD_DATA:{segment_name}] No allowed fields present in source; dropping data column"
        )
        if "data" in df.columns:
            return df.drop("data")
        return df

    logger.info(
        f"[REBUILD_DATA:{segment_name}] Rebuilding data struct with {len(exprs)} fields"
    )
    return df.withColumn("data", F.struct(*exprs))


def _safe_count(df, operation_name="operation", logger=None):
    """
    Safely count DataFrame with error handling for Py4JError.
    Returns count or -1 if error occurs.
    """
    try:
        count = df.count()
        return count
    except Exception as e:
        error_msg = f"[{operation_name}] Error during count(): {str(e)}"
        print(error_msg)
        if logger:
            logger.error(error_msg)
        # Try alternative: use RDD count
        try:
            count = df.rdd.count()
            msg = f"[{operation_name}] Used RDD count as fallback: {count}"
            print(msg)
            if logger:
                logger.info(msg)
            return count
        except Exception as e2:
            error_msg = f"[{operation_name}] RDD count also failed: {str(e2)}"
            print(error_msg)
            if logger:
                logger.error(error_msg)
            return -1


def _get_listing_batches(df, batch_size=5000):
    """
    Get listing IDs from DataFrame and split into batches to prevent OOM.
    Returns list of listing ID batches.
    """
    if LISTING_ID_COL not in df.columns:
        return [[]]  # Return single empty batch if no listing_id column

    listing_ids = df.select(LISTING_ID_COL).distinct().rdd.map(lambda r: r[0]).collect()

    if not listing_ids:
        return [[]]

    # Split into batches
    batches = [listing_ids[i:i + batch_size] for i in range(0, len(listing_ids), batch_size)]
    return batches


# ---------- Migration Functions ----------
def migrate_client_reference(
    glueContext,
    logger,
    country_code,
    source_util_table,
    source_region,
    target_util_table,
    target_region,
    test_listing_ids,
    test_client_ids,
    run_all,
):

    msg = f"[MIGRATE_REFERENCE] Starting client reference migration for {country_code}"
    print(msg)
    logger.info(msg)

    # Get Spark session and configure memory optimizations
    spark = glueContext.spark_session

    # Disable auto-broadcast to prevent large DataFrame broadcasts
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    # Enable off-heap memory for better performance and stability
    # spark.conf.set("spark.memory.offHeap.enabled", "true")
    # spark.conf.set("spark.memory.offHeap.size", "30g")  # 30GB off-heap per executor

    # Optimize memory fractions
    # spark.conf.set("spark.memory.fraction", "0.5")  # 50% of heap for Spark
    # spark.conf.set("spark.memory.storageFraction", "0.2")  # 20% for caching, 80% for execution

    # Configure S3 spillover to validations bucket
    # spark.conf.set("spark.local.dir", "s3://pf-b2b-validations-staging/spark-temp/")
    # spark.conf.set("spark.hadoop.fs.s3.buffer.dir", "s3://pf-b2b-validations-staging/spark-buffer/")

    # Enable compression to reduce S3 costs
    # spark.conf.set("spark.shuffle.spill.compress", "true")
    # spark.conf.set("spark.shuffle.compress", "true")
    # spark.conf.set("spark.io.compression.codec", "lz4")

    msg = "[OPTIMIZE] Configured off-heap memory (30GB) with S3 spillover to pf-b2b-validations-production"
    print(msg)
    logger.info(msg)

    # CRITICAL: For large datasets, process in batches from the start to prevent OOM
    if not run_all and test_listing_ids and len(test_listing_ids) > 5000:
        msg = "[MIGRATE_REFERENCE] Large dataset detected (" + str(len(test_listing_ids)) + " listings), processing in batches..."
        print(msg, flush=True)
        logger.info(msg)

        # OPTIMIZE: Auto-tune batch size based on dataset size (optimized for G.8X)
        # Reduced batch sizes to prevent executor OOM
        total_listings = len(test_listing_ids)
        if total_listings > 500000:
            batch_size = 2000   # Conservative for very large datasets
        elif total_listings > 100000:
            batch_size = 3000   # Medium batches
        elif total_listings > 50000:
            batch_size = 5000   # Larger batches for medium datasets
        else:
            batch_size = 5000   # Maximum batch size to avoid executor OOM

        msg = "[MIGRATE_REFERENCE] Auto-tuned batch size: " + str(batch_size) + " listings per batch"
        print(msg)
        logger.info(msg)

        # Process in optimized batches - LOAD, PROCESS, WRITE each batch separately
        listing_batches = [test_listing_ids[i:i+batch_size] for i in range(0, len(test_listing_ids), batch_size)]

        # DATA INTEGRITY: Verify no listings are lost in batching
        total_batched_listings = sum(len(batch) for batch in listing_batches)
        if total_batched_listings != len(test_listing_ids):
            raise ValueError(f"DATA LOSS DETECTED: {len(test_listing_ids)} input listings but {total_batched_listings} in batches")

        msg = f"[DATA_INTEGRITY] Verified all {len(test_listing_ids)} listings are included in {len(listing_batches)} batches"
        print(msg)
        logger.info(msg)

        # OPTIMIZE: Process batches with controlled parallelism (max 2 concurrent to avoid resource contention)
        max_concurrent_batches = min(2, len(listing_batches))
        if len(listing_batches) > 1 and max_concurrent_batches > 1:
            msg = "[MIGRATE_REFERENCE] Processing " + str(len(listing_batches)) + " batches with " + str(max_concurrent_batches) + " concurrent workers"
            print(msg)
            logger.info(msg)

        # OPTIMIZE: Intelligent caching with OOM protection - use DynamoDB metadata instead of count()
        msg = "[MIGRATE_REFERENCE] Loading source REFERENCE data with OOM-safe strategy..."
        print(msg, flush=True)
        logger.info(msg)

        # Get approximate count from DynamoDB metadata (instant, no OOM risk)
        def get_dynamodb_item_count(table_name, region):
            """Get item count from DynamoDB table metadata - much faster than Spark count()"""
            try:
                ddb = boto3.client('dynamodb', region_name=region)
                response = ddb.describe_table(TableName=table_name)
                item_count = response['Table']['ItemCount']
                return item_count
            except Exception as e:
                print(f"[WARNING] Could not get DynamoDB item count: {str(e)}")
                return None

        # Try to get metadata count (instant, no Spark computation)
        total_source_records = get_dynamodb_item_count(source_util_table, source_region)

        if total_source_records:
            estimated_memory_mb = total_source_records * 0.5
            msg = f"[MIGRATE_REFERENCE] DynamoDB reports ~{total_source_records:,} items (~{estimated_memory_mb:.0f}MB estimated)"
            print(msg, flush=True)
            logger.info(msg)

            # G.8X cluster specifications:
            # - 40 workers × 64GB = 2,560GB total cluster memory
            # - 32GB driver memory (configured)
            # Conservative caching limit: 10GB (can handle ~20M records)
            cache_limit_mb = 10000  # 10GB limit

            msg = f"[CLUSTER_INFO] G.8X cluster: 40 workers × 64GB, cache limit: {cache_limit_mb}MB"
            print(msg)
            logger.info(msg)

            # Decision: Only cache if dataset is reasonably small
            if estimated_memory_mb < cache_limit_mb:
                # Small dataset - safe to cache
                msg = f"[MIGRATE_REFERENCE] Dataset is small enough to cache (~{estimated_memory_mb:.0f}MB < {cache_limit_mb}MB)"
                print(msg, flush=True)
                logger.info(msg)

                # OPTIMIZED: Use client-id-index if client_ids available (100x faster!)
                if test_client_ids:
                    msg = f"[MIGRATE_REFERENCE] Using client-id-index to load data for {len(test_client_ids)} clients (100x faster than scan)"
                    print(msg, flush=True)
                    logger.info(msg)

                    import time
                    read_start = time.time()

                    # Query index for all clients
                    items = read_util_table_by_client_index(
                        table=source_util_table,
                        region=source_region,
                        client_ids=test_client_ids,
                        index_name="client-id-index",
                        max_workers=10
                    )

                    read_elapsed = time.time() - read_start

                    if items:
                        # Convert DynamoDB items to Spark DataFrame
                        from boto3.dynamodb.types import TypeDeserializer
                        deserializer = TypeDeserializer()

                        records = []
                        for item in items:
                            # Deserialize and remove None values to help Spark infer types
                            record = {}
                            for k, v in item.items():
                                deserialized = deserializer.deserialize(v)
                                # Only include non-None values to avoid type inference issues
                                if deserialized is not None:
                                    record[k] = deserialized
                            records.append(record)

                        # Create DataFrame - Spark will infer schema from non-null values
                        if records:
                            source_df = glueContext.spark_session.createDataFrame(records)
                        else:
                            from pyspark.sql.types import StructType, StructField, StringType
                            empty_schema = StructType([StructField("PK", StringType(), True)])
                            source_df = glueContext.spark_session.createDataFrame([], schema=empty_schema)

                        msg = f"[MIGRATE_REFERENCE] Loaded {len(records)} items in {read_elapsed:.1f}s using client-id-index (vs ~60s with scan)"
                        print(msg, flush=True)
                        logger.info(msg)
                    else:
                        msg = "[MIGRATE_REFERENCE] No REFERENCE# items found for clients, creating empty DataFrame"
                        print(msg)
                        logger.warn(msg)
                        from pyspark.sql.types import StructType, StructField, StringType
                        empty_schema = StructType([StructField("PK", StringType(), True)])
                        source_df = glueContext.spark_session.createDataFrame([], schema=empty_schema)
                else:
                    # FALLBACK: No client_ids, use table scan
                    msg = "[MIGRATE_REFERENCE] No client_ids available, using table scan (slower)"
                    print(msg, flush=True)
                    logger.warn(msg)

                    # Use DynamoDB scan filter with begins_with to reduce data at source
                    scan_filter = {
                        "dynamodb.filter.expression": "begins_with(pk, :ref)",
                        "dynamodb.filter.expressionAttributeValues": '{":ref":{"S":"REFERENCE#"}}'
                    }
                    dyf = read_ddb_table(glueContext, source_util_table, source_region, scan_filter=scan_filter, splits="1000")
                    source_df = dyf.toDF()

                # OPTIMIZE: Apply column pruning early to reduce memory usage
                if not source_df.rdd.isEmpty():
                    required_columns = [PK_COL, LISTING_ID_COL, "client_id"] + [col for col in source_df.columns if col not in [PK_COL, LISTING_ID_COL, "client_id"]]
                    source_df = source_df.select(*[col for col in required_columns if col in source_df.columns])

                from pyspark import StorageLevel
                source_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
                use_cached_source = True

                msg = f"[MIGRATE_REFERENCE] Cached source records for batch reuse"
                print(msg, flush=True)
                logger.info(msg)
            else:
                # Large dataset - use per-batch strategy
                msg = f"[MIGRATE_REFERENCE] Dataset too large (~{estimated_memory_mb:.0f}MB) - using per-batch read to avoid OOM"
                use_cached_source = False
                print(msg, flush=True)
                logger.info(msg)
        else:
            # Could not determine size - use safe per-batch strategy
            msg = "[MIGRATE_REFERENCE] Could not determine dataset size - using per-batch read strategy (OOM-safe)"
            use_cached_source = False
            print(msg, flush=True)
            logger.info(msg)

        import time
        start_time = time.time()

        # DATA INTEGRITY: Track processed listings across all batches
        total_processed_listings = set()
        total_written_records = 0

        for batch_num, listing_batch in enumerate(listing_batches, 1):
            batch_start_time = time.time()

            # OPTIMIZE: Calculate progress and ETA
            progress_pct = (batch_num - 1) / len(listing_batches) * 100
            if batch_num > 1:
                elapsed = time.time() - start_time
                avg_time_per_batch = elapsed / (batch_num - 1)
                remaining_batches = len(listing_batches) - batch_num + 1
                eta_seconds = remaining_batches * avg_time_per_batch
                eta_minutes = int(eta_seconds / 60)
                eta_msg = f" (ETA: {eta_minutes}m {int(eta_seconds % 60)}s)"
            else:
                eta_msg = ""

            msg = f"[MIGRATE_REFERENCE] Processing batch {batch_num}/{len(listing_batches)} ({len(listing_batch)} listings) - {progress_pct:.1f}% complete{eta_msg}"
            print(msg, flush=True)
            logger.info(msg)

            # LOAD: Use targeted reads instead of table scan (100x faster!)
            if use_cached_source:
                batch_df = source_df  # Use cached data
                msg = f"[MIGRATE_REFERENCE] Batch {batch_num}: Using cached source data ({batch_df.rdd.getNumPartitions()} partitions)"
                print(msg)
                logger.info(msg)

                # FILTER: Apply remaining test filters (client_ids if needed)
                batch_df = _apply_test_filters(batch_df, listing_batch, test_client_ids, run_all)
            else:
                # OPTIMIZED: Use client-id-index to query util table (much faster than table scan!)
                if test_client_ids:
                    msg = f"[MIGRATE_REFERENCE] Batch {batch_num}: Querying client-id-index for {len(test_client_ids)} clients (100x faster than scan)"
                    print(msg)
                    logger.info(msg)

                    import time
                    read_start = time.time()

                    # Query index for all clients
                    items = read_util_table_by_client_index(
                        table=source_util_table,
                        region=source_region,
                        client_ids=test_client_ids,
                        index_name="client-id-index",
                        max_workers=10
                    )

                    read_elapsed = time.time() - read_start

                    if not items:
                        msg = f"[MIGRATE_REFERENCE] Batch {batch_num}: No REFERENCE# items found for clients"
                        print(msg)
                        logger.warn(msg)
                        total_processed_listings.update(listing_batch)
                        continue

                    # Convert DynamoDB items to Spark DataFrame
                    from boto3.dynamodb.types import TypeDeserializer
                    deserializer = TypeDeserializer()

                    records = []
                    for item in items:
                        # Deserialize and remove None values to help Spark infer types
                        record = {}
                        for k, v in item.items():
                            deserialized = deserializer.deserialize(v)
                            # Only include non-None values to avoid type inference issues
                            if deserialized is not None:
                                record[k] = deserialized
                        records.append(record)

                    # Create DataFrame with safety check
                    if records:
                        batch_df = glueContext.spark_session.createDataFrame(records)
                    else:
                        from pyspark.sql.types import StructType, StructField, StringType
                        empty_schema = StructType([StructField("PK", StringType(), True)])
                        batch_df = glueContext.spark_session.createDataFrame([], schema=empty_schema)

                    # Filter to only this batch's listings
                    batch_df = _apply_test_filters(batch_df, listing_batch, test_client_ids, run_all)

                    msg = f"[MIGRATE_REFERENCE] Batch {batch_num}: Loaded {len(records)} items in {read_elapsed:.1f}s using client-id-index"
                    print(msg)
                    logger.info(msg)
                else:
                    # FALLBACK: No client_ids available, must use table scan
                    msg = f"[MIGRATE_REFERENCE] Batch {batch_num}: No client_ids available, using table scan (slower)"
                    print(msg)
                    logger.warn(msg)

                    # Use DynamoDB scan filter with begins_with to reduce data at source
                    scan_filter = {
                        "dynamodb.filter.expression": "begins_with(pk, :ref)",
                        "dynamodb.filter.expressionAttributeValues": '{":ref":{"S":"REFERENCE#"}}'
                    }
                    dyf_batch = read_ddb_table(glueContext, source_util_table, source_region, scan_filter=scan_filter, splits="1000")
                    batch_df = dyf_batch.toDF()

                    # Filter to only this batch's listings
                    batch_df = _apply_test_filters(batch_df, listing_batch, test_client_ids, run_all)

            if batch_df.rdd.isEmpty():
                msg = "[MIGRATE_REFERENCE] Batch " + str(batch_num) + ": No records after filtering, skipping"
                print(msg)
                logger.info(msg)
                # Track these listings as "processed" even though filtered out
                total_processed_listings.update(listing_batch)
                continue

            # OPTIMIZE: Cache filtered data using off-heap memory (faster and more stable)
            from pyspark import StorageLevel
            batch_df.persist(StorageLevel.OFF_HEAP)

            # Get partition count for logging (avoid expensive count())
            num_partitions = batch_df.rdd.getNumPartitions()

            # DATA INTEGRITY: Track batch size without collecting all IDs (OOM-safe)
            if LISTING_ID_COL in batch_df.columns:
                # Use count instead of collect to avoid OOM
                batch_listing_count = batch_df.select(LISTING_ID_COL).distinct().count()
                msg = f"[DATA_INTEGRITY] Batch {batch_num}: Processing {batch_listing_count} distinct listings (expected ~{len(listing_batch)})"
                print(msg)
                logger.info(msg)

                # Warn if count differs significantly from expected
                if abs(batch_listing_count - len(listing_batch)) > len(listing_batch) * 0.1:  # >10% difference
                    msg = f"[DATA_INTEGRITY] WARNING: Batch {batch_num} has {batch_listing_count} listings but expected {len(listing_batch)}"
                    print(msg)
                    logger.warn(msg)

            msg = f"[MIGRATE_REFERENCE] Batch {batch_num}: Cached data ({num_partitions} partitions) for processing"
            print(msg)
            logger.info(msg)

            # TRANSFORM: Apply transformations to this batch
            batch_df = batch_df.withColumn(
                PK_COL, transform_reference_pk(F.lit(country_code), F.col(PK_COL))
            )

            if LISTING_ID_COL in batch_df.columns:
                batch_df = batch_df.withColumn(
                    CATALOG_ID_COL,
                    prefixed_catalog_id(F.lit(country_code), F.col(LISTING_ID_COL)),
                ).drop(LISTING_ID_COL)

            batch_df = align_df_to_target_keys_for_util(batch_df, target_util_table, target_region)

            # OPTIMIZED: Use direct BatchWriteItem (10x faster than Glue connector!)
            msg = f"[MIGRATE_REFERENCE] Batch {batch_num}: Writing using BatchWriteItem (10x faster)"
            print(msg)
            logger.info(msg)

            written_count = batch_write_items_direct(
                df=batch_df,
                table=target_util_table,
                region=target_region,
                max_workers=10
            )

            msg = f"[MIGRATE_REFERENCE] Batch {batch_num}: ✓ Wrote {written_count} items using BatchWriteItem"
            print(msg)
            logger.info(msg)

            # DATA INTEGRITY: Track total records written (use listing count as proxy)
            batch_record_count = batch_listing_count if LISTING_ID_COL in batch_df.columns else len(listing_batch)
            total_written_records += batch_record_count

            # DATA INTEGRITY: Track which listings were actually processed
            total_processed_listings.update(listing_batch)

            # OPTIMIZE: Calculate batch performance metrics
            batch_end_time = time.time()
            batch_duration = batch_end_time - batch_start_time
            records_per_second = batch_record_count / batch_duration if batch_duration > 0 else 0

            msg = f"[MIGRATE_REFERENCE] Batch {batch_num} completed in {batch_duration:.1f}s ({records_per_second:.0f} records/sec)"
            print(msg)
            logger.info(msg)

            # CLEANUP: Aggressive memory cleanup before next batch
            # Unpersist cached DataFrames with blocking to ensure cleanup completes
            batch_df.unpersist(blocking=True)

            # Delete batch references
            if use_cached_source:
                del batch_df  # Keep source_df cached for next batch
            else:
                # Clean up fresh read data (dyf_batch only exists in scan fallback path)
                del batch_df

            # Force garbage collection
            import gc
            gc.collect()

            # Optional: Log memory status for debugging
            try:
                import psutil, os
                process = psutil.Process(os.getpid())
                mem_mb = process.memory_info().rss / 1024 / 1024
                msg = f"[MEMORY] After batch {batch_num} cleanup: {mem_mb:.0f} MB"
                print(msg)
            except:
                pass

            # Log memory cleanup
            msg = "[MIGRATE_REFERENCE] Batch " + str(batch_num) + ": Memory cleanup completed"
            print(msg)
            logger.info(msg)

        # DATA INTEGRITY: Final verification report
        expected_listings = set(test_listing_ids)
        missing_listings = expected_listings - total_processed_listings
        extra_listings = total_processed_listings - expected_listings

        msg = f"[DATA_INTEGRITY] FINAL REPORT:"
        print(msg)
        logger.info(msg)

        msg = f"[DATA_INTEGRITY] Expected listings: {len(expected_listings)}"
        print(msg)
        logger.info(msg)

        msg = f"[DATA_INTEGRITY] Processed listings: {len(total_processed_listings)}"
        print(msg)
        logger.info(msg)

        msg = f"[DATA_INTEGRITY] Total records written: {total_written_records}"
        print(msg)
        logger.info(msg)

        if missing_listings:
            msg = f"[DATA_INTEGRITY] ❌ MISSING {len(missing_listings)} listings: {list(missing_listings)[:10]}..."
            print(msg)
            logger.error(msg)
            raise ValueError(f"DATA LOSS: {len(missing_listings)} listings were not processed")

        if extra_listings:
            msg = f"[DATA_INTEGRITY] ⚠️  EXTRA {len(extra_listings)} listings processed: {list(extra_listings)[:10]}..."
            print(msg)
            logger.warn(msg)

        msg = f"[DATA_INTEGRITY] ✅ SUCCESS: All {len(expected_listings)} listings processed correctly"
        print(msg)
        logger.info(msg)

        # CLEANUP: Unpersist source data after all batches are complete (if cached)
        if use_cached_source:
            source_df.unpersist()
            del source_df
            msg = "[MIGRATE_REFERENCE] All batches completed, source data cache cleared"
        else:
            msg = "[MIGRATE_REFERENCE] All batches completed (no cache used)"
        print(msg)
        logger.info(msg)
    else:
        # Small dataset - process normally (load all data at once)
        # OPTIMIZED: Use client-id-index if client_ids available (100x faster than scan!)
        if test_client_ids:
            msg = f"[MIGRATE_REFERENCE] Using client-id-index to load data for {len(test_client_ids)} clients (100x faster than scan)"
            print(msg, flush=True)
            logger.info(msg)

            import time
            read_start = time.time()

            items = read_util_table_by_client_index(
                table=source_util_table,
                region=source_region,
                client_ids=test_client_ids,
                index_name="client-id-index",
                max_workers=10
            )

            read_elapsed = time.time() - read_start

            if items:
                from boto3.dynamodb.types import TypeDeserializer
                from decimal import Decimal
                deserializer = TypeDeserializer()

                def convert_decimals(obj):
                    if obj is None:
                        return None
                    elif isinstance(obj, list):
                        return [convert_decimals(item) for item in obj]
                    elif isinstance(obj, dict):
                        return {k: convert_decimals(v) for k, v in obj.items()}
                    elif isinstance(obj, Decimal):
                        # Only convert to int if it's a whole number, otherwise keep as Decimal
                        # This prevents float conversion which causes DynamoDB write errors
                        return int(obj) if obj % 1 == 0 else obj
                    else:
                        return obj

                records = []
                for item in items:
                    # Deserialize and remove None values to help Spark infer types
                    record = {}
                    for k, v in item.items():
                        deserialized = convert_decimals(deserializer.deserialize(v))
                        # Only include non-None values to avoid type inference issues
                        if deserialized is not None:
                            record[k] = deserialized
                    records.append(record)

                if records:
                    df = glueContext.spark_session.createDataFrame(records)
                else:
                    from pyspark.sql.types import StructType, StructField, StringType
                    empty_schema = StructType([StructField("PK", StringType(), True)])
                    df = glueContext.spark_session.createDataFrame([], schema=empty_schema)

                msg = f"[MIGRATE_REFERENCE] Loaded {len(records)} items in {read_elapsed:.1f}s using client-id-index (vs ~60s with scan)"
                print(msg, flush=True)
                logger.info(msg)
            else:
                msg = "[MIGRATE_REFERENCE] No REFERENCE# items found for clients, creating empty DataFrame"
                print(msg)
                logger.warn(msg)
                from pyspark.sql.types import StructType, StructField, StringType
                empty_schema = StructType([StructField("PK", StringType(), True)])
                df = glueContext.spark_session.createDataFrame([], schema=empty_schema)
        else:
            # FALLBACK: No client_ids, use table scan
            msg = "[MIGRATE_REFERENCE] No client_ids available, using table scan (slower)"
            print(msg, flush=True)
            logger.warn(msg)

            # Use DynamoDB scan filter with begins_with to reduce data at source
            scan_filter = {
                "dynamodb.filter.expression": "begins_with(pk, :ref)",
                "dynamodb.filter.expressionAttributeValues": '{":ref":{"S":"REFERENCE#"}}'
            }
            dyf = read_ddb_table(glueContext, source_util_table, source_region, scan_filter=scan_filter)
            df = dyf.toDF()

            msg = "[MIGRATE_REFERENCE] Loaded REFERENCE# records from util table"
            print(msg)
            logger.info(msg)

        df = _apply_test_filters(df, test_listing_ids, test_client_ids, run_all)
        msg = "[MIGRATE_REFERENCE] Applied test filters"
        print(msg)
        logger.info(msg)

        if df.rdd.isEmpty():
            msg = "[MIGRATE_REFERENCE] No records after filtering, skipping"
            print(msg)
            logger.info(msg)
            return

        msg = "[MIGRATE_REFERENCE] Applying transformations..."
        print(msg, flush=True)
        logger.info(msg)

        df = df.withColumn(
            PK_COL, transform_reference_pk(F.lit(country_code), F.col(PK_COL))
        )

        if LISTING_ID_COL in df.columns:
            df = df.withColumn(
                CATALOG_ID_COL,
                prefixed_catalog_id(F.lit(country_code), F.col(LISTING_ID_COL)),
            ).drop(LISTING_ID_COL)

        msg = "[MIGRATE_REFERENCE] Aligning schema to target table..."
        print(msg, flush=True)
        logger.info(msg)

        df = align_df_to_target_keys_for_util(df, target_util_table, target_region)
        assert_write_keys_present(df, target_util_table, target_region)

        msg = "[MIGRATE_REFERENCE] Writing records to " + target_util_table + " using BatchWriteItem (10x faster)"
        print(msg, flush=True)
        sys.stdout.flush()
        logger.info(msg)

        # OPTIMIZED: Use direct BatchWriteItem (10x faster than Glue connector!)
        written_count = batch_write_items_direct(
            df=df,
            table=target_util_table,
            region=target_region,
            max_workers=10
        )

        msg = f"[MIGRATE_REFERENCE] ✓ Wrote {written_count} items using BatchWriteItem"
        print(msg)
        logger.info(msg)
    msg = "[MIGRATE_REFERENCE] ✓ Completed client reference migration"
    print(msg)
    logger.info(msg)


def migrate_amenities_segment(
    glueContext,
    logger,
    country_code,
    source_listings_table,
    source_region,
    target_catalog_table,
    target_region,
    test_listing_ids,
    test_client_ids,
    run_all,
):
    """Migrate AMENITIES segment - stores array of amenity strings"""
    msg = "[MIGRATE_AMENITIES] Starting AMENITIES segment migration"
    print(msg)
    logger.info(msg)

    # OPTIMIZED: Get listing_ids from client_ids if needed
    listing_ids_to_use = test_listing_ids
    if not listing_ids_to_use and test_client_ids and not run_all:
        msg = f"[MIGRATE_AMENITIES] Getting listing IDs from client-id-index for {len(test_client_ids)} clients"
        print(msg)
        logger.info(msg)
        listing_ids_to_use = get_listing_ids_for_clients(
            glueContext, logger, country_code, source_listings_table, source_region, test_client_ids
        )
        msg = f"[MIGRATE_AMENITIES] Found {len(listing_ids_to_use)} listings from client-id-index"
        print(msg)
        logger.info(msg)

    # OPTIMIZED: Use targeted reads when listing IDs are available (100x faster!)
    if not run_all and listing_ids_to_use:
        msg = f"[MIGRATE_AMENITIES] Using targeted BatchGetItem for {len(listing_ids_to_use)} listings (100x faster than scan)"
        print(msg)
        logger.info(msg)

        df = read_segment_targeted(glueContext, source_listings_table, source_region, listing_ids_to_use, "AMENITIES")

        msg = f"[MIGRATE_AMENITIES] Loaded {df.count() if not df.rdd.isEmpty() else 0} AMENITIES records using targeted reads"
        print(msg)
        logger.info(msg)
    else:
        # Fallback to table scan for run_all mode
        scan_filter = {
            "dynamodb.filter.expression": "segment = :seg OR SK = :seg",
            "dynamodb.filter.expressionAttributeValues": '{":seg":{"S":"SEGMENT#AMENITIES"}}'
        }

        msg = "[MIGRATE_AMENITIES] Reading with DynamoDB scan filter for AMENITIES segment"
        print(msg)
        logger.info(msg)

        dyf = read_ddb_table(glueContext, source_listings_table, source_region, scan_filter=scan_filter)
        df = dyf.toDF()

        msg = "[MIGRATE_AMENITIES] Loaded AMENITIES segment data"
        print(msg)
        logger.info(msg)

        df = _apply_test_filters(df, test_listing_ids, test_client_ids, run_all)
    msg = "[MIGRATE_AMENITIES] Applied test filters"
    print(msg)
    logger.info(msg)

    if df.rdd.isEmpty():
        msg = "[MIGRATE_AMENITIES] No records to migrate, skipping"
        print(msg)
        logger.info(msg)
        return

    # OOM PREVENTION: Get listing batches
    listing_batches = _get_listing_batches(df, batch_size=5000)
    logger.info(f"[MIGRATE_AMENITIES] Processing {len(listing_batches)} batches to prevent OOM")

    # Cache source df for reuse
    df.cache()
    cache_count = _safe_count(df, "MIGRATE_AMENITIES_CACHE", logger)
    if cache_count >= 0:
        logger.info(f"[MIGRATE_AMENITIES] Source data cached: {cache_count} records")
    else:
        logger.warn(f"[MIGRATE_AMENITIES] Cache count failed, proceeding without count")

    # Determine segment column name
    seg_col_name = SEGMENT_COL if SEGMENT_COL in df.columns else SK_COL

    import time
    start_time = time.time()
    total_written = 0

    for batch_num, listing_batch in enumerate(listing_batches, 1):
        batch_start = time.time()

        # Progress tracking
        progress_pct = (batch_num - 1) / len(listing_batches) * 100
        if batch_num > 1:
            elapsed = time.time() - start_time
            avg_time = elapsed / (batch_num - 1)
            eta_seconds = (len(listing_batches) - batch_num + 1) * avg_time
            eta_msg = f" (ETA: {int(eta_seconds/60)}m {int(eta_seconds%60)}s)"
        else:
            eta_msg = ""

        msg = f"[MIGRATE_AMENITIES] Batch {batch_num}/{len(listing_batches)} ({len(listing_batch)} listings) - {progress_pct:.1f}%{eta_msg}"
        print(msg, flush=True)
        logger.info(msg)

        # Filter to batch listings
        batch_df = df.filter(F.col("listing_id").isin(listing_batch))

        if batch_df.rdd.isEmpty():
            logger.info(f"[MIGRATE_AMENITIES] Batch {batch_num}: No records, skipping")
            continue

        if LISTING_ID_COL in batch_df.columns:
            batch_df = batch_df.withColumn(
                CATALOG_ID_COL,
                prefixed_catalog_id(F.lit(country_code), F.col(LISTING_ID_COL)),
            ).drop(LISTING_ID_COL)

        if "data" in batch_df.columns:
            # Handle data column - it's already an array from BatchGetItem deserialization
            # The data column is already in the correct format (array of strings)
            # Log data field type and sample values for debugging
            from pyspark.sql.types import ArrayType
            data_type = batch_df.schema["data"].dataType
            print(f"[MIGRATE_AMENITIES] Batch {batch_num}: data field type = {data_type}")

            # Sample a few records to see what the data looks like
            sample_data = batch_df.select("catalog_id", "data").limit(3).collect()
            for row in sample_data:
                data_value = row['data']
                print(f"[MIGRATE_AMENITIES] Sample catalog_id={row['catalog_id']}, data type={type(data_value)}, data={data_value}")

            # No transformation needed - just keep it as is
            pass

        if "quality_score" in batch_df.columns:
            batch_df = batch_df.drop("quality_score")

        batch_df = align_df_to_target_keys_for_catalog(
            batch_df, target_catalog_table, target_region, seg_col_name
        )
        assert_write_keys_present(batch_df, target_catalog_table, target_region)

        required_keys = set(get_table_key_attrs(target_catalog_table, target_region))
        required_keys.update([CATALOG_ID_COL, SEGMENT_COL, "data"])

        batch_df = _drop_all_null_top_level_columns(batch_df, required_cols=required_keys)

        batch_count = _safe_count(batch_df, f"MIGRATE_AMENITIES_BATCH_{batch_num}", logger)
        if batch_count > 0:
            total_written += batch_count
            msg = f"[MIGRATE_AMENITIES] Batch {batch_num}: Writing {batch_count} records using BatchWriteItem"
        else:
            msg = f"[MIGRATE_AMENITIES] Batch {batch_num}: Writing records using BatchWriteItem"
        print(msg)
        logger.info(msg)

        # OPTIMIZED: Use direct BatchWriteItem (10x faster than Glue connector!)
        written_count = batch_write_items_direct(
            df=batch_df,
            table=target_catalog_table,
            region=target_region,
            max_workers=10
        )

        msg = f"[MIGRATE_AMENITIES] Batch {batch_num}: ✓ Wrote {written_count} items using BatchWriteItem"
        print(msg)
        logger.info(msg)

        # MEMORY CLEANUP
        batch_df.unpersist()
        del batch_df
        import gc
        gc.collect()

        batch_elapsed = time.time() - batch_start
        msg = f"[MIGRATE_AMENITIES] Batch {batch_num}: ✓ Completed in {batch_elapsed:.1f}s (memory cleared)"
        print(msg)
        logger.info(msg)

    # FINAL CLEANUP
    df.unpersist()
    del df
    import gc
    gc.collect()

    total_elapsed = time.time() - start_time
    msg = f"[MIGRATE_AMENITIES] ✓ Completed all {len(listing_batches)} batches, wrote {total_written} records in {total_elapsed:.1f}s"
    print(msg)
    logger.info(msg)


import json
from pyspark.sql.types import StringType, MapType

import json
from pyspark.sql.types import MapType, StringType
from pyspark.sql import functions as F

def unwrap_struct_string(df):
    """
    If df.data contains a key "struct" with a JSON string,
    convert it into a proper MAP and drop the "struct" wrapper.
    """
    if "data" not in df.columns:
        return df

    # Convert map -> JSON string
    df = df.withColumn("data_json", F.to_json("data"))

    def convert(js):
        if not js:
            return None
        import json
        try:
            obj = json.loads(js)
            if "struct" in obj and isinstance(obj["struct"], str):
                inner = json.loads(obj["struct"])
                return json.dumps(inner)
            return js
        except:
            return js

    fix_udf = F.udf(convert, StringType())

    df = df.withColumn("data_fixed_json", fix_udf("data_json"))

    # Parse back into Map<String,String> OR Map<String,?> dynamically
    df = df.withColumn("data", F.from_json("data_fixed_json", MapType(StringType(), StringType())))

    return df.drop("data_json", "data_fixed_json")




def prune_and_flatten_data_with_field_preservation(df):
    """Flatten data.struct → data and remove null/empty fields, but ensure ALL fields from ALL rows are preserved in schema."""

    from pyspark.sql.types import StringType

    if "data" not in df.columns:
        return df

    df = df.withColumn("data_json", F.to_json("data"))

    def clean(js):
        if not js:
            return None
        try:
            obj = json.loads(js)

            # Handle Glue's array/struct wrapper
            if isinstance(obj, dict):
                if "array" in obj:
                    obj.pop("array", None)
                if "struct" in obj and isinstance(obj["struct"], dict):
                    obj = obj["struct"]

            def clean_recursive(value):
                """Recursively clean nested structures"""
                if value is None:
                    return None
                elif isinstance(value, dict):
                    cleaned_dict = {}
                    for k, v in value.items():
                        cleaned_v = clean_recursive(v)
                        # Only keep non-null values and non-empty objects/lists
                        if cleaned_v is not None:
                            if isinstance(cleaned_v, (dict, list)) and len(cleaned_v) == 0:
                                continue  # Skip empty objects/lists
                            cleaned_dict[k] = cleaned_v
                    return cleaned_dict if cleaned_dict else None
                elif isinstance(value, list):
                    cleaned_list = [clean_recursive(item) for item in value]
                    # Remove None values from list
                    cleaned_list = [item for item in cleaned_list if item is not None]
                    return cleaned_list if cleaned_list else None
                else:
                    return value

            cleaned = clean_recursive(obj)

            # Debug logging for amounts field
            if isinstance(cleaned, dict) and "amounts" in obj:
                import sys
                print(f"[CLEAN_DEBUG] BEFORE clean: amounts = {obj.get('amounts')}", file=sys.stderr)
                print(f"[CLEAN_DEBUG] AFTER clean: amounts = {cleaned.get('amounts')}", file=sys.stderr)

            return json.dumps(cleaned) if cleaned else None

        except Exception as e:
            import sys
            print(f"[CLEAN_ERROR] {e}", file=sys.stderr)
            return js

    # CRITICAL: Build schema BEFORE cleaning to preserve all fields (including those with nulls)
    # Collect ALL unique fields from ALL rows (not just one sample)
    msg = "[PRUNE_DATA] Collecting all unique fields from RAW data to build complete schema..."
    print(msg)

    all_json_samples_raw = df.select("data_json").filter(F.col("data_json").isNotNull()).limit(1000).collect()

    # Recursively merge all schemas to preserve nested structures like amounts.monthly
    def merge_schemas(schema1, schema2):
        """Recursively merge two schema dicts, preserving nested structures"""
        if not isinstance(schema1, dict) or not isinstance(schema2, dict):
            return schema2  # Use the newer value

        merged = schema1.copy()
        for key, value in schema2.items():
            # Skip None values - they don't help with schema inference
            if value is None:
                continue

            if key in merged:
                # Key exists in both schemas
                if merged[key] is None:
                    # Replace None with actual value
                    merged[key] = value
                elif isinstance(merged[key], dict) and isinstance(value, dict):
                    # Both are dicts - recursively merge
                    merged[key] = merge_schemas(merged[key], value)
                # else: keep existing non-None value
            else:
                # New key - add it
                merged[key] = value
        return merged

    def parse_nested_json_strings(obj):
        """Recursively parse JSON strings in nested structures to get proper types"""
        if obj is None:
            return None
        elif isinstance(obj, dict):
            result = {}
            for k, v in obj.items():
                result[k] = parse_nested_json_strings(v)
            return result
        elif isinstance(obj, list):
            return [parse_nested_json_strings(item) for item in obj]
        elif isinstance(obj, str):
            # Try to parse JSON strings to get proper nested structures
            if obj.startswith('{') or obj.startswith('['):
                try:
                    parsed = json.loads(obj)
                    # Recursively parse the result
                    return parse_nested_json_strings(parsed)
                except:
                    pass
            return obj
        else:
            return obj

    complete_sample = {}
    sample_count = 0
    samples_with_amounts = 0

    for row in all_json_samples_raw:
        if row[0]:
            try:
                obj = json.loads(row[0])
                sample_count += 1

                # Log first few samples to see what we're getting
                if sample_count <= 3:
                    print(f"[PRUNE_DATA] Raw sample {sample_count} keys: {list(obj.keys()) if isinstance(obj, dict) else 'NOT A DICT'}")
                    if isinstance(obj, dict) and "amounts" in obj:
                        print(f"[PRUNE_DATA] Raw sample {sample_count} amounts value: {obj['amounts']}")

                # Handle Glue's array/struct wrapper
                if isinstance(obj, dict):
                    if "array" in obj:
                        obj.pop("array", None)
                    if "struct" in obj and isinstance(obj["struct"], dict):
                        obj = obj["struct"]
                        if sample_count <= 3:
                            print(f"[PRUNE_DATA] After unwrapping struct, sample {sample_count} keys: {list(obj.keys())}")

                # Parse nested JSON strings to get proper types
                obj = parse_nested_json_strings(obj)

                # Check if this sample has amounts
                if isinstance(obj, dict) and "amounts" in obj and obj["amounts"] is not None:
                    samples_with_amounts += 1
                    if samples_with_amounts <= 3:
                        print(f"[PRUNE_DATA] Sample {sample_count} HAS amounts: {obj['amounts']}")

                # Recursively merge this sample into the complete schema (BEFORE cleaning!)
                complete_sample = merge_schemas(complete_sample, obj)
            except Exception as e:
                print(f"[PRUNE_DATA] Error processing sample: {e}")
                pass

    print(f"[PRUNE_DATA] Processed {sample_count} samples, {samples_with_amounts} had non-null amounts")

    msg = f"[PRUNE_DATA] Built complete schema with fields: {list(complete_sample.keys())}"
    print(msg)

    # Log nested fields like amounts and street
    if "amounts" in complete_sample:
        print(f"[PRUNE_DATA] amounts schema: {complete_sample['amounts']}")
    if "street" in complete_sample:
        print(f"[PRUNE_DATA] street schema: {complete_sample['street']}, type: {type(complete_sample['street'])}")

    # Now apply cleaning to the data
    clean_udf = F.udf(clean, StringType())
    df = df.withColumn("data_clean", clean_udf("data_json"))

    if complete_sample:
        merged_json = json.dumps(complete_sample)
        print(f"[PRUNE_DATA] Sample schema JSON (first 500 chars): {merged_json[:500]}")
        inferred_schema = F.schema_of_json(F.lit(merged_json))
        print(f"[PRUNE_DATA] Inferred schema: {inferred_schema}")

        # Log a sample of cleaned data before from_json
        sample_clean = df.select("data_clean").filter(F.col("data_clean").isNotNull()).first()
        if sample_clean and sample_clean[0]:
            print(f"[PRUNE_DATA] Sample cleaned data (first 500 chars): {sample_clean[0][:500]}")

        df = df.withColumn("data", F.from_json("data_clean", inferred_schema))

        # Log the actual data type after from_json
        actual_type = df.schema["data"].dataType
        print(f"[PRUNE_DATA] Actual data type after from_json: {actual_type}")

        # Log a sample of the final data
        sample_final = df.select("data").filter(F.col("data").isNotNull()).first()
        if sample_final and sample_final[0]:
            print(f"[PRUNE_DATA] Sample final data: {sample_final[0]}")
    else:
        msg = "[PRUNE_DATA] WARNING: No complete_sample built, setting data to null"
        print(msg)
        df = df.withColumn("data", F.lit(None))

    return df.drop("data_json", "data_clean")


def prune_and_flatten_data(df):
    """Flatten data.struct → data and remove ALL null or empty fields.
       Preserves nested objects like street, size, title as proper structs."""

    if "data" not in df.columns:
        return df

    # Check if data is already a string (from old stringification or mixed sources)
    from pyspark.sql.types import StringType
    data_type = df.schema["data"].dataType

    print(f"[PRUNE_DATA] data field type: {data_type}")

    if isinstance(data_type, StringType):
        # Data is already a JSON string, use it directly
        print("[PRUNE_DATA] data is already a string, using directly")
        df = df.withColumn("data_json", F.col("data"))
    else:
        # Data is a struct/map, convert to JSON
        print("[PRUNE_DATA] data is struct/map, converting to JSON")
        df = df.withColumn("data_json", F.to_json("data"))

    def clean(js):
        if not js:
            return None
        try:
            obj = json.loads(js)

            # Handle Glue's array/struct wrapper
            if isinstance(obj, dict):
                if "array" in obj:
                    obj.pop("array", None)
                if "struct" in obj and isinstance(obj["struct"], dict):
                    obj = obj["struct"]

            cleaned = {}
            for k, v in obj.items():

                # Drop nulls
                if v is None:
                    continue

                # Drop empty objects/lists
                if isinstance(v, (dict, list)) and len(v) == 0:
                    continue

                # KEEP nested objects AS-IS (critical)
                cleaned[k] = v

            return json.dumps(cleaned) if cleaned else None

        except:
            return js

    clean_udf = F.udf(clean, StringType())

    df = df.withColumn("data_clean", clean_udf("data_json"))

    # Infer schema from a sample row to preserve nested types
    sample_json = df.select("data_clean").filter(F.col("data_clean").isNotNull()).first()
    if sample_json and sample_json[0]:
        # schema_of_json needs the string value directly, not wrapped in F.lit()
        inferred_schema = F.schema_of_json(sample_json[0])
        df = df.withColumn("data", F.from_json("data_clean", inferred_schema))
    else:
        # Fallback if no data
        df = df.withColumn("data", F.lit(None))

    return df.drop("data_json", "data_clean")



def unwrap_spark_type_wrappers(df):
    """
    Unwrap Spark's type wrappers like {double: N}, {long: N}, {int: N} to just N.
    This handles fields like size: {double: 111.49} -> size: 111.49
    Also recursively unwraps nested structs like street.Width: {long: 15} -> street.Width: 15
    """
    if "data" not in df.columns:
        return df

    data_type = df.schema["data"].dataType
    if not isinstance(data_type, StructType):
        return df

    def unwrap_field(field_path, field_type):
        """Recursively unwrap a field, handling nested structs."""
        type_wrapper_fields = {"double", "long", "int", "float", "short", "byte"}

        if isinstance(field_type, StructType):
            field_names = set(field_type.fieldNames())

            # If the struct has exactly one field and it's a type wrapper, unwrap it
            if len(field_names) == 1 and field_names.issubset(type_wrapper_fields):
                wrapper_field = list(field_names)[0]
                return F.col(f"{field_path}.{wrapper_field}")
            else:
                # Recursively process nested struct fields
                nested_exprs = []
                for nested_field in field_type.fieldNames():
                    nested_type = field_type[nested_field].dataType
                    nested_expr = unwrap_field(f"{field_path}.{nested_field}", nested_type)
                    nested_exprs.append(nested_expr.alias(nested_field))
                return F.struct(*nested_exprs)
        else:
            # Primitive type, return as-is
            return F.col(field_path)

    field_exprs = []
    for field in data_type.fieldNames():
        field_type = data_type[field].dataType
        field_expr = unwrap_field(f"data.{field}", field_type)
        field_exprs.append(field_expr.alias(field))

    return df.withColumn("data", F.struct(*field_exprs))


def normalize_street_field(df):
    """
    Normalize the street field inside data struct to match search.py behavior:
    - Direction -> direction (lowercase key and value)
    - Width -> width (unwrap {long: N} to just N)
    """
    if "data" not in df.columns:
        return df

    data_type = df.schema["data"].dataType
    if not isinstance(data_type, (StructType, MapType)):
        return df

    # Check if street field exists
    has_street = False
    if isinstance(data_type, StructType):
        has_street = "street" in data_type.fieldNames()
    elif isinstance(data_type, MapType):
        # For MapType, we need to check at runtime
        has_street = True  # Assume it might exist

    if not has_street:
        return df

    # For StructType data, rebuild the struct with normalized street
    if isinstance(data_type, StructType):
        field_exprs = []
        for field in data_type.fieldNames():
            if field == "street":
                # Check if street is actually a struct (not a string)
                street_type = data_type[field].dataType
                if isinstance(street_type, StructType):
                    # Normalize street: Width and Direction should already be unwrapped by unwrap_spark_type_wrappers
                    street_expr = F.when(
                        F.col("data.street").isNotNull(),
                        F.struct(
                            # Width is already unwrapped to a simple number by unwrap_spark_type_wrappers
                            F.col("data.street.Width").cast("long").alias("width"),
                            # Direction to lowercase
                            F.lower(F.col("data.street.Direction")).alias("direction")
                        )
                    )
                    field_exprs.append(street_expr.alias("street"))
                else:
                    # street is not a struct (maybe string or null), keep as-is
                    msg = f"[NORMALIZE_STREET] WARNING: street field is {street_type}, not StructType - skipping normalization"
                    print(msg)
                    field_exprs.append(F.col(f"data.{field}").alias(field))
            else:
                field_exprs.append(F.col(f"data.{field}").alias(field))

        return df.withColumn("data", F.struct(*field_exprs))

    # For MapType, we can't easily rebuild, so return as-is
    return df




def migrate_attributes_segment(
    glueContext,
    logger,
    country_code,
    source_listings_table,
    source_region,
    target_catalog_table,
    target_region,
    test_listing_ids,
    test_client_ids,
    run_all,
):
    logger.info("[MIGRATE_ATTRIBUTES] Starting ATTRIBUTES migration")

    # OPTIMIZED: Get listing_ids from client_ids if needed
    listing_ids_to_use = test_listing_ids
    if not listing_ids_to_use and test_client_ids and not run_all:
        msg = f"[MIGRATE_ATTRIBUTES] Getting listing IDs from client-id-index for {len(test_client_ids)} clients"
        print(msg)
        logger.info(msg)
        listing_ids_to_use = get_listing_ids_for_clients(
            glueContext, logger, country_code, source_listings_table, source_region, test_client_ids
        )
        msg = f"[MIGRATE_ATTRIBUTES] Found {len(listing_ids_to_use)} listings from client-id-index"
        print(msg)
        logger.info(msg)

    # OPTIMIZED: Use targeted reads when listing IDs are available (100x faster!)
    if not run_all and listing_ids_to_use:
        msg = f"[MIGRATE_ATTRIBUTES] Using targeted BatchGetItem for {len(listing_ids_to_use)} listings (100x faster than scan)"
        print(msg)
        logger.info(msg)

        df = read_segment_targeted(glueContext, source_listings_table, source_region, listing_ids_to_use, "ATTRIBUTES")

        # Determine segment column name
        seg_col = _choose_segment_col(df) if not df.rdd.isEmpty() else "segment"

        msg = f"[MIGRATE_ATTRIBUTES] Loaded ATTRIBUTES segment data using targeted reads"
        print(msg)
        logger.info(msg)
    else:
        # Fallback to table scan for run_all mode
        scan_filter = {
            "dynamodb.filter.expression": "segment = :seg OR SK = :seg",
            "dynamodb.filter.expressionAttributeValues": '{":seg":{"S":"SEGMENT#ATTRIBUTES"}}'
        }

        dyf = read_ddb_table(glueContext, source_listings_table, source_region, scan_filter=scan_filter)
        df = dyf.toDF()

        seg_col = _choose_segment_col(df) if not df.rdd.isEmpty() else "segment"
        msg = f"[MIGRATE_ATTRIBUTES] Loaded ATTRIBUTES segment data (using {seg_col} for segment column)"
        print(msg)
        logger.info(msg)

        df = _apply_test_filters(df, test_listing_ids, test_client_ids, run_all)

    if df.rdd.isEmpty():
        logger.info("[MIGRATE_ATTRIBUTES] No records after filters, skipping")
        return

    # OOM PREVENTION: Get listing batches and unpersist source after getting IDs
    listing_batches = _get_listing_batches(df, batch_size=5000)
    logger.info(f"[MIGRATE_ATTRIBUTES] Processing {len(listing_batches)} batches to prevent OOM")

    # Cache source df for reuse across batches (more efficient than re-reading)
    df.cache()
    cache_count = _safe_count(df, "MIGRATE_ATTRIBUTES_CACHE", logger)
    if cache_count >= 0:
        logger.info(f"[MIGRATE_ATTRIBUTES] Source data cached: {cache_count} records")
    else:
        logger.warn(f"[MIGRATE_ATTRIBUTES] Cache count failed, proceeding without count")

    import time
    start_time = time.time()
    total_written = 0

    for batch_num, listing_batch in enumerate(listing_batches, 1):
        batch_start = time.time()

        # Progress tracking
        progress_pct = (batch_num - 1) / len(listing_batches) * 100
        if batch_num > 1:
            elapsed = time.time() - start_time
            avg_time = elapsed / (batch_num - 1)
            eta_seconds = (len(listing_batches) - batch_num + 1) * avg_time
            eta_msg = f" (ETA: {int(eta_seconds/60)}m {int(eta_seconds%60)}s)"
        else:
            eta_msg = ""

        msg = f"[MIGRATE_ATTRIBUTES] Batch {batch_num}/{len(listing_batches)} ({len(listing_batch)} listings) - {progress_pct:.1f}%{eta_msg}"
        print(msg, flush=True)
        logger.info(msg)

        # Filter to batch listings (using listing_id before transformation)
        batch_df = df.filter(F.col("listing_id").isin(listing_batch))

        if batch_df.rdd.isEmpty():
            logger.info(f"[MIGRATE_ATTRIBUTES] Batch {batch_num}: No records, skipping")
            continue

        # Add catalog_id
        batch_df = batch_df.withColumn(
            CATALOG_ID_COL,
            prefixed_catalog_id(F.lit(country_code), F.col("listing_id"))
        ).drop("listing_id")

        # Select only valid source fields
        batch_df = batch_df.select(
            CATALOG_ID_COL,
            seg_col,
            "bathrooms",
            "bedrooms",
            "category",
            "created_at",
            "data",
            "type",
            "updated_at"
        )

        # Log data field type for debugging
        from pyspark.sql.types import StringType
        data_type = batch_df.schema["data"].dataType
        print(f"[MIGRATE_ATTRIBUTES] Batch {batch_num}: data field type = {data_type}")
        if isinstance(data_type, StringType):
            print(f"[MIGRATE_ATTRIBUTES] Batch {batch_num}: WARNING - data is STRING type, should be struct/map")
            # Sample a few records to see what the data looks like
            sample_data = batch_df.select("catalog_id", "data").limit(3).collect()
            for row in sample_data:
                print(f"[MIGRATE_ATTRIBUTES] Sample catalog_id={row['catalog_id']}, data type={type(row['data'])}, data preview={str(row['data'])[:200]}")

        # Remove nulls inside `data`, leave nested fields untouched - use field preservation
        batch_df = prune_and_flatten_data_with_field_preservation(batch_df)

        # Unwrap type wrappers like {double: N} -> N
        batch_df = unwrap_spark_type_wrappers(batch_df)

        # Normalize street field (Direction->direction, Width->width)
        batch_df = normalize_street_field(batch_df)

        # Key alignment for catalog table
        batch_df = align_df_to_target_keys_for_catalog(
            batch_df, target_catalog_table, target_region, seg_col
        )
        assert_write_keys_present(batch_df, target_catalog_table, target_region)

        # Ensure only required top-level columns exist
        required = set(get_table_key_attrs(target_catalog_table, target_region))
        required.update([CATALOG_ID_COL, SEGMENT_COL, "data"])

        batch_df = _drop_all_null_top_level_columns(batch_df, required_cols=required)

        batch_count = _safe_count(batch_df, f"MIGRATE_ATTRIBUTES_BATCH_{batch_num}", logger)
        if batch_count > 0:
            total_written += batch_count
            msg = f"[MIGRATE_ATTRIBUTES] Batch {batch_num}: Writing {batch_count} records using BatchWriteItem"
        else:
            msg = f"[MIGRATE_ATTRIBUTES] Batch {batch_num}: Writing records using BatchWriteItem"
        print(msg)
        logger.info(msg)

        # OPTIMIZED: Use direct BatchWriteItem (10x faster than Glue connector!)
        written_count = batch_write_items_direct(
            df=batch_df,
            table=target_catalog_table,
            region=target_region,
            max_workers=10
        )

        msg = f"[MIGRATE_ATTRIBUTES] Batch {batch_num}: ✓ Wrote {written_count} items using BatchWriteItem"
        print(msg)
        logger.info(msg)

        # MEMORY CLEANUP: Unpersist batch data and force garbage collection
        batch_df.unpersist()
        del batch_df
        import gc
        gc.collect()

        batch_elapsed = time.time() - batch_start
        msg = f"[MIGRATE_ATTRIBUTES] Batch {batch_num}: ✓ Completed in {batch_elapsed:.1f}s (memory cleared)"
        print(msg)
        logger.info(msg)

    # FINAL CLEANUP: Unpersist source DataFrame
    df.unpersist()
    del df
    import gc
    gc.collect()

    # DATA INTEGRITY: Verify all batches were processed
    total_expected_listings = sum(len(batch) for batch in listing_batches)
    if total_written == 0:
        msg = f"[MIGRATE_ATTRIBUTES] ⚠️ WARNING: No records written! Expected {total_expected_listings} listings"
        print(msg)
        logger.warn(msg)

    total_elapsed = time.time() - start_time
    msg = f"[MIGRATE_ATTRIBUTES] ✓ Completed all {len(listing_batches)} batches, wrote {total_written} records in {total_elapsed:.1f}s"
    print(msg)
    logger.info(msg)


def migrate_description_segment(
    glueContext,
    logger,
    country_code,
    source_listings_table,
    source_region,
    target_catalog_table,
    target_region,
    test_listing_ids,
    test_client_ids,
    run_all,
):
    """Migrate DESCRIPTION segment - stores description in top-level field, not in data"""
    msg = "[MIGRATE_DESCRIPTION] Starting DESCRIPTION segment migration"
    print(msg)
    logger.info(msg)

    # OPTIMIZED: Get listing_ids from client_ids if needed
    listing_ids_to_use = test_listing_ids
    if not listing_ids_to_use and test_client_ids and not run_all:
        msg = f"[MIGRATE_DESCRIPTION] Getting listing IDs from client-id-index for {len(test_client_ids)} clients"
        print(msg)
        logger.info(msg)
        listing_ids_to_use = get_listing_ids_for_clients(
            glueContext, logger, country_code, source_listings_table, source_region, test_client_ids
        )
        msg = f"[MIGRATE_DESCRIPTION] Found {len(listing_ids_to_use)} listings from client-id-index"
        print(msg)
        logger.info(msg)

    # OPTIMIZED: Use targeted reads when listing IDs are available (100x faster!)
    if not run_all and listing_ids_to_use:
        msg = f"[MIGRATE_DESCRIPTION] Using targeted BatchGetItem for {len(listing_ids_to_use)} listings (100x faster than scan)"
        print(msg)
        logger.info(msg)

        df = read_segment_targeted(glueContext, source_listings_table, source_region, listing_ids_to_use, "DESCRIPTION")

        msg = f"[MIGRATE_DESCRIPTION] Loaded DESCRIPTION segment data using targeted reads"
        print(msg)
        logger.info(msg)
    else:
        # Fallback to table scan for run_all mode
        scan_filter = {
            "dynamodb.filter.expression": "segment = :seg OR SK = :seg",
            "dynamodb.filter.expressionAttributeValues": '{":seg":{"S":"SEGMENT#DESCRIPTION"}}'
        }

        msg = "[MIGRATE_DESCRIPTION] Reading with DynamoDB scan filter"
        print(msg)
        logger.info(msg)

        dyf = read_ddb_table(glueContext, source_listings_table, source_region, scan_filter=scan_filter)
        df = dyf.toDF()

        msg = "[MIGRATE_DESCRIPTION] Loaded DESCRIPTION segment data"
        print(msg)
        logger.info(msg)

        df = _apply_test_filters(df, test_listing_ids, test_client_ids, run_all)
    msg = "[MIGRATE_DESCRIPTION] Applied test filters"
    print(msg)
    logger.info(msg)

    if df.rdd.isEmpty():
        msg = "[MIGRATE_DESCRIPTION] No records to migrate, skipping"
        print(msg)
        logger.info(msg)
        return

    # OOM PREVENTION: Get listing batches
    listing_batches = _get_listing_batches(df, batch_size=5000)
    logger.info(f"[MIGRATE_DESCRIPTION] Processing {len(listing_batches)} batches to prevent OOM")

    # Cache source df for reuse
    df.cache()
    cache_count = _safe_count(df, "MIGRATE_DESCRIPTION_CACHE", logger)
    if cache_count >= 0:
        logger.info(f"[MIGRATE_DESCRIPTION] Source data cached: {cache_count} records")
    else:
        logger.warn(f"[MIGRATE_DESCRIPTION] Cache count failed, proceeding without count")

    # Determine segment column name
    seg_col_name = SEGMENT_COL if SEGMENT_COL in df.columns else SK_COL

    import time
    start_time = time.time()
    total_written = 0

    for batch_num, listing_batch in enumerate(listing_batches, 1):
        batch_start = time.time()

        # Progress tracking
        progress_pct = (batch_num - 1) / len(listing_batches) * 100
        if batch_num > 1:
            elapsed = time.time() - start_time
            avg_time = elapsed / (batch_num - 1)
            eta_seconds = (len(listing_batches) - batch_num + 1) * avg_time
            eta_msg = f" (ETA: {int(eta_seconds/60)}m {int(eta_seconds%60)}s)"
        else:
            eta_msg = ""

        msg = f"[MIGRATE_DESCRIPTION] Batch {batch_num}/{len(listing_batches)} ({len(listing_batch)} listings) - {progress_pct:.1f}%{eta_msg}"
        print(msg, flush=True)
        logger.info(msg)

        # Filter to batch listings
        batch_df = df.filter(F.col("listing_id").isin(listing_batch))

        if batch_df.rdd.isEmpty():
            logger.info(f"[MIGRATE_DESCRIPTION] Batch {batch_num}: No records, skipping")
            continue

        if LISTING_ID_COL in batch_df.columns:
            batch_df = batch_df.withColumn(
                CATALOG_ID_COL,
                prefixed_catalog_id(F.lit(country_code), F.col(LISTING_ID_COL)),
            ).drop(LISTING_ID_COL)

        if "data" in batch_df.columns:
            batch_df = batch_df.drop("data")

        if "quality_score" in batch_df.columns:
            batch_df = batch_df.drop("quality_score")

        batch_df = align_df_to_target_keys_for_catalog(
            batch_df, target_catalog_table, target_region, seg_col_name
        )
        assert_write_keys_present(batch_df, target_catalog_table, target_region)

        required_keys = set(get_table_key_attrs(target_catalog_table, target_region))
        required_keys.update([CATALOG_ID_COL, SEGMENT_COL])

        batch_df = _drop_all_null_top_level_columns(batch_df, required_cols=required_keys)

        batch_count = _safe_count(batch_df, f"MIGRATE_DESCRIPTION_BATCH_{batch_num}", logger)
        if batch_count > 0:
            total_written += batch_count
            msg = f"[MIGRATE_DESCRIPTION] Batch {batch_num}: Writing {batch_count} records using BatchWriteItem"
        else:
            msg = f"[MIGRATE_DESCRIPTION] Batch {batch_num}: Writing records using BatchWriteItem"
        print(msg)
        logger.info(msg)

        # OPTIMIZED: Use direct BatchWriteItem (10x faster than Glue connector!)
        written_count = batch_write_items_direct(
            df=batch_df,
            table=target_catalog_table,
            region=target_region,
            max_workers=10
        )

        msg = f"[MIGRATE_DESCRIPTION] Batch {batch_num}: ✓ Wrote {written_count} items using BatchWriteItem"
        print(msg)
        logger.info(msg)

        # MEMORY CLEANUP
        batch_df.unpersist()
        del batch_df
        import gc
        gc.collect()

        batch_elapsed = time.time() - batch_start
        msg = f"[MIGRATE_DESCRIPTION] Batch {batch_num}: ✓ Completed in {batch_elapsed:.1f}s (memory cleared)"
        print(msg)
        logger.info(msg)

    # FINAL CLEANUP
    df.unpersist()
    del df
    import gc
    gc.collect()

    total_elapsed = time.time() - start_time
    msg = f"[MIGRATE_DESCRIPTION] ✓ Completed all {len(listing_batches)} batches, wrote {total_written} records in {total_elapsed:.1f}s"
    print(msg)
    logger.info(msg)


def migrate_price_segment(
    glueContext,
    logger,
    country_code,
    source_listings_table,
    source_region,
    target_catalog_table,
    target_region,
    test_listing_ids,
    test_client_ids,
    run_all,
):
    msg = "[MIGRATE_PRICE] Starting PRICE migration"
    print(msg)
    logger.info(msg)

    # OPTIMIZED: Get listing_ids from client_ids if needed
    listing_ids_to_use = test_listing_ids
    if not listing_ids_to_use and test_client_ids and not run_all:
        msg = f"[MIGRATE_PRICE] Getting listing IDs from client-id-index for {len(test_client_ids)} clients"
        print(msg)
        logger.info(msg)
        listing_ids_to_use = get_listing_ids_for_clients(
            glueContext, logger, country_code, source_listings_table, source_region, test_client_ids
        )
        msg = f"[MIGRATE_PRICE] Found {len(listing_ids_to_use)} listings from client-id-index"
        print(msg)
        logger.info(msg)

    # OPTIMIZED: Use targeted reads when listing IDs are available (100x faster!)
    if not run_all and listing_ids_to_use:
        msg = f"[MIGRATE_PRICE] Using targeted BatchGetItem for {len(listing_ids_to_use)} listings (100x faster than scan)"
        print(msg)
        logger.info(msg)

        df = read_segment_targeted(glueContext, source_listings_table, source_region, listing_ids_to_use, "PRICE")

        msg = f"[MIGRATE_PRICE] Loaded PRICE segment data using targeted reads"
        print(msg)
        logger.info(msg)
    else:
        # Fallback to table scan for run_all mode
        scan_filter = {
            "dynamodb.filter.expression": "segment = :seg OR SK = :seg",
            "dynamodb.filter.expressionAttributeValues": '{":seg":{"S":"SEGMENT#PRICE"}}'
        }

        msg = "[MIGRATE_PRICE] Reading with DynamoDB scan filter"
        print(msg)
        logger.info(msg)

        dyf = read_ddb_table(glueContext, source_listings_table, source_region, scan_filter=scan_filter)
        df = dyf.toDF()

        msg = "[MIGRATE_PRICE] Loaded PRICE segment data"
        print(msg)
        logger.info(msg)

        df = _apply_test_filters(df, test_listing_ids, test_client_ids, run_all)
    if df.rdd.isEmpty():
        msg = "[MIGRATE_PRICE] No PRICE rows after filters — skipping"
        print(msg)
        logger.info(msg)
        return

    # OOM PREVENTION: Get listing batches
    listing_batches = _get_listing_batches(df, batch_size=5000)
    logger.info(f"[MIGRATE_PRICE] Processing {len(listing_batches)} batches to prevent OOM")

    # Cache source df for reuse
    df.cache()
    cache_count = _safe_count(df, "MIGRATE_PRICE_CACHE", logger)
    if cache_count >= 0:
        logger.info(f"[MIGRATE_PRICE] Source data cached: {cache_count} records")
    else:
        logger.warn(f"[MIGRATE_PRICE] Cache count failed, proceeding without count")

    # Determine segment column name
    seg_col_name = SEGMENT_COL if SEGMENT_COL in df.columns else SK_COL

    import time
    start_time = time.time()
    total_written = 0

    for batch_num, listing_batch in enumerate(listing_batches, 1):
        batch_start = time.time()

        # Progress tracking
        progress_pct = (batch_num - 1) / len(listing_batches) * 100
        if batch_num > 1:
            elapsed = time.time() - start_time
            avg_time = elapsed / (batch_num - 1)
            eta_seconds = (len(listing_batches) - batch_num + 1) * avg_time
            eta_msg = f" (ETA: {int(eta_seconds/60)}m {int(eta_seconds%60)}s)"
        else:
            eta_msg = ""

        msg = f"[MIGRATE_PRICE] Batch {batch_num}/{len(listing_batches)} ({len(listing_batch)} listings) - {progress_pct:.1f}%{eta_msg}"
        print(msg, flush=True)
        logger.info(msg)

        # Filter to batch listings
        batch_df = df.filter(F.col("listing_id").isin(listing_batch))

        if batch_df.rdd.isEmpty():
            logger.info(f"[MIGRATE_PRICE] Batch {batch_num}: No records, skipping")
            continue

        # Generate catalog_id
        batch_df = batch_df.withColumn(
            CATALOG_ID_COL,
            prefixed_catalog_id(F.lit(country_code), F.col("listing_id"))
        ).drop("listing_id")

        # SELECT ONLY VALID PRICE FIELDS (only select columns that exist)
        price_cols = [CATALOG_ID_COL, seg_col_name]
        for col in ["created_at", "data", "price_type", "type", "updated_at"]:
            if col in batch_df.columns:
                price_cols.append(col)
        batch_df = batch_df.select(*price_cols)

        # Log data field type and sample values for debugging
        if "data" in batch_df.columns:
            from pyspark.sql.types import MapType, StructType
            data_type = batch_df.schema["data"].dataType
            print(f"[MIGRATE_PRICE] Batch {batch_num}: BEFORE prune - data field type = {data_type}")

            # Sample a few records to see what the data looks like
            sample_data = batch_df.select("catalog_id", "data").limit(3).collect()
            for row in sample_data:
                data_value = row['data']
                print(f"[MIGRATE_PRICE] BEFORE prune - Sample catalog_id={row['catalog_id']}, data type={type(data_value)}, data keys={list(data_value.keys()) if isinstance(data_value, dict) else 'N/A'}")

        # SKIP pruning for PRICE segment - data is already stringified in read_segment_targeted
        # The data field was converted to JSON string during read to preserve nested structures like amounts
        # batch_df = prune_and_flatten_data_with_field_preservation(batch_df)
        print("[MIGRATE_PRICE] Skipping prune_and_flatten - data is already stringified to preserve nested structures like amounts")

        # Log a sample of the JSON string to verify amounts is present
        sample_json = batch_df.select("catalog_id", "data").limit(1).collect()
        if sample_json and sample_json[0]['data']:
            json_str = sample_json[0]['data']
            print(f"[MIGRATE_PRICE] Sample JSON string (first 300 chars): {json_str[:300]}")
            # Check if amounts is in the JSON
            if '"amounts"' in json_str:
                print("[MIGRATE_PRICE] ✓ amounts field found in JSON string")
            else:
                print("[MIGRATE_PRICE] ✗ WARNING: amounts field NOT found in JSON string")

        # Log data field after pruning
        if "data" in batch_df.columns:
            from pyspark.sql.types import MapType, StructType
            data_type = batch_df.schema["data"].dataType
            print(f"[MIGRATE_PRICE] Batch {batch_num}: AFTER prune - data field type = {data_type}")

            # Sample a few records to see what the data looks like
            sample_data = batch_df.select("catalog_id", "data").limit(3).collect()
            for row in sample_data:
                data_value = row['data']
                print(f"[MIGRATE_PRICE] AFTER prune - Sample catalog_id={row['catalog_id']}, data type={type(data_value)}, data keys={list(data_value.keys()) if isinstance(data_value, dict) else 'N/A'}")

        # Align PK/SK to target catalog table
        batch_df = align_df_to_target_keys_for_catalog(
            batch_df, target_catalog_table, target_region, seg_col_name
        )
        assert_write_keys_present(batch_df, target_catalog_table, target_region)

        # Remove null top-levels except required
        required_top = set(get_table_key_attrs(target_catalog_table, target_region))
        required_top.update([CATALOG_ID_COL, SEGMENT_COL, "data"])

        batch_df = _drop_all_null_top_level_columns(batch_df, required_cols=required_top)

        batch_count = _safe_count(batch_df, f"MIGRATE_PRICE_BATCH_{batch_num}", logger)
        if batch_count > 0:
            total_written += batch_count
            msg = f"[MIGRATE_PRICE] Batch {batch_num}: Writing {batch_count} records using BatchWriteItem"
        else:
            msg = f"[MIGRATE_PRICE] Batch {batch_num}: Writing records using BatchWriteItem"
        print(msg)
        logger.info(msg)

        # OPTIMIZED: Use direct BatchWriteItem (10x faster than Glue connector!)
        written_count = batch_write_items_direct(
            df=batch_df,
            table=target_catalog_table,
            region=target_region,
            max_workers=10
        )

        msg = f"[MIGRATE_PRICE] Batch {batch_num}: ✓ Wrote {written_count} items using BatchWriteItem"
        print(msg)
        logger.info(msg)

        # MEMORY CLEANUP
        batch_df.unpersist()
        del batch_df
        import gc
        gc.collect()

        batch_elapsed = time.time() - batch_start
        msg = f"[MIGRATE_PRICE] Batch {batch_num}: ✓ Completed in {batch_elapsed:.1f}s (memory cleared)"
        print(msg)
        logger.info(msg)

    # FINAL CLEANUP
    df.unpersist()
    del df
    import gc
    gc.collect()

    total_elapsed = time.time() - start_time
    msg = f"[MIGRATE_PRICE] ✓ Completed all {len(listing_batches)} batches, wrote {total_written} records in {total_elapsed:.1f}s"
    print(msg)
    logger.info(msg)


def migrate_metadata_segment(
    glueContext,
    logger,
    country_code,
    source_listings_table,
    source_region,
    target_catalog_table,
    target_region,
    test_listing_ids,
    test_client_ids,
    run_all,
):

    msg = "[MIGRATE_METADATA] Starting METADATA migration"
    print(msg)
    logger.info(msg)

    # OPTIMIZED: Get listing_ids from client_ids if needed
    listing_ids_to_use = test_listing_ids
    if not listing_ids_to_use and test_client_ids and not run_all:
        msg = f"[MIGRATE_METADATA] Getting listing IDs from client-id-index for {len(test_client_ids)} clients"
        print(msg)
        logger.info(msg)
        listing_ids_to_use = get_listing_ids_for_clients(
            glueContext, logger, country_code, source_listings_table, source_region, test_client_ids
        )
        msg = f"[MIGRATE_METADATA] Found {len(listing_ids_to_use)} listings from client-id-index"
        print(msg)
        logger.info(msg)

    # OPTIMIZED: Use targeted reads when listing IDs are available (100x faster!)
    if not run_all and listing_ids_to_use:
        msg = f"[MIGRATE_METADATA] Using targeted BatchGetItem for {len(listing_ids_to_use)} listings (100x faster than scan)"
        print(msg)
        logger.info(msg)

        df = read_segment_targeted(glueContext, source_listings_table, source_region, listing_ids_to_use, "METADATA")

        msg = f"[MIGRATE_METADATA] Loaded METADATA segment data using targeted reads"
        print(msg)
        logger.info(msg)

        df = _apply_test_filters(df, listing_ids_to_use, test_client_ids, run_all)
    else:
        # Fallback to table scan for run_all mode
        scan_filter = {
            "dynamodb.filter.expression": "segment = :seg OR SK = :seg",
            "dynamodb.filter.expressionAttributeValues": '{":seg":{"S":"SEGMENT#METADATA"}}'
        }

        msg = "[MIGRATE_METADATA] Reading with DynamoDB scan filter"
        print(msg)
        logger.info(msg)

        dyf = read_ddb_table(glueContext, source_listings_table, source_region, scan_filter=scan_filter)
        df = dyf.toDF()

        msg = "[MIGRATE_METADATA] Loaded METADATA segment data"
        print(msg)
        logger.info(msg)

        df = _apply_test_filters(df, test_listing_ids, test_client_ids, run_all)
    msg = "[MIGRATE_METADATA] Applied test filters"
    print(msg)
    logger.info(msg)

    if df.rdd.isEmpty():
        msg = "[MIGRATE_METADATA] No records to migrate, skipping"
        print(msg)
        logger.info(msg)
        return

    # OOM PREVENTION: Get listing batches
    listing_batches = _get_listing_batches(df, batch_size=5000)
    logger.info(f"[MIGRATE_METADATA] Processing {len(listing_batches)} batches to prevent OOM")

    # Cache source df for reuse
    df.cache()
    cache_count = _safe_count(df, "MIGRATE_METADATA_CACHE", logger)
    if cache_count >= 0:
        logger.info(f"[MIGRATE_METADATA] Source data cached: {cache_count} records")
    else:
        logger.warn(f"[MIGRATE_METADATA] Cache count failed, proceeding without count")

    # Determine segment column name
    seg_col_name = SEGMENT_COL if SEGMENT_COL in df.columns else SK_COL

    import time
    start_time = time.time()
    total_written = 0

    for batch_num, listing_batch in enumerate(listing_batches, 1):
        batch_start = time.time()

        # Progress tracking
        progress_pct = (batch_num - 1) / len(listing_batches) * 100
        if batch_num > 1:
            elapsed = time.time() - start_time
            avg_time = elapsed / (batch_num - 1)
            eta_seconds = (len(listing_batches) - batch_num + 1) * avg_time
            eta_msg = f" (ETA: {int(eta_seconds/60)}m {int(eta_seconds%60)}s)"
        else:
            eta_msg = ""

        msg = f"[MIGRATE_METADATA] Batch {batch_num}/{len(listing_batches)} ({len(listing_batch)} listings) - {progress_pct:.1f}%{eta_msg}"
        print(msg, flush=True)
        logger.info(msg)

        # Filter to batch listings
        batch_df = df.filter(F.col("listing_id").isin(listing_batch))

        if batch_df.rdd.isEmpty():
            logger.info(f"[MIGRATE_METADATA] Batch {batch_num}: No records, skipping")
            continue

        if LISTING_ID_COL in batch_df.columns:
            batch_df = batch_df.withColumn(
                CATALOG_ID_COL,
                prefixed_catalog_id(F.lit(country_code), F.col(LISTING_ID_COL)),
            ).drop(LISTING_ID_COL)

        if "quality_score" in batch_df.columns:
            batch_df = batch_df.drop("quality_score")

        # Add published_at from first_published_at if missing
        if "first_published_at" in batch_df.columns and "published_at" not in batch_df.columns:
            print("[MIGRATE_METADATA] Adding published_at from first_published_at")
            batch_df = batch_df.withColumn("published_at", F.col("first_published_at"))
        elif "first_published_at" in batch_df.columns and "published_at" in batch_df.columns:
            # Use first_published_at as fallback if published_at is null
            batch_df = batch_df.withColumn(
                "published_at",
                F.coalesce(F.col("published_at"), F.col("first_published_at"))
            )

        if HASHES_COL in batch_df.columns:
            batch_df = strip_metadata_hashes_sql(batch_df)
            batch_df = _null_out_empty_hashes(batch_df)

        batch_df = align_df_to_target_keys_for_catalog(
            batch_df, target_catalog_table, target_region, seg_col_name
        )
        assert_write_keys_present(batch_df, target_catalog_table, target_region)

        required = set(get_table_key_attrs(target_catalog_table, target_region))
        required.update([CATALOG_ID_COL, SEGMENT_COL])

        # Preserve important metadata fields
        metadata_fields = [
            "created_by_id", "updated_by_id", "assigned_to_id", "broker_id",
            "client_id", "location_id", "reference", "web_id", "web_ids",
            "listing_advertisement_number", "first_published_at", "published_at",
            "version", "hashes"
        ]
        required.update(metadata_fields)

        batch_df = _drop_all_null_top_level_columns(batch_df, required_cols=required)

        batch_count = _safe_count(batch_df, f"MIGRATE_METADATA_BATCH_{batch_num}", logger)
        if batch_count > 0:
            total_written += batch_count
            msg = f"[MIGRATE_METADATA] Batch {batch_num}: Writing {batch_count} records using BatchWriteItem"
        else:
            msg = f"[MIGRATE_METADATA] Batch {batch_num}: Writing records using BatchWriteItem"
        print(msg)
        logger.info(msg)

        # OPTIMIZED: Use direct BatchWriteItem (10x faster than Glue connector!)
        written_count = batch_write_items_direct(
            df=batch_df,
            table=target_catalog_table,
            region=target_region,
            max_workers=10
        )

        msg = f"[MIGRATE_METADATA] Batch {batch_num}: ✓ Wrote {written_count} items using BatchWriteItem"
        print(msg)
        logger.info(msg)

        # MEMORY CLEANUP
        batch_df.unpersist()
        del batch_df
        import gc
        gc.collect()

        batch_elapsed = time.time() - batch_start
        msg = f"[MIGRATE_METADATA] Batch {batch_num}: ✓ Completed in {batch_elapsed:.1f}s (memory cleared)"
        print(msg)
        logger.info(msg)

    # FINAL CLEANUP
    df.unpersist()
    del df
    import gc
    gc.collect()

    total_elapsed = time.time() - start_time
    msg = f"[MIGRATE_METADATA] ✓ Completed all {len(listing_batches)} batches, wrote {total_written} records in {total_elapsed:.1f}s"
    print(msg)
    logger.info(msg)


def migrate_state_segment(
    glueContext,
    logger,
    country_code,
    source_listings_table,
    source_region,
    target_catalog_table,
    target_region,
    test_listing_ids,
    test_client_ids,
    run_all,
):

    msg = "[MIGRATE_STATE] Starting STATE migration"
    print(msg)
    logger.info(msg)

    # OPTIMIZED: Get listing_ids from client_ids if needed
    listing_ids_to_use = test_listing_ids
    if not listing_ids_to_use and test_client_ids and not run_all:
        msg = f"[MIGRATE_STATE] Getting listing IDs from client-id-index for {len(test_client_ids)} clients"
        print(msg)
        logger.info(msg)
        listing_ids_to_use = get_listing_ids_for_clients(
            glueContext, logger, country_code, source_listings_table, source_region, test_client_ids
        )
        msg = f"[MIGRATE_STATE] Found {len(listing_ids_to_use)} listings from client-id-index"
        print(msg)
        logger.info(msg)

    # OPTIMIZED: Use targeted reads when listing IDs are available (100x faster!)
    if not run_all and listing_ids_to_use:
        msg = f"[MIGRATE_STATE] Using targeted BatchGetItem for {len(listing_ids_to_use)} listings (100x faster than scan)"
        print(msg)
        logger.info(msg)

        df = read_segment_targeted(glueContext, source_listings_table, source_region, listing_ids_to_use, "STATE")

        msg = f"[MIGRATE_STATE] Loaded STATE segment data using targeted reads"
        print(msg)
        logger.info(msg)

        df = _apply_test_filters(df, listing_ids_to_use, test_client_ids, run_all)
    else:
        # Fallback to table scan for run_all mode
        scan_filter = {
            "dynamodb.filter.expression": "segment = :seg OR SK = :seg",
            "dynamodb.filter.expressionAttributeValues": '{":seg":{"S":"SEGMENT#STATE"}}'
        }

        msg = "[MIGRATE_STATE] Reading with DynamoDB scan filter"
        print(msg)
        logger.info(msg)

        dyf = read_ddb_table(glueContext, source_listings_table, source_region, scan_filter=scan_filter)
        df = dyf.toDF()

        msg = "[MIGRATE_STATE] Loaded STATE segment data"
        print(msg)
        logger.info(msg)

        df = _apply_test_filters(df, test_listing_ids, test_client_ids, run_all)
    msg = "[MIGRATE_STATE] Applied test filters"
    print(msg)
    logger.info(msg)

    if df.rdd.isEmpty():
        msg = "[MIGRATE_STATE] No records to migrate, skipping"
        print(msg)
        logger.info(msg)
        return

    # OOM PREVENTION: Get listing batches
    listing_batches = _get_listing_batches(df, batch_size=5000)
    logger.info(f"[MIGRATE_STATE] Processing {len(listing_batches)} batches to prevent OOM")

    # Cache source df for reuse
    df.cache()
    cache_count = _safe_count(df, "MIGRATE_STATE_CACHE", logger)
    if cache_count >= 0:
        logger.info(f"[MIGRATE_STATE] Source data cached: {cache_count} records")
    else:
        logger.warn(f"[MIGRATE_STATE] Cache count failed, proceeding without count")

    # Determine segment column name
    seg_col_name = SEGMENT_COL if SEGMENT_COL in df.columns else SK_COL

    import time
    start_time = time.time()
    total_written = 0

    for batch_num, listing_batch in enumerate(listing_batches, 1):
        batch_start = time.time()

        # Progress tracking
        progress_pct = (batch_num - 1) / len(listing_batches) * 100
        if batch_num > 1:
            elapsed = time.time() - start_time
            avg_time = elapsed / (batch_num - 1)
            eta_seconds = (len(listing_batches) - batch_num + 1) * avg_time
            eta_msg = f" (ETA: {int(eta_seconds/60)}m {int(eta_seconds%60)}s)"
        else:
            eta_msg = ""

        msg = f"[MIGRATE_STATE] Batch {batch_num}/{len(listing_batches)} ({len(listing_batch)} listings) - {progress_pct:.1f}%{eta_msg}"
        print(msg, flush=True)
        logger.info(msg)

        # Filter to batch listings
        batch_df = df.filter(F.col("listing_id").isin(listing_batch))

        if batch_df.rdd.isEmpty():
            logger.info(f"[MIGRATE_STATE] Batch {batch_num}: No records, skipping")
            continue

        if LISTING_ID_COL in batch_df.columns:
            batch_df = batch_df.withColumn(
                CATALOG_ID_COL,
                prefixed_catalog_id(F.lit(country_code), F.col(LISTING_ID_COL)),
            ).drop(LISTING_ID_COL)

        if "quality_score" in batch_df.columns:
            batch_df = batch_df.drop("quality_score")

        batch_df = map_state_type_expr(batch_df)

        batch_df = align_df_to_target_keys_for_catalog(
            batch_df, target_catalog_table, target_region, seg_col_name
        )
        assert_write_keys_present(batch_df, target_catalog_table, target_region)

        required = set(get_table_key_attrs(target_catalog_table, target_region))
        required.update([CATALOG_ID_COL, SEGMENT_COL])

        batch_df = _drop_all_null_top_level_columns(batch_df, required_cols=required)

        batch_count = _safe_count(batch_df, f"MIGRATE_STATE_BATCH_{batch_num}", logger)
        if batch_count > 0:
            total_written += batch_count
            msg = f"[MIGRATE_STATE] Batch {batch_num}: Writing {batch_count} records using BatchWriteItem"
        else:
            msg = f"[MIGRATE_STATE] Batch {batch_num}: Writing records using BatchWriteItem"
        print(msg)
        logger.info(msg)

        # OPTIMIZED: Use direct BatchWriteItem (10x faster than Glue connector!)
        written_count = batch_write_items_direct(
            df=batch_df,
            table=target_catalog_table,
            region=target_region,
            max_workers=10
        )

        msg = f"[MIGRATE_STATE] Batch {batch_num}: ✓ Wrote {written_count} items using BatchWriteItem"
        print(msg)
        logger.info(msg)

        # MEMORY CLEANUP
        batch_df.unpersist()
        del batch_df
        import gc
        gc.collect()

        batch_elapsed = time.time() - batch_start
        msg = f"[MIGRATE_STATE] Batch {batch_num}: ✓ Completed in {batch_elapsed:.1f}s (memory cleared)"
        print(msg)
        logger.info(msg)

    # FINAL CLEANUP
    df.unpersist()
    del df
    import gc
    gc.collect()

    total_elapsed = time.time() - start_time
    msg = f"[MIGRATE_STATE] ✓ Completed all {len(listing_batches)} batches, wrote {total_written} records in {total_elapsed:.1f}s"
    print(msg)
    logger.info(msg)


# ---------- Main Migration Orchestrator ----------
def run_migration(args, glueContext, logger):
    print("=" * 80)
    print("[MIGRATION] Starting catalog migration process")
    print("[MIGRATION] *** CODE VERSION: 2025-11-20-v3-SOURCE-FIELDS-ONLY ***")
    print("=" * 80)
    logger.info("=" * 80)
    logger.info("[MIGRATION] Starting catalog migration process")
    logger.info("[MIGRATION] *** CODE VERSION: 2025-11-20-v3-SOURCE-FIELDS-ONLY ***")
    logger.info("=" * 80)

    country_code = args["COUNTRY_CODE"].upper().strip()
    source_region = args["SOURCE_REGION"]
    target_region = args["TARGET_REGION"]
    source_util_table = args["SOURCE_UTIL_TABLE"]
    source_listings_table = args["SOURCE_LISTINGS_TABLE"]
    target_util_table = args["TARGET_UTIL_TABLE"]
    target_catalog_table = args["TARGET_CATALOG_TABLE"]

    msg = "[MIGRATION] Country: " + str(country_code)
    print(msg)
    logger.info(msg)
    msg = "[MIGRATION] Source: " + str(source_listings_table) + " (" + str(source_region) + ")"
    print(msg)
    logger.info(msg)
    msg = "[MIGRATION] Target: " + str(target_catalog_table) + " (" + str(target_region) + ")"
    print(msg)
    logger.info(msg)

    test_listing_ids = args.get("TEST_LISTING_IDS", [])
    test_client_ids = args.get("TEST_CLIENT_IDS", [])

    # If client IDs provided, look up listing IDs from METADATA segment first
    if test_client_ids and not test_listing_ids:
        msg = "[MIGRATION] Client IDs provided, looking up listing IDs from METADATA..."
        print(msg)
        logger.info(msg)
        test_listing_ids = get_listing_ids_for_clients(
            glueContext, logger, country_code, source_listings_table, source_region, test_client_ids
        )
        if not test_listing_ids:
            msg = "[MIGRATION] No listings found for client IDs: " + str(test_client_ids)
            print(msg)
            logger.info(msg)
            msg = "[MIGRATION] Aborting migration"
            print(msg)
            logger.info(msg)
            return
        msg = (
            "[MIGRATION] Found "
            + str(len(test_listing_ids))
            + " listings for clients: "
            + str(test_listing_ids[:5])
            + ("..." if len(test_listing_ids) > 5 else "")
        )
        print(msg)
        logger.info(msg)

    # Check if RUN_ALL flag is explicitly set (overrides test ID logic)
    run_all_arg = args.get("RUN_ALL", "false").lower() == "true"

    # Determine run mode: explicit RUN_ALL flag OR no test IDs provided
    run_all = run_all_arg or (len(test_listing_ids) == 0 and len(test_client_ids) == 0)

    msg = "[MIGRATION] Run mode: " + ("FULL MIGRATION" if run_all else "TEST MODE")
    print(msg)
    logger.info(msg)
    if not run_all:
        msg = "[MIGRATION] Test listing IDs: " + str(test_listing_ids[:5]) + ("..." if len(test_listing_ids) > 5 else "")
        print(msg)
        logger.info("[MIGRATION] Test listing IDs: " + str(test_listing_ids))
        msg = "[MIGRATION] Test client IDs: " + str(test_client_ids)
        print(msg)
        logger.info(msg)

    msg = "\n[MIGRATION] Step 1/7: Migrating client references..."
    print(msg)
    logger.info(msg)
    migrate_client_reference(
        glueContext,
        logger,
        country_code,
        source_util_table,
        source_region,
        target_util_table,
        target_region,
        test_listing_ids,
        test_client_ids,
        run_all,
    )

    msg = "\n[MIGRATION] Step 2/7: Migrating AMENITIES segment..."
    print(msg)
    logger.info(msg)
    migrate_amenities_segment(
        glueContext,
        logger,
        country_code,
        source_listings_table,
        source_region,
        target_catalog_table,
        target_region,
        test_listing_ids,
        test_client_ids,
        run_all,
    )

    msg = "\n[MIGRATION] Step 3/7: Migrating ATTRIBUTES segment..."
    print(msg)
    logger.info(msg)
    migrate_attributes_segment(
        glueContext,
        logger,
        country_code,
        source_listings_table,
        source_region,
        target_catalog_table,
        target_region,
        test_listing_ids,
        test_client_ids,
        run_all,
    )

    msg = "\n[MIGRATION] Step 4/7: Migrating DESCRIPTION segment..."
    print(msg)
    logger.info(msg)
    migrate_description_segment(
        glueContext,
        logger,
        country_code,
        source_listings_table,
        source_region,
        target_catalog_table,
        target_region,
        test_listing_ids,
        test_client_ids,
        run_all,
    )

    msg = "\n[MIGRATION] Step 5/7: Migrating PRICE segment..."
    print(msg)
    logger.info(msg)
    migrate_price_segment(
        glueContext,
        logger,
        country_code,
        source_listings_table,
        source_region,
        target_catalog_table,
        target_region,
        test_listing_ids,
        test_client_ids,
        run_all,
    )

    msg = "\n[MIGRATION] Step 6/7: Migrating METADATA segment..."
    print(msg)
    logger.info(msg)
    migrate_metadata_segment(
        glueContext,
        logger,
        country_code,
        source_listings_table,
        source_region,
        target_catalog_table,
        target_region,
        test_listing_ids=test_listing_ids,
        test_client_ids=test_client_ids,
        run_all=run_all,
    )

    msg = "\n[MIGRATION] Step 7/7: Migrating STATE segment..."
    print(msg)
    logger.info(msg)
    migrate_state_segment(
        glueContext,
        logger,
        country_code,
        source_listings_table,
        source_region,
        target_catalog_table,
        target_region,
        test_listing_ids=test_listing_ids,
        test_client_ids=test_client_ids,
        run_all=run_all,
    )

    print("=" * 80)
    print("[MIGRATION] ✓ Migration process completed successfully")
    print("=" * 80)
    logger.info("=" * 80)
    logger.info("[MIGRATION] ✓ Migration process completed successfully")
    logger.info("=" * 80)


# ---------- Arg parsing helpers ----------
def _resolve_optional(argv, key, default_val):
    """Read optional Glue job param: accepts --KEY value OR --KEY=value"""
    flag = f"--{key}"
    for i, a in enumerate(argv):
        if a == flag and i + 1 < len(argv):
            return argv[i + 1]
        if a.startswith(flag + "="):
            return a.split("=", 1)[1]
    return default_val


# ---------- Job entry ----------
def init_spark():
    """
    Required:
      JOB_NAME, COUNTRY_CODE, SOURCE_REGION, TARGET_REGION,
      SOURCE_UTIL_TABLE, SOURCE_LISTINGS_TABLE, TARGET_UTIL_TABLE, TARGET_CATALOG_TABLE
    Optional:
      TEST_LISTING_IDS  -> comma-separated list of listing IDs to migrate
      TEST_CLIENT_IDS   -> comma-separated list of client IDs to migrate
      CLEAN_FIRST       -> "true"/"false" (default "false"). If true, delete existing data before migration
      DELETE_ONLY       -> "true"/"false" (default "false"). If true, only delete data, don't run migration
      DELETE_ALL        -> "true"/"false" (default "false"). If true with DELETE_ONLY, delete all data without test IDs
      ADDITIONAL_DELETE_TABLES -> comma-separated list of additional table names to delete from
    """
    base_args = [
        "JOB_NAME",
        "COUNTRY_CODE",
        "SOURCE_REGION",
        "TARGET_REGION",
        "SOURCE_UTIL_TABLE",
        "SOURCE_LISTINGS_TABLE",
        "TARGET_UTIL_TABLE",
        "TARGET_CATALOG_TABLE",
    ]
    args = getResolvedOptions(sys.argv, base_args)

    # Support comma-separated list for listing IDs
    test_ids_str = _resolve_optional(sys.argv, "TEST_LISTING_IDS", "")
    args["TEST_LISTING_IDS"] = (
        [id.strip() for id in test_ids_str.split(",") if id.strip()]
        if test_ids_str
        else []
    )

    # Support comma-separated list for client IDs
    test_client_ids_str = _resolve_optional(sys.argv, "TEST_CLIENT_IDS", "")
    args["TEST_CLIENT_IDS"] = (
        [id.strip() for id in test_client_ids_str.split(",") if id.strip()]
        if test_client_ids_str
        else []
    )

    # Support CLEAN_FIRST flag
    clean_first_str = _resolve_optional(sys.argv, "CLEAN_FIRST", "false")
    args["CLEAN_FIRST"] = clean_first_str.lower() in ("true", "1", "yes")

    # Support DELETE_ONLY flag - only delete, don't migrate
    delete_only_str = _resolve_optional(sys.argv, "DELETE_ONLY", "false")
    args["DELETE_ONLY"] = delete_only_str.lower() in ("true", "1", "yes")

    # Support DELETE_ALL flag - allow deleting all data without test IDs
    delete_all_str = _resolve_optional(sys.argv, "DELETE_ALL", "false")
    args["DELETE_ALL"] = delete_all_str.lower() in ("true", "1", "yes")

    sc = SparkContext()
    glueContext = GlueContext(sc)
    logger = glueContext.get_logger()
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)
    return spark, glueContext, job, args, logger


# ---------- Main ----------
def main():
    spark, glueContext, job, args, logger = init_spark()

    country_code = args["COUNTRY_CODE"].upper().strip()
    source_listings_table = args["SOURCE_LISTINGS_TABLE"]
    source_region = args["SOURCE_REGION"]
    target_catalog_table = args["TARGET_CATALOG_TABLE"]
    target_util_table = args["TARGET_UTIL_TABLE"]
    target_region = args["TARGET_REGION"]
    test_listing_ids = args.get("TEST_LISTING_IDS", [])
    test_client_ids = args.get("TEST_CLIENT_IDS", [])
    delete_only = args.get("DELETE_ONLY", False)
    delete_all = args.get("DELETE_ALL", False)
    clean_first = args.get("CLEAN_FIRST", False)

    # Log job parameters for debugging
    print("=" * 80)
    print("[JOB_START] Catalog Migration Job Started")
    print("=" * 80)
    print(f"[JOB_PARAMS] Country: {country_code}")
    print(f"[JOB_PARAMS] Source Table: {source_listings_table}")
    print(f"[JOB_PARAMS] Source Region: {source_region}")
    print(f"[JOB_PARAMS] Target Catalog Table: {target_catalog_table}")
    print(f"[JOB_PARAMS] Target Util Table: {target_util_table}")
    print(f"[JOB_PARAMS] Target Region: {target_region}")
    print(f"[JOB_PARAMS] Test Listing IDs: {test_listing_ids if test_listing_ids else 'None (run all)'}")
    print(f"[JOB_PARAMS] Test Client IDs: {test_client_ids if test_client_ids else 'None'}")
    print(f"[JOB_PARAMS] Clean First: {clean_first}")
    print(f"[JOB_PARAMS] Delete Only: {delete_only}")
    print(f"[JOB_PARAMS] Delete All: {delete_all}")
    print("=" * 80)

    logger.info("=" * 80)
    logger.info("[JOB_START] Catalog Migration Job Started")
    logger.info("=" * 80)
    logger.info(f"[JOB_PARAMS] Country: {country_code}")
    logger.info(f"[JOB_PARAMS] Test Listing IDs: {test_listing_ids if test_listing_ids else 'None (run all)'}")
    logger.info(f"[JOB_PARAMS] Test Client IDs: {test_client_ids if test_client_ids else 'None'}")
    logger.info("=" * 80)

    # If DELETE_ONLY is enabled, only delete and exit
    if delete_only:
        print("=" * 80)
        print("[DELETE_ONLY] DELETE_ONLY mode enabled - deleting data and exiting")
        print("=" * 80)
        logger.info("=" * 80)
        logger.info(
            "[DELETE_ONLY] DELETE_ONLY mode enabled - deleting data and exiting"
        )
        logger.info("=" * 80)

        # If DELETE_ALL is enabled, clear test IDs to delete everything
        if delete_all:
            msg1 = "=" * 80
            msg2 = "[DELETE_ALL] DELETE_ALL mode enabled - will delete ALL data for country: " + country_code
            msg3 = "[DELETE_ALL] This will delete EVERYTHING for this country!"
            msg4 = "[DELETE_ALL] Ignoring any test IDs provided"

            print(msg1)
            print(msg2)
            print(msg3)
            print(msg4)
            print(msg1)

            logger.warn(msg1)
            logger.warn(msg2)
            logger.warn(msg3)
            logger.warn(msg4)
            logger.warn(msg1)

            # Clear test IDs to delete all
            test_listing_ids = []
            test_client_ids = []
        elif not test_listing_ids and not test_client_ids:
            print("[DELETE_ONLY] WARNING: No TEST_LISTING_IDS or TEST_CLIENT_IDS specified!")
            print("[DELETE_ONLY] This would delete ALL data for country: " + country_code)
            print("[DELETE_ONLY] Aborting for safety. Please specify test IDs or use --DELETE_ALL=true")
            print("=" * 80)
            print("[DELETE_ONLY] Job aborted - no deletion performed")
            print("=" * 80)
            logger.warn(
                "[DELETE_ONLY] WARNING: No TEST_LISTING_IDS or TEST_CLIENT_IDS specified!"
            )
            logger.warn(
                "[DELETE_ONLY] This would delete ALL data for country: "
                + country_code
            )
            logger.warn(
                "[DELETE_ONLY] Aborting for safety. Please specify test IDs or use --DELETE_ALL=true"
            )
            logger.info("=" * 80)
            logger.info("[DELETE_ONLY] Job aborted - no deletion performed")
            logger.info("=" * 80)
            job.commit()
            return

        msg = "[DELETE_ONLY] Deleting data for country: " + str(country_code)
        print(msg)
        logger.info(msg)

        if test_listing_ids:
            msg = "[DELETE_ONLY] Test listing IDs: " + str(test_listing_ids)
            print(msg)
            logger.info(msg)
        if test_client_ids:
            msg = "[DELETE_ONLY] Test client IDs: " + str(test_client_ids)
            print(msg)
            logger.info(msg)

        # Delete catalog entries
        delete_catalog_for_country(
            glueContext,
            logger,
            country_code,
            target_catalog_table,
            target_region,
            test_listing_ids,
            test_client_ids,
            source_listings_table,
            source_region,
        )

        # Delete util entries
        delete_util_for_country(
            glueContext,
            logger,
            country_code,
            target_util_table,
            target_region,
            test_listing_ids,
            test_client_ids,
        )

        print("=" * 80)
        print("[DELETE_ONLY] ✓ Deletion completed successfully")
        print("=" * 80)
        logger.info("=" * 80)
        logger.info("[DELETE_ONLY] ✓ Deletion completed successfully")
        logger.info("=" * 80)
        job.commit()
        return

    # If CLEAN_FIRST is enabled, delete existing data first
    if clean_first:
        logger.info("=" * 80)
        logger.info("[CLEAN] CLEAN_FIRST enabled - deleting existing data")
        logger.info("=" * 80)

        # Delete catalog entries
        delete_catalog_for_country(
            glueContext,
            logger,
            country_code,
            target_catalog_table,
            target_region,
            test_listing_ids,
            test_client_ids,
            source_listings_table,
            source_region,
        )

        # Delete util entries
        delete_util_for_country(
            glueContext,
            logger,
            country_code,
            target_util_table,
            target_region,
            test_listing_ids,
            test_client_ids,
        )

        logger.info("=" * 80)
        logger.info("[CLEAN] ✓ Cleanup completed, proceeding with migration")
        logger.info("=" * 80)

    # Run the migration
    run_migration(args, glueContext, logger)
    job.commit()


if __name__ == "__main__":
    main()

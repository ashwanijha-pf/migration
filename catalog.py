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


def get_listing_ids_for_clients(
    glueContext,
    logger,
    country_code,
    source_listings_table,
    source_region,
    client_ids,
):
    """Get listing IDs for given client IDs by querying the METADATA segment in source listings table."""
    if not client_ids:
        return []

    msg = "[LOOKUP] Looking up listing IDs for " + str(len(client_ids)) + " client IDs"
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
        logger.warn(msg)
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

            # Remove null array
            if "array" in obj and (obj["array"] is None or obj["array"] == []):
                obj.pop("array", None)

            # Clean struct
            if "struct" in obj and isinstance(obj["struct"], dict):
                cleaned = {k: v for k, v in obj["struct"].items() if v is not None}
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
    c = F.col(colname)
    s = c.cast("string")
    return F.coalesce(F.when(s.rlike(r"^\s*\{"), s), F.to_json(c))


def _extract_state_type_as_json_only():
    j = _json_of(STATE_DATA_COL)
    c1 = F.get_json_object(j, "$.type")
    c2 = F.get_json_object(j, "$.M.type.S")
    c3 = F.get_json_object(j, "$.M.type")
    c4 = F.get_json_object(j, "$.type.S")
    c5 = F.get_json_object(j, "$.S")
    return F.lower(F.coalesce(c1, c2, c3, c4, c5))


def _extract_state_reasons_as_array():
    """Extract reasons as JSON and parse to array<string>, or null if absent/not an array."""
    j = _json_of(STATE_DATA_COL)
    r1 = F.get_json_object(j, "$.reasons")
    r2 = F.get_json_object(j, "$.M.reasons")
    r_json = F.coalesce(r1, r2)
    return F.when(r_json.isNull(), F.lit(None).cast("array<string>")).otherwise(
        F.from_json(r_json, ArrayType(StringType()))
    )


def map_state_type_expr(df):
    """
    Rebuild data as struct<type:string, reasons:array<string>> and set top-level state_type.
    Uses JSON-based extraction first; if that yields null, falls back to existing top-level state_type.
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

    # uniform struct
    new_state_struct = F.struct(
        mapped.alias("type"),
        reasons_arr.alias("reasons"),
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


def read_ddb_table(glueContext, table, region, read_percent="1.0", splits="400", scan_filter=None):
    """
    Read DynamoDB table with optional scan filter to reduce data at source.
    scan_filter example: "segment = :seg" with expression_attribute_values
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

    # Use DynamoDB scan filter with begins_with to reduce data at source
    scan_filter = {
        "dynamodb.filter.expression": "begins_with(pk, :ref)",
        "dynamodb.filter.expressionAttributeValues": '{":ref":{"S":"REFERENCE#"}}'
    }

    msg = f"[MIGRATE_REFERENCE] Reading source table {source_util_table} with DynamoDB scan filter for REFERENCE# records"
    print(msg)
    logger.info(msg)

    # OPTIMIZE: Broadcast country code to avoid serialization overhead
    spark = glueContext.spark_session
    country_broadcast = spark.sparkContext.broadcast(country_code)

    # CRITICAL: For large datasets, process in batches from the start to prevent OOM
    if not run_all and test_listing_ids and len(test_listing_ids) > 5000:
        msg = "[MIGRATE_REFERENCE] Large dataset detected (" + str(len(test_listing_ids)) + " listings), processing in batches..."
        print(msg, flush=True)
        logger.info(msg)

        # OPTIMIZE: Auto-tune batch size based on dataset size (optimized for G.8X)
        # Keep batches under 10k to avoid broadcast join memory issues
        total_listings = len(test_listing_ids)
        if total_listings > 500000:
            batch_size = 5000   # Conservative for very large datasets
        elif total_listings > 100000:
            batch_size = 8000   # Medium batches
        elif total_listings > 50000:
            batch_size = 10000  # Larger batches for medium datasets
        else:
            batch_size = 10000  # Maximum batch size to avoid broadcast issues

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

        # OPTIMIZE: Intelligent caching with OOM protection
        msg = "[MIGRATE_REFERENCE] Loading source REFERENCE data with OOM-safe caching..."
        print(msg, flush=True)
        logger.info(msg)

        dyf = read_ddb_table(glueContext, source_util_table, source_region, scan_filter=scan_filter, splits="200")
        source_df = dyf.toDF()

        # OPTIMIZE: Apply column pruning early to reduce memory usage
        required_columns = [PK_COL, LISTING_ID_COL, "client_id"] + [col for col in source_df.columns if col not in [PK_COL, LISTING_ID_COL, "client_id"]]
        source_df = source_df.select(*[col for col in required_columns if col in source_df.columns])

        # SAFETY: Check source data size before caching (optimized for G.8X cluster)
        total_source_records = source_df.count()
        estimated_memory_mb = total_source_records * 0.5  # Rough estimate: 0.5KB per record

        # G.8X cluster specifications:
        # - 40 workers × 64GB = 2,560GB total cluster memory
        # - 32GB driver memory (configured)
        # - 200 partitions = ~128MB per partition if evenly distributed
        # Conservative caching limit: 10GB (can handle ~20M records)
        cache_limit_mb = 10000  # 10GB limit - can cache up to 20M records safely

        msg = f"[CLUSTER_INFO] G.8X cluster: 40 workers × 64GB, cache limit: {cache_limit_mb}MB"
        print(msg)
        logger.info(msg)

        if estimated_memory_mb < cache_limit_mb:
            from pyspark import StorageLevel
            source_df.persist(StorageLevel.MEMORY_AND_DISK_SER)
            msg = f"[MIGRATE_REFERENCE] Cached {total_source_records} source records (~{estimated_memory_mb:.0f}MB) for batch reuse"
            use_cached_source = True
        else:
            msg = f"[MIGRATE_REFERENCE] Source data too large ({total_source_records} records, ~{estimated_memory_mb:.0f}MB) - will read per batch to avoid OOM"
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

            # LOAD: Use cached data if available, otherwise read fresh
            if use_cached_source:
                batch_df = source_df  # Use cached data
                msg = "[MIGRATE_REFERENCE] Batch " + str(batch_num) + ": Using cached source data (" + str(batch_df.rdd.getNumPartitions()) + " partitions), filtering to batch listings..."
            else:
                # Read fresh data for each batch to avoid OOM
                dyf_batch = read_ddb_table(glueContext, source_util_table, source_region, scan_filter=scan_filter, splits="200")
                batch_df = dyf_batch.toDF()
                msg = "[MIGRATE_REFERENCE] Batch " + str(batch_num) + ": Reading fresh source data (" + str(batch_df.rdd.getNumPartitions()) + " partitions), filtering to batch listings..."

            print(msg)
            logger.info(msg)

            # FILTER: Apply remaining test filters (client_ids if needed)
            batch_df = _apply_test_filters(batch_df, listing_batch, test_client_ids, run_all)

            if batch_df.rdd.isEmpty():
                msg = "[MIGRATE_REFERENCE] Batch " + str(batch_num) + ": No records after filtering, skipping"
                print(msg)
                logger.info(msg)
                continue

            # OPTIMIZE: Cache filtered data since we'll access it multiple times during transformations
            batch_df.cache()
            record_count = batch_df.count()  # Trigger caching

            # DATA INTEGRITY: Track which listings are actually processed in this batch
            if LISTING_ID_COL in batch_df.columns:
                batch_processed_listings = set(batch_df.select(LISTING_ID_COL).rdd.map(lambda r: r[0]).collect())
                total_processed_listings.update(batch_processed_listings)

                # Verify we're processing the expected listings for this batch
                expected_listings = set(listing_batch)
                missing_listings = expected_listings - batch_processed_listings
                if missing_listings:
                    msg = f"[DATA_INTEGRITY] WARNING: Batch {batch_num} missing {len(missing_listings)} expected listings: {list(missing_listings)[:5]}..."
                    print(msg)
                    logger.warn(msg)

            msg = "[MIGRATE_REFERENCE] Batch " + str(batch_num) + ": Cached " + str(record_count) + " records for processing"
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

            # WRITE: Convert and write this batch immediately
            batch_out = to_dynamic_frame(glueContext, batch_df)
            batch_out = DropNullFields.apply(frame=batch_out)

            # OPTIMIZE: Write with higher throughput settings for batch processing
            write_to_ddb(
                glueContext,
                batch_out,
                target_util_table,
                target_region,
                segment_name="CLIENT_REFERENCE",
                write_batch_size="500",  # Larger batch writes
                write_throughput_percentage="90"  # Use more write capacity
            )

            # DATA INTEGRITY: Track total records written
            total_written_records += record_count

            # OPTIMIZE: Calculate batch performance metrics
            batch_end_time = time.time()
            batch_duration = batch_end_time - batch_start_time
            records_per_second = record_count / batch_duration if batch_duration > 0 else 0

            msg = f"[MIGRATE_REFERENCE] Batch {batch_num} completed in {batch_duration:.1f}s ({records_per_second:.0f} records/sec)"
            print(msg)
            logger.info(msg)

            # CLEANUP: Aggressive memory cleanup before next batch
            # Unpersist cached DataFrames
            batch_df.unpersist()

            # Delete batch references
            if use_cached_source:
                del batch_df, batch_out  # Keep source_df cached for next batch
            else:
                del batch_df, batch_out, dyf_batch  # Clean up fresh read data

            # Force garbage collection
            import gc
            gc.collect()

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

        msg = "[MIGRATE_REFERENCE] Converting to DynamicFrame..."
        print(msg, flush=True)
        logger.info(msg)

        out = to_dynamic_frame(glueContext, df)
        out = DropNullFields.apply(frame=out)

        msg = "[MIGRATE_REFERENCE] Writing records to " + target_util_table
        print(msg, flush=True)
        sys.stdout.flush()
        logger.info(msg)
        write_to_ddb(
            glueContext,
            out,
            target_util_table,
            target_region,
            segment_name="CLIENT_REFERENCE",
        )
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

    # Use DynamoDB scan filter to reduce data at source
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
    df.count()
    logger.info(f"[MIGRATE_AMENITIES] Source data cached for batch processing")

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
            batch_df = batch_df.withColumn("data", F.col("data.array"))
            batch_df = batch_df.withColumn(
                "data",
                F.when(
                    F.col("data").isNull() | (F.size(F.col("data")) == 0),
                    F.lit(None),
                ).otherwise(F.col("data")),
            )

        if "quality_score" in batch_df.columns:
            batch_df = batch_df.drop("quality_score")

        batch_df = align_df_to_target_keys_for_catalog(
            batch_df, target_catalog_table, target_region, seg_col_name
        )
        assert_write_keys_present(batch_df, target_catalog_table, target_region)

        required_keys = set(get_table_key_attrs(target_catalog_table, target_region))
        required_keys.update([CATALOG_ID_COL, SEGMENT_COL, "data"])

        batch_df = _drop_all_null_top_level_columns(batch_df, required_cols=required_keys)

        out = to_dynamic_frame(glueContext, batch_df)

        batch_count = batch_df.count()
        total_written += batch_count

        msg = f"[MIGRATE_AMENITIES] Batch {batch_num}: Writing {batch_count} records"
        print(msg)
        logger.info(msg)

        write_to_ddb(
            glueContext,
            out,
            target_catalog_table,
            target_region,
            segment_name="AMENITIES",
            write_percent="1.0",
            batch_size="100",
        )

        # MEMORY CLEANUP
        batch_df.unpersist()
        del batch_df, out
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

            cleaned = {}
            for k, v in obj.items():
                # Drop nulls
                if v is None:
                    continue

                # Drop empty objects/lists
                if isinstance(v, (dict, list)) and len(v) == 0:
                    continue

                # KEEP everything else including empty strings
                cleaned[k] = v

            return json.dumps(cleaned) if cleaned else None

        except:
            return js

    clean_udf = F.udf(clean, StringType())
    df = df.withColumn("data_clean", clean_udf("data_json"))

    # CRITICAL: Collect ALL unique fields from ALL rows (not just one sample)
    # This ensures fields like owner_name are not lost
    msg = "[PRUNE_DATA] Collecting all unique fields from data to build complete schema..."
    print(msg)

    all_json_samples = df.select("data_clean").filter(F.col("data_clean").isNotNull()).limit(1000).collect()

    all_field_types = {}
    for row in all_json_samples:
        if row[0]:
            try:
                obj = json.loads(row[0])
                for key, value in obj.items():
                    # Track the type of each field
                    if key not in all_field_types:
                        all_field_types[key] = type(value).__name__
            except:
                pass

    msg = f"[PRUNE_DATA] Found {len(all_field_types)} unique fields across samples: {list(all_field_types.keys())}"
    print(msg)

    if all_field_types:
        # Create a complete sample JSON with all fields
        complete_sample = {}
        for field, field_type in all_field_types.items():
            if field_type == 'int' or field_type == 'float':
                complete_sample[field] = 0
            elif field_type == 'str':
                complete_sample[field] = ""
            elif field_type == 'dict':
                complete_sample[field] = {"dummy": "value"}
            elif field_type == 'list':
                complete_sample[field] = []
            else:
                complete_sample[field] = None

        merged_json = json.dumps(complete_sample)
        inferred_schema = F.schema_of_json(F.lit(merged_json))
        df = df.withColumn("data", F.from_json("data_clean", inferred_schema))
    else:
        df = df.withColumn("data", F.lit(None))

    return df.drop("data_json", "data_clean")


def prune_and_flatten_data(df):
    """Flatten data.struct → data and remove ALL null or empty fields.
       Preserves nested objects like street, size, title as proper structs."""

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
        inferred_schema = F.schema_of_json(F.lit(sample_json[0]))
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

    # Use DynamoDB scan filter to reduce data at source (more efficient than Spark filter)
    scan_filter = {
        "dynamodb.filter.expression": "segment = :seg OR SK = :seg",
        "dynamodb.filter.expressionAttributeValues": '{":seg":{"S":"SEGMENT#ATTRIBUTES"}}'
    }

    # Read source with filter
    dyf = read_ddb_table(glueContext, source_listings_table, source_region, scan_filter=scan_filter)
    df = dyf.toDF()

    seg_col = _choose_segment_col(df)
    if not seg_col:
        raise ValueError("Missing segment column")

    logger.info(f"[MIGRATE_ATTRIBUTES] Loaded ATTRIBUTES segment data")

    # Test listing/client filters
    df = _apply_test_filters(df, test_listing_ids, test_client_ids, run_all)

    if df.rdd.isEmpty():
        logger.info("[MIGRATE_ATTRIBUTES] No records after filters, skipping")
        return

    # OOM PREVENTION: Get listing batches and unpersist source after getting IDs
    listing_batches = _get_listing_batches(df, batch_size=5000)
    logger.info(f"[MIGRATE_ATTRIBUTES] Processing {len(listing_batches)} batches to prevent OOM")

    # Cache source df for reuse across batches (more efficient than re-reading)
    df.cache()
    df.count()  # Trigger caching
    logger.info(f"[MIGRATE_ATTRIBUTES] Source data cached for batch processing")

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

        # Remove nulls inside `data`, leave nested fields untouched
        batch_df = prune_and_flatten_data(batch_df)

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

        out = to_dynamic_frame(glueContext, batch_df)
        out = DropNullFields.apply(frame=out)

        batch_count = batch_df.count()
        total_written += batch_count

        msg = f"[MIGRATE_ATTRIBUTES] Batch {batch_num}: Writing {batch_count} records"
        print(msg)
        logger.info(msg)

        write_to_ddb(
            glueContext, out, target_catalog_table, target_region,
            segment_name="ATTRIBUTES", write_percent="1.0", batch_size="100"
        )

        # MEMORY CLEANUP: Unpersist batch data and force garbage collection
        batch_df.unpersist()
        del batch_df, out
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

    # Use DynamoDB scan filter to reduce data at source
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
    df.count()
    logger.info(f"[MIGRATE_DESCRIPTION] Source data cached for batch processing")

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

        out = to_dynamic_frame(glueContext, batch_df)
        out = DropNullFields.apply(frame=out)

        batch_count = batch_df.count()
        total_written += batch_count

        msg = f"[MIGRATE_DESCRIPTION] Batch {batch_num}: Writing {batch_count} records"
        print(msg)
        logger.info(msg)

        write_to_ddb(
            glueContext,
            out,
            target_catalog_table,
            target_region,
            segment_name="DESCRIPTION",
            write_percent="1.0",
            batch_size="100",
        )

        # MEMORY CLEANUP
        batch_df.unpersist()
        del batch_df, out
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

    # Use DynamoDB scan filter to reduce data at source
    scan_filter = {
        "dynamodb.filter.expression": "segment = :seg OR SK = :seg",
        "dynamodb.filter.expressionAttributeValues": '{":seg":{"S":"SEGMENT#PRICE"}}'
    }

    msg = "[MIGRATE_PRICE] Reading with DynamoDB scan filter"
    print(msg)
    logger.info(msg)

    # Read source listings table with filter
    dyf = read_ddb_table(glueContext, source_listings_table, source_region, scan_filter=scan_filter)
    df = dyf.toDF()

    msg = "[MIGRATE_PRICE] Loaded PRICE segment data"
    print(msg)
    logger.info(msg)

    # Test filters for listing/client ID
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
    df.count()
    logger.info(f"[MIGRATE_PRICE] Source data cached for batch processing")

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

        # SELECT ONLY VALID PRICE FIELDS
        batch_df = batch_df.select(
            CATALOG_ID_COL,
            seg_col_name,
            "created_at",
            "data",
            "price_type",
            "type",
            "updated_at"
        )

        # Clean data map
        batch_df = prune_and_flatten_data(batch_df)

        # Align PK/SK to target catalog table
        batch_df = align_df_to_target_keys_for_catalog(
            batch_df, target_catalog_table, target_region, seg_col_name
        )
        assert_write_keys_present(batch_df, target_catalog_table, target_region)

        # Remove null top-levels except required
        required_top = set(get_table_key_attrs(target_catalog_table, target_region))
        required_top.update([CATALOG_ID_COL, SEGMENT_COL, "data"])
        batch_df = _drop_all_null_top_level_columns(batch_df, required_cols=required_top)

        # Convert to dynamic frame + final null strip
        out = to_dynamic_frame(glueContext, batch_df)
        out = DropNullFields.apply(out)

        batch_count = batch_df.count()
        total_written += batch_count

        msg = f"[MIGRATE_PRICE] Batch {batch_num}: Writing {batch_count} records"
        print(msg)
        logger.info(msg)

        write_to_ddb(
            glueContext, out,
            target_catalog_table, target_region,
            segment_name="PRICE",
            write_percent="1.0",
            batch_size="100"
        )

        # MEMORY CLEANUP
        batch_df.unpersist()
        del batch_df, out
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

    msg = "[MIGRATE_METADATA] Starting METADATA segment migration"
    print(msg)
    logger.info(msg)

    # Use DynamoDB scan filter to reduce data at source
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
    df.count()
    logger.info(f"[MIGRATE_METADATA] Source data cached for batch processing")

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

        if HASHES_COL in batch_df.columns:
            batch_df = strip_metadata_hashes_sql(batch_df)
            batch_df = _null_out_empty_hashes(batch_df)

        batch_df = align_df_to_target_keys_for_catalog(
            batch_df, target_catalog_table, target_region, seg_col_name
        )
        assert_write_keys_present(batch_df, target_catalog_table, target_region)

        required = set(get_table_key_attrs(target_catalog_table, target_region))
        required.update([CATALOG_ID_COL, SEGMENT_COL])

        batch_df = _drop_all_null_top_level_columns(batch_df, required_cols=required)

        out = to_dynamic_frame(glueContext, batch_df)
        out = DropNullFields.apply(frame=out)

        batch_count = batch_df.count()
        total_written += batch_count

        msg = f"[MIGRATE_METADATA] Batch {batch_num}: Writing {batch_count} records"
        print(msg)
        logger.info(msg)

        write_to_ddb(
            glueContext,
            out,
            target_catalog_table,
            target_region,
            segment_name="METADATA",
            write_percent="1.0",
            batch_size="100",
        )

        # MEMORY CLEANUP
        batch_df.unpersist()
        del batch_df, out
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

    msg = "[MIGRATE_STATE] Starting STATE segment migration"
    print(msg)
    logger.info(msg)

    # Use DynamoDB scan filter to reduce data at source
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
    df.count()
    logger.info(f"[MIGRATE_STATE] Source data cached for batch processing")

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

        out = to_dynamic_frame(glueContext, batch_df)
        out = DropNullFields.apply(frame=out)

        batch_count = batch_df.count()
        total_written += batch_count

        msg = f"[MIGRATE_STATE] Batch {batch_num}: Writing {batch_count} records"
        print(msg)
        logger.info(msg)

        write_to_ddb(
            glueContext,
            out,
            target_catalog_table,
            target_region,
            segment_name="STATE",
            write_percent="1.0",
            batch_size="100",
        )

        # MEMORY CLEANUP
        batch_df.unpersist()
        del batch_df, out
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

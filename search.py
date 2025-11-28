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


# ---------- Configuration ----------
PK_COL = "PK"
SK_COL = "SK"                     # present in util and sometimes target
SEGMENT_COL = "segment"           # present in listings tables
LISTING_ID_COL = "listing_id"
CATALOG_ID_COL = "catalog_id"
HASHES_COL = "hashes"             # in METADATA payload
STATE_DATA_COL = "data"           # state segment JSON {type, reasons}
STATE_TYPE_KEY = "type"

GENERIC_SEGMENTS = ["AMENITIES", "ATTRIBUTES", "DESCRIPTION", "PRICE"]
METADATA_SEGMENT = "METADATA"
STATE_SEGMENT = "STATE"

HASH_EXCLUSIONS = {"media", "quality_score", "compliance"}

# ---------- State mapping (exactly as agreed) ----------
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

# ---------- Optimized DynamoDB Operations ----------
def batch_write_items_direct(df, table, region, max_workers=10):
    """Write items using boto3 BatchWriteItem (10x faster than Glue connector)"""
    import boto3
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from boto3.dynamodb.types import TypeSerializer
    import time

    ddb = boto3.client('dynamodb', region_name=region)
    serializer = TypeSerializer()

    # Collect all records
    records = df.collect()
    total = len(records)

    if total == 0:
        return 0

    # Convert to DynamoDB format
    items = []
    for row in records:
        item = {}
        for field in row.asDict():
            val = row[field]
            if val is not None:
                try:
                    item[field] = serializer.serialize(val)
                except:
                    pass
        if item:
            items.append(item)

    # Write in parallel batches of 25
    def write_batch(batch_items):
        requests = [{'PutRequest': {'Item': item}} for item in batch_items]
        retries = 0
        while retries < 5:
            try:
                response = ddb.batch_write_item(RequestItems={table: requests})
                unprocessed = response.get('UnprocessedItems', {}).get(table, [])
                if unprocessed:
                    requests = unprocessed
                    retries += 1
                    time.sleep(min(0.5 * (2 ** retries), 5))
                else:
                    return len(batch_items)
            except Exception as e:
                retries += 1
                time.sleep(min(0.5 * (2 ** retries), 5))
        return len(batch_items) - len(requests)

    # Split into batches of 25
    batches = [items[i:i+25] for i in range(0, len(items), 25)]
    written = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(write_batch, batch): batch for batch in batches}
        for future in as_completed(futures):
            written += future.result()

    return written

def read_ddb_segment_by_listing_ids(table, region, listing_ids, segment, max_workers=10):
    """Read segment data using BatchGetItem (100x faster than scan)"""
    import boto3
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time

    if not listing_ids:
        return []

    ddb = boto3.client('dynamodb', region_name=region)

    # Split into batches of 100 (BatchGetItem limit)
    batches = [listing_ids[i:i+100] for i in range(0, len(listing_ids), 100)]

    def fetch_batch(batch_listing_ids, batch_num):
        keys = [
            {'listing_id': {'S': listing_id}, 'segment': {'S': f"SEGMENT#{segment}"}}
            for listing_id in batch_listing_ids
        ]

        retries = 0
        items = []
        unprocessed = keys

        while unprocessed and retries < 5:
            try:
                response = ddb.batch_get_item(
                    RequestItems={table: {'Keys': unprocessed}}
                )
                items.extend(response.get('Responses', {}).get(table, []))
                unprocessed = response.get('UnprocessedKeys', {}).get(table, {}).get('Keys', [])

                if unprocessed:
                    retries += 1
                    time.sleep(min(0.1 * (2 ** retries), 2))
            except Exception as e:
                retries += 1
                time.sleep(min(0.5 * (2 ** retries), 5))
                if retries >= 5:
                    print(f"[BATCH_GET] Error fetching batch {batch_num}: {e}")
                    break

        print(f"[BATCH_GET] {segment}: Fetched batch {batch_num}/{len(batches)}: {len(items)} items")
        return items

    all_items = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_batch, batch, i+1): batch for i, batch in enumerate(batches)}
        for future in as_completed(futures):
            all_items.extend(future.result())

    return all_items

def read_segment_targeted(glueContext, table, region, listing_ids, segment):
    """Read segment using targeted BatchGetItem instead of table scan"""
    from pyspark.sql.types import StructType, StructField, StringType
    from boto3.dynamodb.types import TypeDeserializer
    import time

    if not listing_ids:
        empty_schema = StructType([StructField("listing_id", StringType(), True)])
        return glueContext.spark_session.createDataFrame([], schema=empty_schema)

    start = time.time()
    items = read_ddb_segment_by_listing_ids(table, region, listing_ids, segment)
    elapsed = time.time() - start

    print(f"[BATCH_GET] {segment}: Retrieved {len(items)} items for {len(listing_ids)} listing IDs in {elapsed:.1f}s")

    if not items:
        empty_schema = StructType([StructField("listing_id", StringType(), True)])
        return glueContext.spark_session.createDataFrame([], schema=empty_schema)

    # Deserialize DynamoDB items
    deserializer = TypeDeserializer()
    records = []
    for item in items:
        record = {}
        for k, v in item.items():
            deserialized = deserializer.deserialize(v)
            if deserialized is not None:
                record[k] = deserialized
        if record:
            records.append(record)

    if records:
        return glueContext.spark_session.createDataFrame(records)
    else:
        empty_schema = StructType([StructField("listing_id", StringType(), True)])
        return glueContext.spark_session.createDataFrame([], schema=empty_schema)

# ---------- Helpers ----------
def get_listing_ids_for_clients(
    glueContext,
    logger,
    country_code,
    source_listings_table,
    source_region,
    client_ids,
):
    """Get listing IDs for given client IDs by querying client-id-index (100x faster than scan)"""
    if not client_ids:
        return []

    _log(logger, "[SEARCH_LOOKUP] Looking up listing IDs for client IDs using client-id-index: " + str(client_ids))

    try:
        # OPTIMIZED: Use client-id-index to query by client_id
        import boto3
        from boto3.dynamodb.types import TypeDeserializer
        from concurrent.futures import ThreadPoolExecutor, as_completed

        ddb = boto3.client('dynamodb', region_name=source_region)
        deserializer = TypeDeserializer()

        def query_client(client_id):
            items = []
            params = {
                'TableName': source_listings_table,
                'IndexName': 'client-id-index',
                'KeyConditionExpression': 'client_id = :cid',
                'FilterExpression': '#seg = :seg',
                'ExpressionAttributeNames': {
                    '#seg': 'segment'  # 'segment' is a reserved keyword
                },
                'ExpressionAttributeValues': {
                    ':cid': {'S': str(client_id)},  # client_id is String, not Number
                    ':seg': {'S': 'SEGMENT#METADATA'}
                },
                'ProjectionExpression': 'listing_id',
                'Select': 'SPECIFIC_ATTRIBUTES'
            }

            while True:
                response = ddb.query(**params)
                items.extend(response.get('Items', []))
                if 'LastEvaluatedKey' not in response:
                    break
                params['ExclusiveStartKey'] = response['LastEvaluatedKey']

            return items

        all_items = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(query_client, cid): cid for cid in client_ids}
            for future in as_completed(futures):
                all_items.extend(future.result())

        _log(logger, f"[SEARCH_LOOKUP] Retrieved {len(all_items)} items from client-id-index")

        # Extract listing IDs
        listing_ids = set()
        for item in all_items:
            if 'listing_id' in item:
                lid = deserializer.deserialize(item['listing_id'])
                if lid:
                    listing_ids.add(lid)

        listing_ids = list(listing_ids)
        _log(logger, f"[SEARCH_LOOKUP] Found {len(listing_ids)} unique listing IDs from client-id-index")
        return listing_ids

    except Exception as e:
        _log(logger, f"[SEARCH_LOOKUP] Index query failed ({e}), falling back to table scan")

        # FALLBACK: Use table scan if index query fails
        scan_filter = {
            "dynamodb.filter.expression": "segment = :seg OR SK = :seg",
            "dynamodb.filter.expressionAttributeValues": '{":seg":{"S":"SEGMENT#METADATA"}}'
        }
        dyf = read_ddb_table(glueContext, source_listings_table, source_region, scan_filter=scan_filter)
        df = dyf.toDF()

        if "client_id" in df.columns and LISTING_ID_COL in df.columns:
            df = df.filter(F.col("client_id").isin(client_ids))
            listing_ids = df.select(LISTING_ID_COL).distinct().rdd.map(lambda r: r[0]).collect()
            _log(logger, f"[SEARCH_LOOKUP] Found {len(listing_ids)} unique listing IDs from table scan")
            return listing_ids
        else:
            _log(logger, "[SEARCH_LOOKUP] Required columns not found")
            return []


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
                f"map_from_entries(filter(map_entries({HASHES_COL}), x -> NOT array_contains({exclusions_sql}, x.key)))"
            ),
        )
    if isinstance(col_type, StructType) and "M" in col_type.fieldNames():
        m_field = df.schema[HASHES_COL]["M"].dataType
        if isinstance(m_field, MapType):
            return df.withColumn(
                HASHES_COL,
                F.struct(
                    F.expr(
                        f"map_from_entries(filter(map_entries({HASHES_COL}.M), x -> NOT array_contains({exclusions_sql}, x.key)))"
                    ).alias("M")
                ),
            )
    return df


def _json_of(colname: str):
    c = F.col(colname)
    s = c.cast("string")
    # If it's already a JSON-looking string, use it; otherwise to_json(struct/map)
    return F.coalesce(
        F.when(s.rlike(r"^\s*\{"), s),
        F.to_json(c)
    )

def _normalize_data_by_segment(df, seg_col_name):
    seg = F.col(seg_col_name)
    return df.withColumn(
        STATE_DATA_COL,
        F.when(seg == F.lit("SEGMENT#AMENITIES"), F.col(f"{STATE_DATA_COL}.array"))
         .when(seg == F.lit("SEGMENT#ATTRIBUTES"), F.col(f"{STATE_DATA_COL}.struct"))
         .when(seg == F.lit("SEGMENT#PRICE"), F.col(f"{STATE_DATA_COL}.struct"))
         .otherwise(F.col(STATE_DATA_COL))
    )


def _extract_state_type_expr():
    """Best-effort extractor for state.data.type across shapes."""
    p1 = F.col(f"{STATE_DATA_COL}.type")
    p2 = F.col(f"{STATE_DATA_COL}.M.type.S")
    p3 = F.col(f"{STATE_DATA_COL}.M.type")
    j_all = F.to_json(F.col(STATE_DATA_COL))
    p4 = F.get_json_object(j_all, "$.M.type.S")
    p5 = F.get_json_object(j_all, "$.type")
    return F.lower(F.coalesce(p1.cast("string"), p2.cast("string"), p3.cast("string"), p4, p5))

def _jsonify(col):
    """
    Ensure we have a JSON string for get_json_object().
    If 'col' is already a JSON-looking string, use it; else to_json(struct/map).
    """
    s = F.col(col).cast("string")
    return F.when(F.col(col).isNull(), F.lit(None).cast("string")) \
            .when(s.rlike(r"^\s*\{"), s) \
            .otherwise(F.to_json(F.col(col)))

def _extract_state_type_as_json_only():
    j = _json_of(STATE_DATA_COL)
    c1 = F.get_json_object(j, "$.type")         # plain struct
    c2 = F.get_json_object(j, "$.M.type.S")     # AV common
    c3 = F.get_json_object(j, "$.M.type")       # AV sometimes directly under M
    c4 = F.get_json_object(j, "$.type.S")       # AV-ish variant
    c5 = F.get_json_object(j, "$.S")            # extreme AV edge
    return F.lower(F.coalesce(c1, c2, c3, c4, c5))

def _extract_state_reasons_as_array():
    """
    Extract reasons as JSON and parse to array<struct<ar:string, en:string>>, or null if absent/not an array.
    Accept both plain and AV-wrapped shapes.
    """
    from pyspark.sql.types import StructType, StructField
    j = _json_of(STATE_DATA_COL)
    r1 = F.get_json_object(j, "$.reasons")
    r2 = F.get_json_object(j, "$.M.reasons")
    r_json = F.coalesce(r1, r2)
    # Define reasons schema as array of structs with ar/en fields
    reasons_schema = ArrayType(StructType([
        StructField("ar", StringType(), True),
        StructField("en", StringType(), True)
    ]))
    # Convert empty array string "[]" to null, otherwise parse as array of structs
    return F.when(r_json.isNull() | (r_json == "[]"), F.lit(None).cast(reasons_schema)) \
            .otherwise(F.from_json(r_json, reasons_schema))


def map_state_type_expr(df):
    """
    Rebuild data as struct<type:string, reasons:array<struct<ar:string, en:string>>> and set top-level state_type.
    Uses JSON-based extraction first; if that yields null, falls back to existing top-level state_type.
    """
    if STATE_DATA_COL not in df.columns:
        return df

    # 1) Try JSON-only extraction from data; 2) fall back to existing state_type column
    src_type_from_json = _extract_state_type_as_json_only()
    src_type_str = F.coalesce(src_type_from_json, F.col("state_type").cast("string"))

    # Normalize + map (lowercased)
    mapping_map = F.create_map(*sum([[F.lit(k), F.lit(v)] for k, v in STATE_TYPE_MAP.items()], []))
    src_lc = F.lower(src_type_str)
    mapped = F.coalesce(mapping_map.getItem(src_lc), src_lc).cast("string")

    # reasons -> array<string> (or null)
    reasons_arr = _extract_state_reasons_as_array()

    # uniform struct
    new_state_struct = F.struct(
        mapped.alias("type"),
        reasons_arr.alias("reasons"),
    )

    # Always write a consistent shape (avoids CASE type mismatches)
    return (
        df
        .withColumn(STATE_DATA_COL, new_state_struct)
        .withColumn("state_type", mapped)
    )



def to_dynamic_frame(glueContext, df):
    return DynamicFrame.fromDF(df, glueContext, "tmp")

# ---------- Delete-all utilities (parallel & batched) ----------
def _delete_batch(ddb, table_name, requests, backoff_s=0.5, max_retries=12):
    attempts = 0
    remaining = list(requests)
    deleted_count = 0
    while remaining and attempts < max_retries:
        chunk = remaining[:25]
        try:
            resp = ddb.batch_write_item(RequestItems={table_name: chunk})
        except ClientError:
            attempts += 1
            time.sleep(min(backoff_s * (2 ** (attempts - 1)) * (1 + random.random()), 10.0))
            continue
        un = resp.get("UnprocessedItems", {}).get(table_name, [])
        processed = len(chunk) - len(un)
        deleted_count += processed
        if un:
            remaining = un + remaining[len(chunk):]
            attempts += 1
            time.sleep(min(backoff_s * (2 ** (attempts - 1)) * (1 + random.random()), 10.0))
        else:
            remaining = remaining[len(chunk):]
            attempts = 0
    # best-effort fallback
    for r in remaining:
        key = r["DeleteRequest"]["Key"]
        for i in range(6):
            try:
                ddb.delete_item(TableName=table_name, Key=key)
                deleted_count += 1
                break
            except ClientError:
                time.sleep(min((0.25 * (2 ** i)) * (1 + random.random()), 6.0))
    return deleted_count

def _scan_and_delete_segment(ddb, table_name, key_schema, segment, total_segments):
    expr_names = {f"#k{i}": k for i, k in enumerate(key_schema)}
    projection_expr = ",".join(expr_names.keys())
    paginator = ddb.get_paginator("scan")
    deleted = 0
    for page in paginator.paginate(
        TableName=table_name,
        ProjectionExpression=projection_expr,
        ExpressionAttributeNames=expr_names,
        ConsistentRead=True,
        Segment=segment,
        TotalSegments=total_segments
    ):
        items = page.get("Items", [])
        if not items:
            continue

        # one key dict per item (previous version accidentally multiplied entries)
        keys = [{k: item.get(k) for k in key_schema if k in item} for item in items]

        # de-duplicate if any anomalies
        uniq, seen = [], set()
        for k in keys:
            t = tuple(sorted((kk, tuple(v.items()) if isinstance(v, dict) and v is not None else str(v)) for kk, v in k.items()))
            if t not in seen:
                uniq.append(k); seen.add(t)

        for i in range(0, len(uniq), 25):
            batch_keys = uniq[i:i+25]
            requests = [{"DeleteRequest": {"Key": k}} for k in batch_keys]
            deleted += _delete_batch(ddb, table_name, requests)
    return deleted

def _table_has_items(ddb, table_name):
    try:
        resp = ddb.scan(TableName=table_name, Limit=1)
        return bool(resp.get("Items"))
    except ClientError:
        return True

def delete_all_items(table_name, region, key_schema, total_segments=4):
    ddb = boto3.client("dynamodb", region_name=region)
    while True:
        deleted_this_pass = 0
        for seg in range(total_segments):
            deleted_this_pass += _scan_and_delete_segment(ddb, table_name, key_schema, seg, total_segments)
        if deleted_this_pass == 0:
            if not _table_has_items(ddb, table_name):
                break
            time.sleep(2)
        # else: loop again for another pass

# ---------- Key alignment & assertions ----------
def align_df_to_target_keys_for_catalog(df, target_table, target_region, seg_col_name):
    """Shape DF columns to match target catalog key schema (PK/SK or catalog_id/segment)."""
    keys = get_table_key_attrs(target_table, target_region)
    segment_expr = F.col(seg_col_name) if seg_col_name in df.columns else (
        F.col(SEGMENT_COL) if SEGMENT_COL in df.columns else F.lit(None).alias(SEGMENT_COL)
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
        raise ValueError(f"DataFrame missing required DynamoDB key columns {missing} for {table}")
    null_checks = [F.count(F.when(F.col(k).isNull(), 1)).alias(k) for k in keys]
    nulls = df.agg(*null_checks).collect()[0].asDict()
    if any(nulls[k] > 0 for k in keys):
        raise ValueError(f"Nulls in key columns for {table}: {nulls}")

# ---------- Readers / Writers ----------
def write_to_ddb(glueContext, dyf, table, region, write_percent="0.6", batch_size="25"):
    glueContext.write_dynamic_frame.from_options(
        frame=dyf,
        connection_type="dynamodb",
        connection_options={
            "dynamodb.output.tableName": table,
            "dynamodb.region": region,
            "dynamodb.throughput.write.percent": write_percent,
            "dynamodb.batch.size": batch_size,
        },
    )

def read_ddb_table(glueContext, table, region, read_percent="0.5", splits="200", scan_filter=None):
    """
    Read DynamoDB table with optional scan filter to reduce data at source.
    scan_filter example: {"dynamodb.filter.expression": "segment = :seg",
                          "dynamodb.filter.expressionAttributeValues": '{":seg":{"S":"SEGMENT#AMENITIES"}}'}
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

# ---------- Migrations ----------
def _choose_segment_col(df):
    """Listings tables may store the segment name in 'segment' (preferred) or 'SK'."""
    return SEGMENT_COL if SEGMENT_COL in df.columns else (SK_COL if SK_COL in df.columns else None)

def _apply_test_filters(df, test_listing_ids, test_client_ids, run_all):
    """
    Filter dataframe by listing IDs only.

    Note: We filter by listing_id (not client_id) because client_id only exists
    in METADATA segment. All other segments (ATTRIBUTES, DESCRIPTION, PRICE, etc.)
    only have listing_id. The client_ids should already be resolved to listing_ids
    before calling this function.

    Args:
        df: DataFrame to filter
        test_listing_ids: List of listing IDs to include
        test_client_ids: List of client IDs (should already be resolved to listing_ids)
        run_all: If True, return all records (ignore filters)
    """
    if run_all:
        return df

    # Filter by listing_id only - this works for ALL segments
    if test_listing_ids and LISTING_ID_COL in df.columns:
        df = df.filter(F.col(LISTING_ID_COL).isin(test_listing_ids))

    # DO NOT filter by client_id here - it only exists in METADATA segment!
    # Client IDs should already be resolved to listing IDs before this function

    return df

def _drop_all_null_top_level_columns(df, required_cols):
    """
    Return df with any top-level columns dropped if they are all NULL across df.
    required_cols are always kept (e.g., keys).
    """
    cols = df.columns
    checks = [F.max(F.when(F.col(c).isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias(c) for c in cols]
    flags = df.agg(*checks).collect()[0].asDict()

    keep = [c for c in cols if (flags.get(c, 0) == 1) or (c in required_cols)]
    return df.select(*keep)

def _null_out_empty_hashes(df):
    """
    If hashes is an empty map (or AV struct with M as map and it's empty), set hashes to NULL.
    """
    if HASHES_COL not in df.columns:
        return df
    dt = df.schema[HASHES_COL].dataType
    if isinstance(dt, MapType):
        return df.withColumn(HASHES_COL,
                             F.when(F.size(F.map_entries(F.col(HASHES_COL))) == 0, F.lit(None)).otherwise(F.col(HASHES_COL)))
    if isinstance(dt, StructType) and "M" in dt.fieldNames() and isinstance(dt["M"].dataType, MapType):
        return df.withColumn(HASHES_COL,
                             F.when(F.size(F.map_entries(F.col(f"{HASHES_COL}.M"))) == 0, F.lit(None)).otherwise(F.col(HASHES_COL)))
    return df


def _select_non_null_columns(df, required_cols):
    """
    Keep only columns that are non-null in the current (single) row, plus required_cols.
    This prevents Glue's DynamoDB writer from emitting NULL attributes for absent fields.
    """
    sample = df.limit(1).collect()
    if not sample:
        return df
    r = sample[0].asDict(recursive=True)
    keep = [c for c in df.columns if (c in required_cols) or (r.get(c) is not None)]
    return df.select(*[F.col(c) for c in keep])


def migrate_client_reference(glueContext, logger, country_code, source_util_table, source_region, target_util_table, target_region, test_listing_ids, test_client_ids, run_all):

    logger.info(f"[MIGRATE_REFERENCE] Starting client reference migration for {country_code}")

    dyf = read_ddb_table(glueContext, source_util_table, source_region)
    df = dyf.toDF()

    logger.info(f"[MIGRATE_REFERENCE] Read {df.count()} total records from {source_util_table}")

    if PK_COL not in df.columns:
        raise ValueError(f"{source_util_table} is expected to have column '{PK_COL}'")

    df = df.filter(F.col(PK_COL).startswith("REFERENCE#"))
    logger.info(f"[MIGRATE_REFERENCE] Filtered to {df.count()} REFERENCE# records")

    df = _apply_test_filters(df, test_listing_ids, test_client_ids, run_all)
    logger.info(f"[MIGRATE_REFERENCE] After test filters: {df.count()} records")

    df = df.withColumn(PK_COL, transform_reference_pk(F.lit(country_code), F.col(PK_COL)))

    if LISTING_ID_COL in df.columns:
        df = df.withColumn(CATALOG_ID_COL, prefixed_catalog_id(F.lit(country_code), F.col(LISTING_ID_COL))).drop(LISTING_ID_COL)

    df = align_df_to_target_keys_for_util(df, target_util_table, target_region)
    assert_write_keys_present(df, target_util_table, target_region)

    out = to_dynamic_frame(glueContext, df.limit(1) if not run_all else df)
    out = DropNullFields.apply(frame=out)

    logger.info(f"[MIGRATE_REFERENCE] Writing {df.count()} records to {target_util_table}")
    write_to_ddb(glueContext, out, target_util_table, target_region)
    logger.info(f"[MIGRATE_REFERENCE] ✓ Completed client reference migration")

# def migrate_generic_segments(glueContext, logger, country_code, source_listings_table, source_region,
#                              target_catalog_table, target_region, segment_names,
#                              test_listing_ids, test_client_ids, run_all):

#     logger.info(f"[MIGRATE_SEGMENTS] Starting migration for segments: {segment_names}")

#     dyf = read_ddb_table(glueContext, source_listings_table, source_region)
#     df = dyf.toDF()

#     logger.info(f"[MIGRATE_SEGMENTS] Read {df.count()} total records from {source_listings_table}")

#     seg_col_name = _choose_segment_col(df)
#     if not seg_col_name:
#         raise ValueError(f"{source_listings_table} does not have '{SEGMENT_COL}' or '{SK_COL}'")

#     wanted = [f"SEGMENT#{s}" for s in segment_names]
#     df = df.filter(F.col(seg_col_name).isin(wanted))
#     logger.info(f"[MIGRATE_SEGMENTS] Filtered to {df.count()} segment records")

#     df = _apply_test_filters(df, test_listing_ids, test_client_ids, run_all)
#     logger.info(f"[MIGRATE_SEGMENTS] After test filters: {df.count()} records")

#     # listing_id -> catalog_id
#     if LISTING_ID_COL in df.columns:
#         df = df.withColumn(
#             CATALOG_ID_COL,
#             prefixed_catalog_id(F.lit(country_code), F.col(LISTING_ID_COL))
#         ).drop(LISTING_ID_COL)

#     # Key alignment once (keeps both key styles)
#     df = align_df_to_target_keys_for_catalog(df, target_catalog_table, target_region, seg_col_name)
#     assert_write_keys_present(df, target_catalog_table, target_region)

#     # DROP quality_score from ALL segments
#     if "quality_score" in df.columns:
#         logger.info(f"[MIGRATE_SEGMENTS] Dropping quality_score column")
#         df = df.drop("quality_score")

#     # --- Write each segment separately so we can prune null-only columns per segment ---
#     required_keys = set(get_table_key_attrs(target_catalog_table, target_region))
#     required_keys.update([CATALOG_ID_COL, SEGMENT_COL])  # keep key aliases if present

#     for seg_name in segment_names:
#         seg_value = f"SEGMENT#{seg_name}"
#         logger.info(f"[MIGRATE_SEGMENTS] Processing {seg_name} segment...")

#         # CRITICAL: Filter to THIS segment ONLY - creates isolated dataframe
#         df_seg = df.filter(F.col(seg_col_name) == F.lit(seg_value))

#         seg_count = df_seg.count()
#         if seg_count == 0:
#             logger.info(f"[MIGRATE_SEGMENTS] ⚠ No records for {seg_name}, skipping")
#             continue

#         logger.info(f"[MIGRATE_SEGMENTS] Found {seg_count} records for {seg_name}")

#         # --- normalize `data` per segment to avoid CASE type mixing ---
#         if seg_name == "AMENITIES":
#             if "data" in df_seg.columns:
#                 df_seg = df_seg.withColumn("data", F.col("data.array"))
#                 df_seg = df_seg.withColumn(
#                     "data",
#                     F.when(F.col("data").isNull() | (F.size(F.col("data")) == 0), F.lit(None)).otherwise(F.col("data"))
#                 )
#                 logger.info(f"[MIGRATE_SEGMENTS] Normalized AMENITIES data to array")

#         elif seg_name == "ATTRIBUTES":
#             if "data" in df_seg.columns:
#                 df_seg = df_seg.withColumn("data", F.col("data.struct"))
#                 logger.info(f"[MIGRATE_SEGMENTS] Normalized ATTRIBUTES data to struct")

#         elif seg_name == "PRICE":
#             if "data" in df_seg.columns:
#                 df_seg = df_seg.withColumn("data", F.col("data.struct"))
#                 logger.info(f"[MIGRATE_SEGMENTS] Normalized PRICE data to struct")

#         elif seg_name == "DESCRIPTION":
#             # DESCRIPTION stores content in top-level `description`, not `data`
#             if "data" in df_seg.columns:
#                 df_seg = df_seg.drop("data")
#                 logger.info(f"[MIGRATE_SEGMENTS] Dropped data column for DESCRIPTION")

#         # drop top-level columns that are all-null within THIS segment (keeps keys)
#         df_seg = _drop_all_null_top_level_columns(
#             df_seg,
#             required_cols=required_keys
#         )

#         logger.info(f"[MIGRATE_SEGMENTS] After null pruning: {len(df_seg.columns)} columns")

#         out = to_dynamic_frame(glueContext, df_seg.limit(1) if not run_all else df_seg)
#         out = DropNullFields.apply(frame=out)

#         logger.info(f"[MIGRATE_SEGMENTS] Writing {seg_count} records for {seg_name} to {target_catalog_table}")
#         write_to_ddb(glueContext, out, target_catalog_table, target_region)
#         logger.info(f"[MIGRATE_SEGMENTS] ✓ Completed {seg_name} migration")


def migrate_amenities_segment(glueContext, logger, country_code, source_listings_table, source_region,
                              target_catalog_table, target_region, test_listing_ids, test_client_ids, run_all):
    """Migrate AMENITIES segment - stores array of amenity strings"""
    logger.info(f"[MIGRATE_AMENITIES] Starting AMENITIES segment migration")

    dyf = read_ddb_table(glueContext, source_listings_table, source_region)
    df = dyf.toDF()

    logger.info(f"[MIGRATE_AMENITIES] Read {df.count()} total records")

    seg_col_name = _choose_segment_col(df)
    if not seg_col_name:
        raise ValueError(f"{source_listings_table} does not have '{SEGMENT_COL}' or '{SK_COL}'")

    df = df.filter(F.col(seg_col_name) == F.lit("SEGMENT#AMENITIES"))
    logger.info(f"[MIGRATE_AMENITIES] Filtered to {df.count()} AMENITIES records")

    df = _apply_test_filters(df, test_listing_ids, test_client_ids, run_all)
    logger.info(f"[MIGRATE_AMENITIES] After test filters: {df.count()} records")

    if df.rdd.isEmpty():
        logger.info(f"[MIGRATE_AMENITIES] No records to migrate, skipping")
        return

    # listing_id -> catalog_id
    if LISTING_ID_COL in df.columns:
        df = df.withColumn(CATALOG_ID_COL, prefixed_catalog_id(F.lit(country_code), F.col(LISTING_ID_COL))).drop(LISTING_ID_COL)

    # Extract array payload from data
    if "data" in df.columns:
        df = df.withColumn("data", F.col("data.array"))
        # Null out empty arrays
        df = df.withColumn(
            "data",
            F.when(F.col("data").isNull() | (F.size(F.col("data")) == 0), F.lit(None)).otherwise(F.col("data"))
        )
        logger.info(f"[MIGRATE_AMENITIES] Extracted array payload from data")

    # Drop quality_score
    if "quality_score" in df.columns:
        df = df.drop("quality_score")

    df = align_df_to_target_keys_for_catalog(df, target_catalog_table, target_region, seg_col_name)
    assert_write_keys_present(df, target_catalog_table, target_region)

    required_keys = set(get_table_key_attrs(target_catalog_table, target_region))
    required_keys.update([CATALOG_ID_COL, SEGMENT_COL])

    df = _drop_all_null_top_level_columns(df, required_cols=required_keys)
    logger.info(f"[MIGRATE_AMENITIES] After null pruning: {len(df.columns)} columns")

    out = to_dynamic_frame(glueContext, df.limit(1) if not run_all else df)
    out = DropNullFields.apply(frame=out)

    logger.info(f"[MIGRATE_AMENITIES] Writing {df.count()} records to {target_catalog_table}")
    write_to_ddb(glueContext, out, target_catalog_table, target_region)
    logger.info(f"[MIGRATE_AMENITIES] ✓ Completed AMENITIES migration")


def migrate_attributes_segment(glueContext, logger, country_code, source_listings_table, source_region,
                               target_catalog_table, target_region, test_listing_ids, test_client_ids, run_all):
    """Migrate ATTRIBUTES segment - stores property attributes as struct"""
    logger.info(f"[MIGRATE_ATTRIBUTES] Starting ATTRIBUTES segment migration")

    dyf = read_ddb_table(glueContext, source_listings_table, source_region)
    df = dyf.toDF()

    logger.info(f"[MIGRATE_ATTRIBUTES] Read {df.count()} total records")

    seg_col_name = _choose_segment_col(df)
    if not seg_col_name:
        raise ValueError(f"{source_listings_table} does not have '{SEGMENT_COL}' or '{SK_COL}'")

    df = df.filter(F.col(seg_col_name) == F.lit("SEGMENT#ATTRIBUTES"))
    logger.info(f"[MIGRATE_ATTRIBUTES] Filtered to {df.count()} ATTRIBUTES records")

    df = _apply_test_filters(df, test_listing_ids, test_client_ids, run_all)
    logger.info(f"[MIGRATE_ATTRIBUTES] After test filters: {df.count()} records")

    if df.rdd.isEmpty():
        logger.info(f"[MIGRATE_ATTRIBUTES] No records to migrate, skipping")
        return

    # listing_id -> catalog_id
    if LISTING_ID_COL in df.columns:
        df = df.withColumn(CATALOG_ID_COL, prefixed_catalog_id(F.lit(country_code), F.col(LISTING_ID_COL))).drop(LISTING_ID_COL)

    # Define ATTRIBUTES-specific fields
    ATTRIBUTES_FIELDS = [
        "age", "available_from", "bathrooms", "bedrooms", "category", "developer",
        "finishing_type", "floor_number", "furnishing_type", "has_garden", "has_kitchen",
        "has_parking_on_site", "land_number", "living_rooms", "moj_deed_location_description",
        "number_of_floors", "owner_name", "parking_slots", "plot_number", "plot_size",
        "size", "street", "title", "type", "unit_number", "user_confirmed_data_is_correct"
    ]

    # Rebuild data struct with ONLY attributes fields
    if "data" in df.columns:
        attr_struct_fields = []
        for field in ATTRIBUTES_FIELDS:
            try:
                # Check if field exists in schema
                df.select(f"data.struct.{field}").first()
                attr_struct_fields.append(F.col(f"data.struct.{field}").alias(field))
            except:
                pass  # Field doesn't exist in this schema

        if attr_struct_fields:
            df = df.withColumn("data", F.struct(*attr_struct_fields))
            logger.info(f"[MIGRATE_ATTRIBUTES] Rebuilt data struct with {len(attr_struct_fields)} attributes fields")
        else:
            df = df.withColumn("data", F.col("data.struct"))
            logger.info(f"[MIGRATE_ATTRIBUTES] Used full struct (no field filtering applied)")

    # Drop quality_score
    if "quality_score" in df.columns:
        df = df.drop("quality_score")

    df = align_df_to_target_keys_for_catalog(df, target_catalog_table, target_region, seg_col_name)
    assert_write_keys_present(df, target_catalog_table, target_region)

    required_keys = set(get_table_key_attrs(target_catalog_table, target_region))
    required_keys.update([CATALOG_ID_COL, SEGMENT_COL])

    df = _drop_all_null_top_level_columns(df, required_cols=required_keys)
    logger.info(f"[MIGRATE_ATTRIBUTES] After null pruning: {len(df.columns)} columns")

    out = to_dynamic_frame(glueContext, df.limit(1) if not run_all else df)
    out = DropNullFields.apply(frame=out)

    logger.info(f"[MIGRATE_ATTRIBUTES] Writing {df.count()} records to {target_catalog_table}")
    write_to_ddb(glueContext, out, target_catalog_table, target_region)
    logger.info(f"[MIGRATE_ATTRIBUTES] ✓ Completed ATTRIBUTES migration")


def migrate_description_segment(glueContext, logger, country_code, source_listings_table, source_region,
                                target_catalog_table, target_region, test_listing_ids, test_client_ids, run_all):
    """Migrate DESCRIPTION segment - stores description in top-level field, not in data"""
    logger.info(f"[MIGRATE_DESCRIPTION] Starting DESCRIPTION segment migration")

    dyf = read_ddb_table(glueContext, source_listings_table, source_region)
    df = dyf.toDF()

    logger.info(f"[MIGRATE_DESCRIPTION] Read {df.count()} total records")

    seg_col_name = _choose_segment_col(df)
    if not seg_col_name:
        raise ValueError(f"{source_listings_table} does not have '{SEGMENT_COL}' or '{SK_COL}'")

    df = df.filter(F.col(seg_col_name) == F.lit("SEGMENT#DESCRIPTION"))
    logger.info(f"[MIGRATE_DESCRIPTION] Filtered to {df.count()} DESCRIPTION records")

    df = _apply_test_filters(df, test_listing_ids, test_client_ids, run_all)
    logger.info(f"[MIGRATE_DESCRIPTION] After test filters: {df.count()} records")

    if df.rdd.isEmpty():
        logger.info(f"[MIGRATE_DESCRIPTION] No records to migrate, skipping")
        return

    # listing_id -> catalog_id
    if LISTING_ID_COL in df.columns:
        df = df.withColumn(CATALOG_ID_COL, prefixed_catalog_id(F.lit(country_code), F.col(LISTING_ID_COL))).drop(LISTING_ID_COL)

    # DESCRIPTION stores content in top-level 'description' field, drop 'data' if present
    if "data" in df.columns:
        df = df.drop("data")
        logger.info(f"[MIGRATE_DESCRIPTION] Dropped data column (description stored at top level)")

    # Drop quality_score
    if "quality_score" in df.columns:
        df = df.drop("quality_score")

    df = align_df_to_target_keys_for_catalog(df, target_catalog_table, target_region, seg_col_name)
    assert_write_keys_present(df, target_catalog_table, target_region)

    required_keys = set(get_table_key_attrs(target_catalog_table, target_region))
    required_keys.update([CATALOG_ID_COL, SEGMENT_COL])

    df = _drop_all_null_top_level_columns(df, required_cols=required_keys)
    logger.info(f"[MIGRATE_DESCRIPTION] After null pruning: {len(df.columns)} columns")

    out = to_dynamic_frame(glueContext, df.limit(1) if not run_all else df)
    out = DropNullFields.apply(frame=out)

    logger.info(f"[MIGRATE_DESCRIPTION] Writing {df.count()} records to {target_catalog_table}")
    write_to_ddb(glueContext, out, target_catalog_table, target_region)
    logger.info(f"[MIGRATE_DESCRIPTION] ✓ Completed DESCRIPTION migration")


def migrate_price_segment(glueContext, logger, country_code, source_listings_table, source_region,
                          target_catalog_table, target_region, test_listing_ids, test_client_ids, run_all):
    """Migrate PRICE segment - stores pricing information as struct"""
    logger.info(f"[MIGRATE_PRICE] Starting PRICE segment migration")

    dyf = read_ddb_table(glueContext, source_listings_table, source_region)
    df = dyf.toDF()

    logger.info(f"[MIGRATE_PRICE] Read {df.count()} total records")

    seg_col_name = _choose_segment_col(df)
    if not seg_col_name:
        raise ValueError(f"{source_listings_table} does not have '{SEGMENT_COL}' or '{SK_COL}'")

    df = df.filter(F.col(seg_col_name) == F.lit("SEGMENT#PRICE"))
    logger.info(f"[MIGRATE_PRICE] Filtered to {df.count()} PRICE records")

    df = _apply_test_filters(df, test_listing_ids, test_client_ids, run_all)
    logger.info(f"[MIGRATE_PRICE] After test filters: {df.count()} records")

    if df.rdd.isEmpty():
        logger.info(f"[MIGRATE_PRICE] No records to migrate, skipping")
        return

    # listing_id -> catalog_id
    if LISTING_ID_COL in df.columns:
        df = df.withColumn(CATALOG_ID_COL, prefixed_catalog_id(F.lit(country_code), F.col(LISTING_ID_COL))).drop(LISTING_ID_COL)

    # Define PRICE-specific fields
    PRICE_FIELDS = [
        "amounts", "downpayment", "FilterAmount", "minimal_rental_period", "mortgage",
        "number_of_cheques", "number_of_mortgage_years", "obligation", "on_request",
        "payment_methods", "SortAmount", "type", "utilities_inclusive", "value_affected"
    ]

    # Rebuild data struct with ONLY price fields
    if "data" in df.columns:
        price_struct_fields = []
        for field in PRICE_FIELDS:
            try:
                # Check if field exists in schema
                df.select(f"data.struct.{field}").first()
                price_struct_fields.append(F.col(f"data.struct.{field}").alias(field))
            except:
                pass  # Field doesn't exist in this schema

        if price_struct_fields:
            df = df.withColumn("data", F.struct(*price_struct_fields))
            logger.info(f"[MIGRATE_PRICE] Rebuilt data struct with {len(price_struct_fields)} price fields")
        else:
            df = df.withColumn("data", F.col("data.struct"))
            logger.info(f"[MIGRATE_PRICE] Used full struct (no field filtering applied)")

    # Drop quality_score
    if "quality_score" in df.columns:
        df = df.drop("quality_score")

    df = align_df_to_target_keys_for_catalog(df, target_catalog_table, target_region, seg_col_name)
    assert_write_keys_present(df, target_catalog_table, target_region)

    required_keys = set(get_table_key_attrs(target_catalog_table, target_region))
    required_keys.update([CATALOG_ID_COL, SEGMENT_COL])

    df = _drop_all_null_top_level_columns(df, required_cols=required_keys)
    logger.info(f"[MIGRATE_PRICE] After null pruning: {len(df.columns)} columns")

    out = to_dynamic_frame(glueContext, df.limit(1) if not run_all else df)
    out = DropNullFields.apply(frame=out)

    logger.info(f"[MIGRATE_PRICE] Writing {df.count()} records to {target_catalog_table}")
    write_to_ddb(glueContext, out, target_catalog_table, target_region)
    logger.info(f"[MIGRATE_PRICE] ✓ Completed PRICE migration")

def migrate_metadata_segment(glueContext, logger, country_code, source_listings_table, source_region, target_catalog_table, target_region, test_listing_ids, test_client_ids, run_all):

    logger.info(f"[MIGRATE_METADATA] Starting METADATA segment migration")

    dyf = read_ddb_table(glueContext, source_listings_table, source_region)
    df = dyf.toDF()

    logger.info(f"[MIGRATE_METADATA] Read {df.count()} total records")

    seg_col_name = _choose_segment_col(df)
    if not seg_col_name:
        raise ValueError(f"{source_listings_table} does not have '{SEGMENT_COL}' or '{SK_COL}'")

    df = df.filter(F.col(seg_col_name) == F.lit(f"SEGMENT#{METADATA_SEGMENT}"))
    logger.info(f"[MIGRATE_METADATA] Filtered to {df.count()} METADATA records")

    df = _apply_test_filters(df, test_listing_ids, test_client_ids, run_all)
    logger.info(f"[MIGRATE_METADATA] After test filters: {df.count()} records")

    if LISTING_ID_COL in df.columns:
        df = df.withColumn(CATALOG_ID_COL, prefixed_catalog_id(F.lit(country_code), F.col(LISTING_ID_COL))).drop(LISTING_ID_COL)

    # DROP quality_score if present
    if "quality_score" in df.columns:
        logger.info(f"[MIGRATE_METADATA] Dropping quality_score column")
        df = df.drop("quality_score")

    if HASHES_COL in df.columns:
        logger.info(f"[MIGRATE_METADATA] Stripping excluded keys from hashes")
        df = strip_metadata_hashes_sql(df)
        df = _null_out_empty_hashes(df)

    df = align_df_to_target_keys_for_catalog(df, target_catalog_table, target_region, seg_col_name)
    assert_write_keys_present(df, target_catalog_table, target_region)

    required = set(get_table_key_attrs(target_catalog_table, target_region))
    required.update([CATALOG_ID_COL, SEGMENT_COL])

    df = _drop_all_null_top_level_columns(df, required_cols=required)
    logger.info(f"[MIGRATE_METADATA] After null pruning: {len(df.columns)} columns")

    out = to_dynamic_frame(glueContext, df.limit(1) if not run_all else df)
    out = DropNullFields.apply(frame=out)

    logger.info(f"[MIGRATE_METADATA] Writing {df.count()} records to {target_catalog_table}")
    write_to_ddb(glueContext, out, target_catalog_table, target_region)
    logger.info(f"[MIGRATE_METADATA] ✓ Completed METADATA migration")

def migrate_state_segment(glueContext, logger, country_code, source_listings_table, source_region, target_catalog_table, target_region, test_listing_ids, test_client_ids, run_all):

    logger.info(f"[MIGRATE_STATE] Starting STATE segment migration")

    dyf = read_ddb_table(glueContext, source_listings_table, source_region)
    df = dyf.toDF()

    logger.info(f"[MIGRATE_STATE] Read {df.count()} total records")

    seg_col_name = _choose_segment_col(df)
    if not seg_col_name:
        raise ValueError(f"{source_listings_table} does not have '{SEGMENT_COL}' or '{SK_COL}'")

    # Filter to STATE segment early
    df = df.filter(F.col(seg_col_name) == F.lit(f"SEGMENT#{STATE_SEGMENT}"))
    logger.info(f"[MIGRATE_STATE] Filtered to {df.count()} STATE records")

    # Apply filters
    df = _apply_test_filters(df, test_listing_ids, test_client_ids, run_all)
    logger.info(f"[MIGRATE_STATE] After test filters: {df.count()} records")

    # listing_id -> catalog_id
    if LISTING_ID_COL in df.columns:
        df = df.withColumn(CATALOG_ID_COL, prefixed_catalog_id(F.lit(country_code), F.col(LISTING_ID_COL))).drop(LISTING_ID_COL)

    # DROP quality_score if present
    if "quality_score" in df.columns:
        logger.info(f"[MIGRATE_STATE] Dropping quality_score column")
        df = df.drop("quality_score")

    # Map and rebuild state.data + state_type robustly
    logger.info(f"[MIGRATE_STATE] Mapping state types")
    df = map_state_type_expr(df)

    # Align keys and assert
    df = align_df_to_target_keys_for_catalog(df, target_catalog_table, target_region, seg_col_name)
    assert_write_keys_present(df, target_catalog_table, target_region)

    required = set(get_table_key_attrs(target_catalog_table, target_region))
    required.update([CATALOG_ID_COL, SEGMENT_COL])

    df = _drop_all_null_top_level_columns(df, required_cols=required)
    logger.info(f"[MIGRATE_STATE] After null pruning: {len(df.columns)} columns")

    # Write only the single row in test mode
    out = to_dynamic_frame(glueContext, df.limit(1) if not run_all else df)
    out = DropNullFields.apply(frame=out)

    logger.info(f"[MIGRATE_STATE] Writing {df.count()} records to {target_catalog_table}")
    write_to_ddb(glueContext, out, target_catalog_table, target_region)
    logger.info(f"[MIGRATE_STATE] ✓ Completed STATE migration")


def run_migration(args, glueContext, logger):
    logger.info("="*80)
    logger.info("[MIGRATION] Starting full migration process")
    logger.info("="*80)

    country_code = args["COUNTRY_CODE"].upper().strip()
    source_region = args["SOURCE_REGION"]
    target_region = args["TARGET_REGION"]
    source_util_table = args["SOURCE_UTIL_TABLE"]
    source_listings_table = args["SOURCE_LISTINGS_TABLE"]
    target_util_table = args["TARGET_UTIL_TABLE"]
    target_catalog_table = args["TARGET_CATALOG_TABLE"]

    logger.info(f"[MIGRATION] Country: {country_code}")
    logger.info(f"[MIGRATION] Source: {source_listings_table} ({source_region})")
    logger.info(f"[MIGRATION] Target: {target_catalog_table} ({target_region})")

    test_listing_ids = args.get("TEST_LISTING_IDS", [])
    test_client_ids = args.get("TEST_CLIENT_IDS", [])
    run_all = (len(test_listing_ids) == 0 and len(test_client_ids) == 0)

    logger.info(f"[MIGRATION] Run mode: {'FULL MIGRATION' if run_all else 'TEST MODE'}")
    if not run_all:
        logger.info(f"[MIGRATION] Test listing IDs: {test_listing_ids}")
        logger.info(f"[MIGRATION] Test client IDs: {test_client_ids}")

    logger.info("\n[MIGRATION] Step 1/6: Migrating client references...")
    migrate_client_reference(
        glueContext, logger, country_code,
        source_util_table, source_region,
        target_util_table, target_region,
        test_listing_ids, test_client_ids, run_all,
    )

    logger.info("\n[MIGRATION] Step 2/6: Migrating AMENITIES segment...")
    migrate_amenities_segment(
        glueContext, logger, country_code,
        source_listings_table, source_region,
        target_catalog_table, target_region,
        test_listing_ids, test_client_ids, run_all,
    )

    logger.info("\n[MIGRATION] Step 3/6: Migrating ATTRIBUTES segment...")
    migrate_attributes_segment(
        glueContext, logger, country_code,
        source_listings_table, source_region,
        target_catalog_table, target_region,
        test_listing_ids, test_client_ids, run_all,
    )

    logger.info("\n[MIGRATION] Step 4/6: Migrating DESCRIPTION segment...")
    migrate_description_segment(
        glueContext, logger, country_code,
        source_listings_table, source_region,
        target_catalog_table, target_region,
        test_listing_ids, test_client_ids, run_all,
    )

    logger.info("\n[MIGRATION] Step 5/6: Migrating PRICE segment...")
    migrate_price_segment(
        glueContext, logger, country_code,
        source_listings_table, source_region,
        target_catalog_table, target_region,
        test_listing_ids, test_client_ids, run_all,
    )

    logger.info("\n[MIGRATION] Step 6/6: Migrating METADATA segment...")
    migrate_metadata_segment(
        glueContext, logger, country_code,
        source_listings_table, source_region,
        target_catalog_table, target_region,
        test_listing_ids=test_listing_ids,
        test_client_ids=test_client_ids,
        run_all=run_all,
    )

    logger.info("\n[MIGRATION] Step 7/7: Migrating STATE segment...")
    migrate_state_segment(
        glueContext, logger, country_code,
        source_listings_table, source_region,
        target_catalog_table, target_region,
        test_listing_ids=test_listing_ids,
        test_client_ids=test_client_ids,
        run_all=run_all,
    )

    logger.info("="*80)
    logger.info("[MIGRATION] ✓ Migration process completed successfully")
    logger.info("="*80)

# ---------- Arg parsing helpers ----------
def _resolve_optional(argv, key, default_val):
    """
    Robustly read optional Glue job param:
    accepts --KEY value  OR  --KEY=value (case-sensitive KEY).
    Returns string (like getResolvedOptions) or default_val if absent.
    """
    flag = f"--{key}"
    for i, a in enumerate(argv):
        if a == flag and i + 1 < len(argv):
            return argv[i + 1]
        if a.startswith(flag + "="):
            return a.split("=", 1)[1]
    return default_val

def _as_bool(v, default=False):
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in ("1", "true", "t", "yes", "y")



############################### SEARCH AGGREGATOR ############################################

# ================================
# v2 ADDITIONS (fixed to use JSON-only; no unresolved columns)
# ================================
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType

# ---- v2 helpers ----
def _json_of_v2(colname: str):
    s = F.col(colname).cast("string")
    return F.coalesce(F.when(s.rlike(r"^\s*\{"), s), F.to_json(F.col(colname)))

def _jget_v2(path: str, col: str = "data"):
    # Safe JSON extractor from struct/map or stringified JSON
    return F.get_json_object(_json_of_v2(col), f"$.{path}")

def _jget_multi_v2(paths, col: str = "data"):
    # COALESCE across multiple JSON paths
    return F.coalesce(*[_jget_v2(p, col) for p in paths])

def _group_first_non_null_v2(df, key_col, cols):
    """
    Group by key_col and aggregate columns.
    For 'updated_at', use max to get the latest timestamp across all segments.
    For other columns, use first non-null value.
    """
    aggs = []
    for c in cols:
        if c == "updated_at":
            # Use max to get the latest updated_at across all segments
            aggs.append(F.max(c).alias(c))
        else:
            # Use first non-null for other columns
            aggs.append(F.first(c, ignorenulls=True).alias(c))
    return df.groupBy(key_col).agg(*aggs)

def _amenities_both_shapes_v2(data_col="data"):
    j = _json_of_v2(data_col)
    return F.coalesce(
        F.get_json_object(j, "$.array"),     # plain
        F.get_json_object(j, "$.M.array.L")  # AV
    )

def _latest_per_segment_v2(df, seg_col_name, seg_name):
    from pyspark.sql import Window
    sdf = (df.filter(F.col(seg_col_name) == F.lit(f"SEGMENT#{seg_name}"))
             .withColumn("_ts", F.coalesce(F.col("updated_at"), F.col("created_at")).cast("timestamp")))
    w = Window.partitionBy(LISTING_ID_COL).orderBy(F.col("_ts").desc_nulls_last())
    return (sdf.withColumn("__rn", F.row_number().over(w))
               .filter(F.col("__rn") == 1)
               .drop("__rn", "_ts"))

def map_state_type_expr_v2(df, state_col_name="data"):
    if state_col_name not in df.columns:
        return df
    j = _json_of_v2(state_col_name)

    src_type = F.lower(F.coalesce(
        F.get_json_object(j, "$.type"),
        F.get_json_object(j, "$.M.type.S"),
        F.get_json_object(j, "$.M.type"),
        F.get_json_object(j, "$.type.S"),
        F.get_json_object(j, "$.S")
    ))
    src_stage = F.coalesce(
        F.get_json_object(j, "$.stage"),
        F.get_json_object(j, "$.M.stage.S"),
        F.get_json_object(j, "$.M.stage"),
        F.get_json_object(j, "$.stage.S")
    )
    reasons_json = F.coalesce(
        F.get_json_object(j, "$.reasons"),
        F.get_json_object(j, "$.M.reasons")
    )
    # Define reasons schema as array of structs with ar/en fields
    from pyspark.sql.types import StructType, StructField
    reasons_schema = ArrayType(StructType([
        StructField("ar", StringType(), True),
        StructField("en", StringType(), True)
    ]))
    # Convert empty array string "[]" to null, otherwise parse as array of structs
    reasons_arr = F.when(reasons_json.isNull() | (reasons_json == "[]"), F.lit(None).cast(reasons_schema)) \
                   .otherwise(F.from_json(reasons_json, reasons_schema))

    mapping_map = F.create_map(*sum([[F.lit(k), F.lit(v)] for k, v in STATE_TYPE_MAP.items()], []))
    mapped_type = F.coalesce(mapping_map.getItem(src_type), src_type).cast("string")

    return (df
            .withColumn(state_col_name, F.struct(
                mapped_type.alias("type"),
                src_stage.alias("stage"),
                reasons_arr.alias("reasons"),
            ))
            .withColumn("state_type", mapped_type))


def _extract_attr_fields_v2(attr_df):
    """Extract attributes segment fields"""
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType


    # Helper function to safely check if a nested field exists
    def has_nested_field(df, field_path):
        try:
            df.select(field_path).first()
            return True
        except:
            return False

    # Helper to extract numeric value from potentially wrapped field
    def extract_numeric_value(col_path):
        """
        Extract numeric from {long: N}, {double: N}, or plain N.
        Similar to catalog.py's unwrap_spark_type_wrappers but for a single field.

        This checks the schema to determine if the field is a struct wrapper or already unwrapped.
        """
        try:
            # Navigate to the field in the schema
            parts = col_path.split(".")
            field_type = attr_df.schema
            for part in parts:
                if isinstance(field_type, StructType):
                    field_type = field_type[part].dataType
                else:
                    # Can't navigate further, return None
                    print(f"[DEBUG] extract_numeric_value: Can't navigate to {col_path}, stopped at {part}")
                    return F.lit(None).cast("long")

            # Check if it's a struct with long/double wrapper
            if isinstance(field_type, StructType):
                field_names = field_type.fieldNames()
                print(f"[DEBUG] extract_numeric_value: {col_path} is struct with fields: {field_names}")
                # If both long and double exist, use coalesce to get whichever is not null
                if "long" in field_names and "double" in field_names:
                    return F.coalesce(
                        F.col(f"{col_path}.double"),
                        F.col(f"{col_path}.long").cast("double")
                    )
                elif "long" in field_names:
                    return F.col(f"{col_path}.long")
                elif "double" in field_names:
                    return F.col(f"{col_path}.double")
                else:
                    # Unknown struct type
                    return F.lit(None)
            else:
                # Already a primitive type
                print(f"[DEBUG] extract_numeric_value: {col_path} is primitive type: {field_type}")
                return F.col(col_path)
        except Exception as e:
            # If anything fails, return None
            print(f"[DEBUG] extract_numeric_value: Exception for {col_path}: {e}")
            return F.lit(None).cast("long")

    # Detect if data has 'struct' wrapper or fields are directly under 'data'
    # After JSON parsing, the structure might be data.field instead of data.struct.field
    has_struct_wrapper = has_nested_field(attr_df, "data.struct")
    data_prefix = "data.struct" if has_struct_wrapper else "data"

    print(f"[EXTRACT_ATTRIBUTES] data_prefix: {data_prefix}")
    print(f"[EXTRACT_ATTRIBUTES] has owner_name: {has_nested_field(attr_df, f'{data_prefix}.owner_name')}")

    # Helper to safely get field or return None
    def safe_col(field_path, alias_name, cast_type="string"):
        if has_nested_field(attr_df, field_path):
            return F.col(field_path).alias(alias_name)
        else:
            return F.lit(None).cast(cast_type).alias(alias_name)

    select_cols = [
        LISTING_ID_COL,
        safe_col(f"{data_prefix}.age", "age"),
        safe_col(f"{data_prefix}.available_from", "availableFrom"),

        F.coalesce(
            F.col("bathrooms") if "bathrooms" in attr_df.columns else F.lit(None),
            F.col(f"{data_prefix}.bathrooms") if has_nested_field(attr_df, f"{data_prefix}.bathrooms") else F.lit(None)
        ).alias("bathrooms"),

        F.coalesce(
            F.col("bedrooms") if "bedrooms" in attr_df.columns else F.lit(None),
            F.col(f"{data_prefix}.bedrooms") if has_nested_field(attr_df, f"{data_prefix}.bedrooms") else F.lit(None)
        ).alias("bedrooms"),

        F.coalesce(
            F.col("category") if "category" in attr_df.columns else F.lit(None),
            F.col(f"{data_prefix}.category") if has_nested_field(attr_df, f"{data_prefix}.category") else F.lit(None)
        ).alias("category"),

        safe_col(f"{data_prefix}.developer", "developer"),
        safe_col(f"{data_prefix}.finishing_type", "finishingType"),
        safe_col(f"{data_prefix}.floor_number", "floorNumber"),
        safe_col(f"{data_prefix}.furnishing_type", "furnishingType"),

        # Boolean fields
        safe_col(f"{data_prefix}.has_garden", "hasGarden", "boolean"),
        safe_col(f"{data_prefix}.has_kitchen", "hasKitchen", "boolean"),
        safe_col(f"{data_prefix}.has_parking_on_site", "hasParkingOnSite", "boolean"),

        # Land/plot fields
        safe_col(f"{data_prefix}.land_number", "landNumber"),
        safe_col(f"{data_prefix}.number_of_floors", "numberOfFloors"),
        safe_col(f"{data_prefix}.plot_number", "plotNumber"),
        safe_col(f"{data_prefix}.plot_size", "plotSize"),

        safe_col(f"{data_prefix}.parking_slots", "parkingSlots"),
        safe_col(f"{data_prefix}.project_status", "projectStatus"),
        # Size field - extract as-is, will unwrap later if needed
        safe_col(f"{data_prefix}.size", "size_raw"),
        # Transform street field - create struct with width (int) and direction (string)
        # Source has street.Width and street.Direction, output should be street: {width: N, direction: "X"}
        # Handle both cases: street as struct (STRUCT<Direction: STRING, Width: BIGINT>) or as JSON string
        (F.when(
            F.col(f"{data_prefix}.street").isNotNull(),
            F.struct(
                # Try struct access first (when street is already a struct), fallback to JSON parsing
                F.coalesce(
                    F.col(f"{data_prefix}.street.Width") if has_nested_field(attr_df, f"{data_prefix}.street.Width") else F.lit(None),
                    F.when(F.col(f"{data_prefix}.street").cast("string").rlike(r'^\s*\{'),
                           F.coalesce(
                               F.get_json_object(F.col(f"{data_prefix}.street").cast("string"), "$.Width.long"),
                               F.get_json_object(F.col(f"{data_prefix}.street").cast("string"), "$.Width")
                           ))
                ).cast("int").alias("width"),
                F.coalesce(
                    F.col(f"{data_prefix}.street.Direction") if has_nested_field(attr_df, f"{data_prefix}.street.Direction") else F.lit(None),
                    F.when(F.col(f"{data_prefix}.street").cast("string").rlike(r'^\s*\{'),
                           F.get_json_object(F.col(f"{data_prefix}.street").cast("string"), "$.Direction"))
                ).alias("direction")
            )
        ) if has_nested_field(attr_df, f"{data_prefix}.street")
         else F.lit(None)).alias("street"),
        F.coalesce(
            F.col("type") if "type" in attr_df.columns else F.lit(None),
            F.col(f"{data_prefix}.type") if has_nested_field(attr_df, f"{data_prefix}.type") else F.lit(None)
        ).alias("type"),

        safe_col(f"{data_prefix}.unit_number", "unitNumber"),
        safe_col(f"{data_prefix}.title.en", "title_en") if has_nested_field(attr_df, f"{data_prefix}.title") else F.lit(None).alias("title_en"),
        safe_col(f"{data_prefix}.title.ar", "title_ar") if has_nested_field(attr_df, f"{data_prefix}.title") else F.lit(None).alias("title_ar"),
        safe_col(f"{data_prefix}.owner_name", "ownerName"),
        safe_col(f"{data_prefix}.uae_emirate", "uaeEmirate")
    ]

    # First select all columns
    result_df = attr_df.select(*select_cols)

    # Check the actual schema of size_raw to determine how to handle it
    from pyspark.sql.types import StructType
    size_raw_type = result_df.schema["size_raw"].dataType

    print(f"[DEBUG] size_raw schema type: {size_raw_type}")
    print(f"[DEBUG] is StructType: {isinstance(size_raw_type, StructType)}")

    # Build the appropriate expression based on the schema
    if isinstance(size_raw_type, StructType):
        # It's a struct, unwrap it and cast to double
        print("[DEBUG] Unwrapping size as struct")
        size_expr = F.coalesce(
            F.col("size_raw.double"),
            F.col("size_raw.long").cast("double")
        )
    else:
        # It's already a primitive, cast to double for consistency
        print("[DEBUG] Using size as primitive, casting to double")
        size_expr = F.col("size_raw").cast("double")

    # Apply the transformation
    result_df = result_df.withColumn("size", size_expr).drop("size_raw")

    # Debug: Check if ownerName is populated
    sample_owner = result_df.select(LISTING_ID_COL, "ownerName").limit(3).collect()
    for row in sample_owner:
        print(f"[EXTRACT_ATTRIBUTES] listing_id={row[LISTING_ID_COL]}, ownerName={row['ownerName']}")

    # Debug: Check street struct extraction
    sample_street = result_df.select(LISTING_ID_COL, "street").filter(F.col("street").isNotNull()).limit(3).collect()
    for row in sample_street:
        street_obj = row['street']
        if street_obj:
            print(f"[EXTRACT_ATTRIBUTES] listing_id={row[LISTING_ID_COL]}, street.width={street_obj.width}, street.direction={street_obj.direction}")
        else:
            print(f"[EXTRACT_ATTRIBUTES] listing_id={row[LISTING_ID_COL]}, street=None")

    return result_df

def _extract_description_fields_v2(desc_df):
    """Extract description segment fields"""
    from pyspark.sql import functions as F

    return desc_df.select(
        LISTING_ID_COL,
        F.col("description.en").alias("description_en"),
        F.col("description.ar").alias("description_ar"),
    )


def _extract_price_fields_v2(price_df):
    """Extract price segment fields"""
    from pyspark.sql import functions as F

    # Helper function to safely check if a nested field exists
    def has_nested_field(df, field_path):
        try:
            df.select(field_path).first()
            return True
        except:
            return False

    # Detect if data has 'struct' wrapper or fields are directly under 'data'
    has_struct_wrapper = has_nested_field(price_df, "data.struct")
    data_prefix = "data.struct" if has_struct_wrapper else "data"

    # Check if data is a MapType (from DynamoDB) or StructType
    from pyspark.sql.types import MapType
    data_type = price_df.schema["data"].dataType if "data" in price_df.columns else None
    is_map_type = isinstance(data_type, MapType)

    # Debug: Log data type and sample
    print(f"[EXTRACT_PRICE] data_type: {data_type}, is_map_type: {is_map_type}, data_prefix: {data_prefix}")
    print(f"[EXTRACT_PRICE] Full schema: {price_df.schema}")
    sample_data = price_df.select("listing_id", "data").limit(2).collect()
    for row in sample_data:
        print(f"[EXTRACT_PRICE] Sample - listing_id: {row['listing_id']}, data: {row['data']}")
        if row['data']:
            print(f"[EXTRACT_PRICE] data type: {type(row['data'])}, data dict: {row['data'].asDict() if hasattr(row['data'], 'asDict') else row['data']}")

    # Helper to safely get field or return None
    def safe_col(field_path, alias_name, cast_type="string"):
        if is_map_type and field_path.startswith(data_prefix + "."):
            # For MapType, use getItem instead of dot notation
            field_name = field_path.replace(data_prefix + ".", "")
            return F.col("data").getItem(field_name).cast(cast_type).alias(alias_name)
        elif has_nested_field(price_df, field_path):
            return F.col(field_path).alias(alias_name)
        else:
            return F.lit(None).cast(cast_type).alias(alias_name)

    return price_df.select(
        LISTING_ID_COL,

        # Price type - handle both MapType and StructType
        F.coalesce(
            F.col("price_type") if "price_type" in price_df.columns else F.lit(None),
            F.col("data").getItem("type") if is_map_type else (F.col(f"{data_prefix}.type") if has_nested_field(price_df, f"{data_prefix}.type") else F.lit(None))
        ).alias("price_type"),

        # amounts.sale/sell - handle both MapType and StructType, and both field names (sale/sell)
        # Source uses "sale", destination uses "sell"
        (F.coalesce(
            F.when(
                F.col("data").getItem("amounts").getItem("sale").isNotNull(),
                F.col("data").getItem("amounts").getItem("sale").cast("string")
            ) if is_map_type else (
                F.when(
                    F.col(f"{data_prefix}.amounts.sale").isNotNull(),
                    F.col(f"{data_prefix}.amounts.sale").cast("string")
                ) if has_nested_field(price_df, f"{data_prefix}.amounts.sale") else F.lit(None)
            ),
            F.when(
                F.col("data").getItem("amounts").getItem("sell").isNotNull(),
                F.col("data").getItem("amounts").getItem("sell").cast("string")
            ) if is_map_type else (
                F.when(
                    F.col(f"{data_prefix}.amounts.sell").isNotNull(),
                    F.col(f"{data_prefix}.amounts.sell").cast("string")
                ) if has_nested_field(price_df, f"{data_prefix}.amounts.sell") else F.lit(None)
            )
        )).alias("price_amount_sell"),

        # amounts.monthly - handle both MapType and StructType
        (F.when(
            F.col("data").getItem("amounts").getItem("monthly").isNotNull(),
            F.col("data").getItem("amounts").getItem("monthly").cast("string")
        ) if is_map_type else (
            F.when(
                F.col(f"{data_prefix}.amounts.monthly").isNotNull(),
                F.col(f"{data_prefix}.amounts.monthly").cast("string")
            ) if has_nested_field(price_df, f"{data_prefix}.amounts.monthly") else F.lit(None)
        )).alias("price_amount_monthly"),

        # amounts.yearly - handle both MapType and StructType
        (F.when(
            F.col("data").getItem("amounts").getItem("yearly").isNotNull(),
            F.col("data").getItem("amounts").getItem("yearly").cast("string")
        ) if is_map_type else (
            F.when(
                F.col(f"{data_prefix}.amounts.yearly").isNotNull(),
                F.col(f"{data_prefix}.amounts.yearly").cast("string")
            ) if has_nested_field(price_df, f"{data_prefix}.amounts.yearly") else F.lit(None)
        )).alias("price_amount_yearly"),

        # Downpayment - already BIGINT, default to 0 if not present
        F.coalesce(
            safe_col(f"{data_prefix}.downpayment", "price_downpayment", "long"),
            F.lit(0).cast("long")
        ).alias("price_downpayment"),

        # Number of mortgage years - already BIGINT
        safe_col(f"{data_prefix}.number_of_mortgage_years", "price_number_years", "long"),

        # Number of cheques - INT
        safe_col(f"{data_prefix}.number_of_cheques", "price_number_of_cheques", "int"),

        # Payment methods - already stringified during DataFrame creation
        # Just extract it if it exists
        safe_col(f"{data_prefix}.payment_methods", "price_payment_methods_json"),

        # Utilities inclusive - BOOLEAN
        safe_col(f"{data_prefix}.utilities_inclusive", "price_utilities_inclusive", "boolean"),

        # On request - BOOLEAN
        safe_col(f"{data_prefix}.on_request", "price_on_request", "boolean"),

        # FilterAmount - DECIMAL/NUMBER (used for filtering)
        safe_col(f"{data_prefix}.FilterAmount", "price_filter_amount", "decimal(20,2)"),

        # SortAmount - DECIMAL/NUMBER (used for sorting)
        safe_col(f"{data_prefix}.SortAmount", "price_sort_amount", "decimal(20,2)"),

        # Obligation - struct with comment and enabled (optional field, only in some countries like SA)
        # Skip this field for now - will be added later if it exists
        F.lit(None).cast("struct<comment:string,enabled:boolean>").alias("price_obligation"),

        # Mortgage - struct with comment and enabled (optional field)
        F.lit(None).cast("struct<comment:string,enabled:boolean>").alias("price_mortgage"),

        # Value affected - struct with comment and enabled (optional field)
        F.lit(None).cast("struct<comment:string,enabled:boolean>").alias("price_value_affected"),
    )


def _extract_amenities_fields_v2(amen_df):
    """Extract amenities segment fields"""
    from pyspark.sql import functions as F

    # Helper function to safely check if a nested field exists
    def has_nested_field(df, field_path):
        try:
            df.select(field_path).first()
            return True
        except:
            return False

    # After JSON parsing, data is directly an array (not data.array)
    # Check if data is an array or if we need to access data.array
    if has_nested_field(amen_df, "data.array"):
        # Old structure: data.array
        data_col = "data.array"
    else:
        # New structure after JSON parsing: data is directly the array
        data_col = "data"

    # Log data field type for debugging
    from pyspark.sql.types import ArrayType, StringType
    if "data" in amen_df.columns:
        data_type = amen_df.schema["data"].dataType
        print(f"[EXTRACT_AMENITIES] data field type = {data_type}, using column: {data_col}")

        # Sample a few records
        sample_data = amen_df.select(LISTING_ID_COL, "data").limit(3).collect()
        for row in sample_data:
            print(f"[EXTRACT_AMENITIES] Sample listing_id={row[LISTING_ID_COL]}, data type={type(row['data'])}, data={row['data']}")

    return amen_df.select(
        LISTING_ID_COL,
        (F.when(
            F.col(data_col).isNotNull() & (F.size(F.col(data_col)) > 0),
            F.col(data_col)
        ) if has_nested_field(amen_df, data_col) else F.lit(None)).alias("amenities_arr")
    )


def _extract_metadata_fields_v2(meta_df):
    """Extract metadata segment fields"""
    from pyspark.sql import functions as F

    # Check which optional columns exist
    has_web_ids = "web_ids" in meta_df.columns
    has_published_at = "published_at" in meta_df.columns
    has_first_published_at = "first_published_at" in meta_df.columns
    has_reference = "reference" in meta_df.columns

    print(f"[EXTRACT_METADATA] Columns in meta_df: {meta_df.columns}")
    print(f"[EXTRACT_METADATA] has_published_at: {has_published_at}")
    print(f"[EXTRACT_METADATA] has_first_published_at: {has_first_published_at}")

    # Sample to check values
    if has_published_at:
        sample = meta_df.select(LISTING_ID_COL, "published_at").limit(3).collect()
        for row in sample:
            print(f"[EXTRACT_METADATA] listing_id={row[LISTING_ID_COL]}, published_at={row['published_at']}")

    select_cols = [
        LISTING_ID_COL,

        # All metadata fields are top-level and already converted to simple types
        F.col("client_id").alias("meta_client_id"),
        F.col("assigned_to_id").alias("meta_assigned_to_id"),
        F.col("broker_id").alias("meta_broker_id"),
        F.col("location_id").alias("meta_location_id"),
        F.col("created_by_id").alias("meta_created_by_id"),
        F.col("updated_by_id").alias("meta_updated_by_id")
    ]

    # Add first_published_at only if it exists (optional field, not all records have it)
    if has_first_published_at:
        select_cols.append(F.col("first_published_at").alias("meta_first_published_at"))
    else:
        select_cols.append(F.lit(None).cast("string").alias("meta_first_published_at"))

    # Add published_at only if it exists (optional field, not all records have it)
    if has_published_at:
        select_cols.append(F.col("published_at").alias("meta_published_at"))
    else:
        select_cols.append(F.lit(None).cast("string").alias("meta_published_at"))

    # Add reference only if it exists (optional field)
    if has_reference:
        select_cols.append(F.col("reference").alias("meta_reference"))
    else:
        select_cols.append(F.lit(None).cast("string").alias("meta_reference"))

    # Add web_ids only if it exists in the schema
    if has_web_ids:
        select_cols.append(F.col("web_ids").alias("meta_web_ids"))
    else:
        select_cols.append(F.lit(None).cast("array<string>").alias("meta_web_ids"))

    return meta_df.select(*select_cols)


def _extract_state_fields_v2(state_df, state_col_name="data"):
    """Extract and normalize state segment fields"""
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, StringType

    # Helper function to safely check if a nested field exists
    def has_nested_field(df, field_path):
        try:
            df.select(field_path).first()
            return True
        except:
            return False

    # Detect if data has 'struct' wrapper or fields are directly under 'data'
    has_struct_wrapper = has_nested_field(state_df, "data.struct")
    data_prefix = "data.struct" if has_struct_wrapper else "data"

    # Draft states list (after mapping has been applied)
    draft_states = [
        "draft",
        "draft_pending_approval",
        "rejected",
        "approved",
        "unpublished",
        "validation_requested",
        "validation_failed",
        "allocation_requested",
        "allocation_failed",
        "takendown_pending_approval"
    ]

    # Determine how to extract reasons based on field type
    reasons_expr = F.lit(None).alias("state_reasons")
    if has_nested_field(state_df, f"{data_prefix}.reasons"):
        # Check if reasons is a string or array
        try:
            field_type = None
            for field in state_df.schema.fields:
                if field.name == "data":
                    if hasattr(field.dataType, 'fields'):
                        for nested in field.dataType.fields:
                            if nested.name == "reasons":
                                field_type = nested.dataType
                            elif nested.name == "struct" and hasattr(nested.dataType, 'fields'):
                                for inner in nested.dataType.fields:
                                    if inner.name == "reasons":
                                        field_type = inner.dataType

            if isinstance(field_type, StringType):
                # String type: check for empty array string "[]"
                # Parse as array of structs with ar/en fields
                from pyspark.sql.types import StructType, StructField
                reasons_schema = ArrayType(StructType([
                    StructField("ar", StringType(), True),
                    StructField("en", StringType(), True)
                ]))
                reasons_expr = (F.when(F.col(f"{data_prefix}.reasons") == "[]", F.lit(None).cast(reasons_schema))
                                 .when(F.col(f"{data_prefix}.reasons").isNull(), F.lit(None).cast(reasons_schema))
                                 .otherwise(F.from_json(F.col(f"{data_prefix}.reasons"), reasons_schema))
                                 .alias("state_reasons"))
            elif isinstance(field_type, ArrayType):
                # Array type: check if it's array<string> that needs parsing or already array<struct>
                from pyspark.sql.types import StructType, StructField
                if isinstance(field_type.elementType, StringType):
                    # Array of strings - need to parse each element as JSON
                    reasons_schema = StructType([
                        StructField("ar", StringType(), True),
                        StructField("en", StringType(), True)
                    ])
                    reasons_expr = (F.when(F.size(F.col(f"{data_prefix}.reasons")) == 0, F.lit(None).cast(ArrayType(reasons_schema)))
                                     .otherwise(F.transform(F.col(f"{data_prefix}.reasons"),
                                                           lambda x: F.from_json(x, reasons_schema)))
                                     .alias("state_reasons"))
                else:
                    # Already array<struct>, just check for empty
                    reasons_expr = (F.when(F.size(F.col(f"{data_prefix}.reasons")) == 0, F.lit(None).cast(field_type))
                                     .otherwise(F.col(f"{data_prefix}.reasons"))
                                     .alias("state_reasons"))
            else:
                # Unknown type, just extract as-is
                reasons_expr = F.col(f"{data_prefix}.reasons").alias("state_reasons")
        except:
            # Fallback: just extract the field
            reasons_expr = F.col(f"{data_prefix}.reasons").alias("state_reasons")

    return state_df.select(
        LISTING_ID_COL,
        # Use the top-level state_type column directly
        F.col("state_type").alias("state_type"),
        # Calculate stage from state_type
        F.when(F.col("state_type").isNotNull(),
            F.when(F.col("state_type").isin(draft_states), F.lit("draft"))
             .when(F.col("state_type").startswith("live"), F.lit("live"))
             .when(F.col("state_type").startswith("takendown"), F.lit("takendown"))
             .when(F.col("state_type").startswith("archived"), F.lit("archived"))
             .otherwise(F.col("state_type"))
        ).alias("state_stage"),
        # Extract reasons with appropriate handling based on type
        reasons_expr
    )


def read_ddb_for_test_listing(table_name, region, listing_id, glueContext, stringify_complex=False):
    """Query DynamoDB by partition key and properly deserialize

    Args:
        stringify_complex: If True, converts complex fields (data) to JSON strings
                          to avoid CANNOT_MERGE_TYPE errors when schema inference
                          encounters heterogeneous types (Array vs Map)
    """
    import boto3
    from boto3.dynamodb.types import TypeDeserializer
    from decimal import Decimal
    import json

    # Use DynamoDB resource (not client) - it auto-deserializes
    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)

    # Query the table - resource API returns deserialized data
    print(f"[QUERY] Querying DynamoDB for listing_id={listing_id}")
    try:
        response = table.query(
            KeyConditionExpression='listing_id = :lid',
            ExpressionAttributeValues={':lid': listing_id}
        )

        items = response['Items']
        print(f"[QUERY] Retrieved {len(items)} items in first page")

        # Handle pagination
        page_count = 1
        while 'LastEvaluatedKey' in response:
            page_count += 1
            response = table.query(
                KeyConditionExpression='listing_id = :lid',
                ExpressionAttributeValues={':lid': listing_id},
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items.extend(response['Items'])
            print(f"[QUERY] Retrieved {len(response['Items'])} items in page {page_count}")

        print(f"[QUERY] Total items retrieved: {len(items)} across {page_count} page(s)")
    except AttributeError as e:
        print(f"[ERROR] boto3 query failed for listing_id={listing_id}: {e}")
        print(f"[ERROR] This usually means malformed data in DynamoDB")
        raise

    if not items:
        print(f"[QUERY] No items found for listing_id={listing_id}, returning empty DataFrame")
        # Return empty DataFrame
        spark = glueContext.spark_session
        from pyspark.sql.types import StructType, StructField, StringType
        empty_schema = StructType([
            StructField("listing_id", StringType(), True),
            StructField("segment", StringType(), True)
        ])
        return spark.createDataFrame([], schema=empty_schema)

    # Convert Decimal to float/int for Spark compatibility
    def convert_decimals(obj):
        if obj is None:
            return None
        elif isinstance(obj, list):
            return [convert_decimals(item) for item in obj]
        elif isinstance(obj, dict):
            result = {}
            for k, v in obj.items():
                converted = convert_decimals(v)
                # Keep the key even if value is None
                result[k] = converted
            return result
        elif isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        else:
            return obj

    # Deserialize and convert
    print(f"[CONVERT] Converting {len(items)} items (Decimals -> float/int)")
    try:
        clean_items = [convert_decimals(item) for item in items]
        print(f"[CONVERT] Successfully converted {len(clean_items)} items")

        # Log sample item structure for debugging
        if clean_items:
            sample = clean_items[0]
            print(f"[CONVERT] Sample item keys: {list(sample.keys()) if isinstance(sample, dict) else 'NOT A DICT'}")
            print(f"[CONVERT] Sample item type: {type(sample)}")
    except AttributeError as e:
        print(f"[ERROR] convert_decimals failed: {e}")
        print(f"[ERROR] Items causing issue: {items[:5]}")  # Print first 5 items for debugging
        raise

    # Stringify complex fields if requested (avoids CANNOT_MERGE_TYPE)
    if stringify_complex:
        print(f"[STRINGIFY] Stringifying complex fields for {len(clean_items)} items")
        stringified_count = 0
        valid_items = []

        # Helper to convert Binary to base64 string
        import base64
        def convert_binary_to_base64(obj):
            if isinstance(obj, bytes):
                return base64.b64encode(obj).decode('utf-8')
            elif isinstance(obj, dict):
                return {k: convert_binary_to_base64(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_binary_to_base64(item) for item in obj]
            else:
                return obj

        for idx, it in enumerate(clean_items):
            # Safety check: ensure it is a dict
            if it is None:
                print(f"[WARN] Item {idx} is None, removing from list")
                continue
            if not isinstance(it, dict):
                print(f"[WARN] Item {idx} is not a dict: {type(it)}, removing from list")
                continue
            # Stringify ALL complex fields (including 'data') to avoid schema conflicts during DataFrame creation
            # We'll parse 'data' back to struct after DataFrame is created and filtered by segment
            for key, value in list(it.items()):
                if value is not None and not isinstance(value, (str, int, float, bool)):
                    try:
                        # Convert Binary to base64 before JSON serialization
                        value_converted = convert_binary_to_base64(value)
                        it[key] = json.dumps(value_converted)
                        stringified_count += 1
                    except (TypeError, AttributeError) as e:
#                         print(f"[WARN] Failed to stringify {key} field for item {idx}: {e}")
                        it[key] = None
            valid_items.append(it)

        clean_items = valid_items
        print(f"[STRINGIFY] Stringified {stringified_count} data fields, kept {len(clean_items)} valid items")

    # Recursive function to remove None values from nested structures
    def remove_none_recursive(obj):
        if obj is None:
            return None
        elif isinstance(obj, dict):
            result = {}
            for k, v in obj.items():
                cleaned_v = remove_none_recursive(v)
                if cleaned_v is not None:
                    result[k] = cleaned_v
            return result  # Return dict/list even if empty
        elif isinstance(obj, list):
            result = []
            for item in obj:
                cleaned_item = remove_none_recursive(item)
                if cleaned_item is not None:
                    result.append(cleaned_item)
            return result  # Return dict/list even if empty
        else:
            return obj

    # Create DataFrame
    print(f"[DATAFRAME] Creating Spark DataFrame from {len(clean_items)} items")
    # Log segments for debugging
    segments = set()
    for item in clean_items[:10]:  # Check first 10 items
        if isinstance(item, dict) and 'segment' in item:
            segments.add(item.get('segment'))
    print(f"[DATAFRAME] Sample segments: {segments}")
    spark = glueContext.spark_session

    if not clean_items:
        print(f"[DATAFRAME] No valid items, returning empty DataFrame")
        from pyspark.sql.types import StructType, StructField, StringType
        empty_schema = StructType([
            StructField("listing_id", StringType(), True),
            StructField("segment", StringType(), True)
        ])
        return spark.createDataFrame([], schema=empty_schema)

    # Final safety check: remove any items that are None or not dict, and recursively remove None values
    final_items = []
    for item in clean_items:
        if item is None or not isinstance(item, dict):
            continue
        # Recursively remove None values from within the dict (Spark can't handle None in nested structures)
        cleaned_item = remove_none_recursive(item)
        if cleaned_item:  # Only add if there are any non-None values
            final_items.append(cleaned_item)

    if len(final_items) < len(clean_items):
        print(f"[DATAFRAME] Filtered out {len(clean_items) - len(final_items)} invalid items")

    if not final_items:
        print(f"[DATAFRAME] No valid items after filtering, returning empty DataFrame")
        from pyspark.sql.types import StructType, StructField, StringType
        empty_schema = StructType([
            StructField("listing_id", StringType(), True),
            StructField("segment", StringType(), True)
        ])
        return spark.createDataFrame([], schema=empty_schema)

    try:
        df = spark.createDataFrame(final_items)
        print(f"[DATAFRAME] Created DataFrame with {df.count()} rows and {len(df.columns)} columns")
        print(f"[DATAFRAME] Columns: {df.columns}")
        return df
    except Exception as e:
        print(f"[ERROR] Failed to create DataFrame: {e}")
        print(f"[ERROR] Sample of items causing issue:")
        for i, item in enumerate(final_items[:3]):
            print(f"[ERROR] Item {i}: {type(item)}, keys: {list(item.keys()) if isinstance(item, dict) else 'NOT A DICT'}")
            if isinstance(item, dict):
                for k, v in list(item.items())[:5]:
                    print(f"[ERROR]   {k}: {type(v)}")
                    # If it's a dict, show its contents
                    if isinstance(v, dict):
                        print(f"[ERROR]     Contents: {v}")
        raise


def read_ddb_for_test_listings(table_name, region, listing_ids, glueContext, stringify_complex=False):
    """Query DynamoDB for multiple listings using batch get

    Args:
        stringify_complex: If True, converts complex fields (data) to JSON strings
                          to avoid CANNOT_MERGE_TYPE errors when schema inference
                          encounters heterogeneous types (Array vs Map)
    """
    import boto3
    from boto3.dynamodb.types import TypeDeserializer
    from decimal import Decimal
    import json

    dynamodb = boto3.resource('dynamodb', region_name=region)
    table = dynamodb.Table(table_name)

    all_items = []

    # Use parallel queries to retrieve all segments for each listing (faster than sequential)
    print(f"[QUERY_PARALLEL] Retrieving all segments for {len(listing_ids)} listing(s) using parallel queries")

    from concurrent.futures import ThreadPoolExecutor, as_completed
    import time

    def query_listing(listing_id):
        """Query all segments for a single listing"""
        items = []
        try:
            response = table.query(
                KeyConditionExpression='listing_id = :lid',
                ExpressionAttributeValues={':lid': listing_id}
            )
            items.extend(response['Items'])

            # Handle pagination
            while 'LastEvaluatedKey' in response:
                response = table.query(
                    KeyConditionExpression='listing_id = :lid',
                    ExpressionAttributeValues={':lid': listing_id},
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                items.extend(response['Items'])

            return listing_id, items, None
        except Exception as e:
            return listing_id, [], str(e)

    try:
        # Use ThreadPoolExecutor for parallel queries (10 workers)
        max_workers = min(10, len(listing_ids))
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(query_listing, lid): lid for lid in listing_ids}

            completed = 0
            for future in as_completed(futures):
                listing_id, items, error = future.result()
                completed += 1

                if error:
                    print(f"[QUERY_PARALLEL] Error querying listing: {error}")
                else:
                    all_items.extend(items)
                    if completed % 100 == 0 or completed == len(listing_ids):
                        print(f"[QUERY_PARALLEL] Progress: {completed}/{len(listing_ids)} listings, {len(all_items)} items retrieved")

        print(f"[QUERY_PARALLEL] Total items retrieved: {len(all_items)} for {len(listing_ids)} listing(s)")
    except Exception as e:
        print(f"[ERROR] Parallel query failed: {e}")
        raise

    if not all_items:
        print(f"[QUERY_MULTI] No items found for any listing, returning empty DataFrame")
        # Return empty DataFrame
        spark = glueContext.spark_session
        from pyspark.sql.types import StructType, StructField, StringType
        empty_schema = StructType([
            StructField("listing_id", StringType(), True),
            StructField("segment", StringType(), True)
        ])
        return spark.createDataFrame([], schema=empty_schema)

    # Convert Decimal to float/int for Spark compatibility
    def convert_decimals(obj):
        if obj is None:
            return None
        elif isinstance(obj, list):
            return [convert_decimals(item) for item in obj]
        elif isinstance(obj, dict):
            result = {}
            for k, v in obj.items():
                converted = convert_decimals(v)
                # Keep the key even if value is None
                result[k] = converted
            return result
        elif isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        else:
            return obj

    print(f"[CONVERT_MULTI] Converting {len(all_items)} items (Decimals -> float/int)")
    try:
        clean_items = [convert_decimals(item) for item in all_items]
        print(f"[CONVERT_MULTI] Successfully converted {len(clean_items)} items")

        # Log sample item structure for debugging
        if clean_items:
            sample = clean_items[0]
            print(f"[CONVERT_MULTI] Sample item keys: {list(sample.keys()) if isinstance(sample, dict) else 'NOT A DICT'}")
            print(f"[CONVERT_MULTI] Sample item type: {type(sample)}")
    except AttributeError as e:
        print(f"[ERROR] convert_decimals failed: {e}")
        print(f"[ERROR] Items causing issue: {all_items[:5]}")  # Print first 5 items for debugging
        raise

    # Stringify complex fields if requested (avoids CANNOT_MERGE_TYPE)
    if stringify_complex:
        print(f"[STRINGIFY_MULTI] Stringifying complex fields for {len(clean_items)} items")
        stringified_count = 0
        valid_items = []

        # Helper to convert Binary to base64 string
        import base64
        def convert_binary_to_base64(obj):
            if isinstance(obj, bytes):
                return base64.b64encode(obj).decode('utf-8')
            elif isinstance(obj, dict):
                return {k: convert_binary_to_base64(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_binary_to_base64(item) for item in obj]
            else:
                return obj

        for idx, it in enumerate(clean_items):
            # Safety check: ensure it is a dict
            if it is None:
                print(f"[WARN] Item {idx} is None, removing from list")
                continue
            if not isinstance(it, dict):
                print(f"[WARN] Item {idx} is not a dict: {type(it)}, removing from list")
                continue
            # Stringify ALL complex fields (including 'data') to avoid schema conflicts during DataFrame creation
            # We'll parse 'data' back to struct after DataFrame is created and filtered by segment
            for key, value in list(it.items()):
                if value is not None and not isinstance(value, (str, int, float, bool)):
                    try:
                        # Convert Binary to base64 before JSON serialization
                        value_converted = convert_binary_to_base64(value)
                        it[key] = json.dumps(value_converted)
                        stringified_count += 1
                    except (TypeError, AttributeError) as e:
#                         print(f"[WARN] Failed to stringify {key} field for item {idx}: {e}")
                        it[key] = None
            valid_items.append(it)

        clean_items = valid_items
        print(f"[STRINGIFY_MULTI] Stringified {stringified_count} data fields, kept {len(clean_items)} valid items")

    # Recursive function to remove None values from nested structures
    def remove_none_recursive(obj):
        if obj is None:
            return None
        elif isinstance(obj, dict):
            result = {}
            for k, v in obj.items():
                cleaned_v = remove_none_recursive(v)
                if cleaned_v is not None:
                    result[k] = cleaned_v
            return result  # Return dict/list even if empty
        elif isinstance(obj, list):
            result = []
            for item in obj:
                cleaned_item = remove_none_recursive(item)
                if cleaned_item is not None:
                    result.append(cleaned_item)
            return result  # Return dict/list even if empty
        else:
            return obj

    # Create DataFrame
    print(f"[DATAFRAME_MULTI] Creating Spark DataFrame from {len(clean_items)} items")
    # Log segments for debugging
    segments = set()
    for item in clean_items[:10]:  # Check first 10 items
        if isinstance(item, dict) and 'segment' in item:
            segments.add(item.get('segment'))
    print(f"[DATAFRAME_MULTI] Sample segments: {segments}")
    spark = glueContext.spark_session

    if not clean_items:
        print(f"[DATAFRAME_MULTI] No valid items, returning empty DataFrame")
        from pyspark.sql.types import StructType, StructField, StringType
        empty_schema = StructType([
            StructField("listing_id", StringType(), True),
            StructField("segment", StringType(), True)
        ])
        return spark.createDataFrame([], schema=empty_schema)

    # Final safety check: remove any items that are None or not dict, and recursively remove None values
    final_items = []
    for item in clean_items:
        if item is None or not isinstance(item, dict):
            continue
        # Recursively remove None values from within the dict (Spark can't handle None in nested structures)
        cleaned_item = remove_none_recursive(item)
        if cleaned_item:  # Only add if there are any non-None values
            final_items.append(cleaned_item)

    if len(final_items) < len(clean_items):
        print(f"[DATAFRAME_MULTI] Filtered out {len(clean_items) - len(final_items)} invalid items")

    if not final_items:
        print(f"[DATAFRAME_MULTI] No valid items after filtering, returning empty DataFrame")
        from pyspark.sql.types import StructType, StructField, StringType
        empty_schema = StructType([
            StructField("listing_id", StringType(), True),
            StructField("segment", StringType(), True)
        ])
        return spark.createDataFrame([], schema=empty_schema)

    try:
        df = spark.createDataFrame(final_items)
        print(f"[DATAFRAME_MULTI] Created DataFrame with {df.count()} rows and {len(df.columns)} columns")
        print(f"[DATAFRAME_MULTI] Columns: {df.columns}")
        return df
    except Exception as e:
        print(f"[ERROR] Failed to create DataFrame: {e}")
        print(f"[ERROR] Sample of items causing issue:")
        for i, item in enumerate(final_items[:3]):
            print(f"[ERROR] Item {i}: {type(item)}, keys: {list(item.keys()) if isinstance(item, dict) else 'NOT A DICT'}")
            if isinstance(item, dict):
                for k, v in list(item.items())[:5]:
                    print(f"[ERROR]   {k}: {type(v)}")
                    # If it's a dict, show its contents
                    if isinstance(v, dict):
                        print(f"[ERROR]     Contents: {v}")
        raise


def _safe_extract_numeric(col_name, df):
    """Check schema type BEFORE attempting field extraction"""
    from pyspark.sql.types import StructType
    from pyspark.sql import functions as F

    if col_name not in df.columns:
        return F.lit(None).cast("long")

    col_type = df.schema[col_name].dataType

    # Only use getField if it's actually a STRUCT
    if isinstance(col_type, StructType):
        field_names = col_type.fieldNames()
        if "long" in field_names:
            return F.col(col_name).getField("long")
        elif "double" in field_names:
            return F.col(col_name).getField("double").cast("long")
        else:
            return F.lit(None).cast("long")
    else:
        # It's already a numeric type (BIGINT)
        return F.col(col_name).cast("long")


def migrate_catalog_segment_to_search_aggregator_v2(
    glueContext,
    country_code,
    source_listings_table,
    source_region,
    target_search_aggregator_table,
    target_region,
    test_listing_ids=[],
    test_client_ids=[],
):
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, StringType, StructType, StructField

    logger = glueContext.get_logger()

    _log(logger, f"[SEARCH_CATALOG] Starting catalog migration - listing_ids={test_listing_ids}, client_ids={test_client_ids}")

    # Convert client IDs to listing IDs if needed (to enable fast path)
    if test_client_ids:
        if test_listing_ids:
            # Already have listing_ids, just clear client_ids
            _log(logger, f"[SEARCH_CATALOG] Listing IDs already provided, clearing client_ids to enable fast path")
            test_client_ids = []
        else:
            # Convert client IDs to listing IDs
            _log(logger, f"[SEARCH_CATALOG] Converting {len(test_client_ids)} client ID(s) to listing IDs using client-id-index")
            test_listing_ids = get_listing_ids_for_clients(
                glueContext, logger, country_code, source_listings_table, source_region, test_client_ids
            )
            _log(logger, f"[SEARCH_CATALOG] Found {len(test_listing_ids)} listing IDs from client IDs")
            # Clear client_ids to enable fast path
            test_client_ids = []

    # Determine if we can use fast path (use whenever we have listing_ids)
    # Fast path avoids CANNOT_MERGE_TYPE errors by reading all segments at once with stringify_complex=True
    can_use_fast_path = (len(test_listing_ids) > 0)
    run_all = (len(test_listing_ids) == 0 and len(test_client_ids) == 0)

    _log(logger, f"[SEARCH_CATALOG] can_use_fast_path={can_use_fast_path}, run_all={run_all}")

    if can_use_fast_path:
        # FAST PATH: Query the table directly by partition key
        _log(logger, f"[SEARCH_CATALOG] Using FAST PATH - querying by partition key for {len(test_listing_ids)} listing(s)")
        if len(test_listing_ids) == 1:
            base = read_ddb_for_test_listing(
                source_listings_table,
                source_region,
                test_listing_ids[0],
                glueContext,
                stringify_complex=True  # Avoid CANNOT_MERGE_TYPE (Array vs Map in data column)
            )
        else:
            base = read_ddb_for_test_listings(
                source_listings_table,
                source_region,
                test_listing_ids,
                glueContext,
                stringify_complex=True  # Avoid CANNOT_MERGE_TYPE (Array vs Map in data column)
            )

        _log(logger, f"[SEARCH_CATALOG] Fast path read {base.count()} records from source")

        if base.rdd.isEmpty():
            _log(logger, "[SEARCH_CATALOG] No records found, exiting")
            return

        # Log which listings we found
        found_listings = base.select(LISTING_ID_COL).distinct().rdd.map(lambda r: r[0]).collect()
        _log(logger, f"[SEARCH_CATALOG] Found {len(found_listings)} unique listings: {found_listings[:10]}" + ("..." if len(found_listings) > 10 else ""))

        # Determine segment column name
        seg_col = SEGMENT_COL if SEGMENT_COL in base.columns else (SK_COL if SK_COL in base.columns else None)
        if not seg_col:
            raise ValueError(f"DataFrame missing '{SEGMENT_COL}' or '{SK_COL}' column")
    else:
        # SLOW PATH: Read METADATA first to avoid CANNOT_MERGE_TYPE error
        # Cannot read full table with all segments because data column has different types
        _log(logger, f"[SEARCH_CATALOG] Using SLOW PATH - reading METADATA segment first")

        # Get listing IDs from client IDs if needed
        listing_ids_to_use = test_listing_ids or []
        if not listing_ids_to_use and test_client_ids:
            _log(logger, f"[SEARCH_CATALOG] Looking up listing IDs for {len(test_client_ids)} client(s)")
            listing_ids_to_use = get_listing_ids_for_clients(
                glueContext, logger, country_code, source_listings_table, source_region, test_client_ids
            )
            _log(logger, f"[SEARCH_CATALOG] Found {len(listing_ids_to_use)} listings for clients")

        # OPTIMIZED: Use BatchGetItem if we have listing IDs
        if not run_all and listing_ids_to_use:
            _log(logger, f"[SEARCH_CATALOG] Using BatchGetItem for {len(listing_ids_to_use)} listing(s) (100x faster)")
            base = read_segment_targeted(glueContext, source_listings_table, source_region, listing_ids_to_use, "METADATA")
        else:
            # Fallback to scan for run_all mode
            _log(logger, f"[SEARCH_CATALOG] Using table scan (run_all mode)")
            meta_filter = {
                "dynamodb.filter.expression": "segment = :seg OR SK = :seg",
                "dynamodb.filter.expressionAttributeValues": '{":seg":{"S":"SEGMENT#METADATA"}}'
            }
            dyf = read_ddb_table(glueContext, source_listings_table, source_region, scan_filter=meta_filter)
            base = dyf.toDF()

        _log(logger, f"[SEARCH_CATALOG] Read {base.count()} METADATA records")

        seg_col = SEGMENT_COL if SEGMENT_COL in base.columns else (SK_COL if SK_COL in base.columns else None)
        if not seg_col:
            raise ValueError(f"{source_listings_table} missing '{SEGMENT_COL}'/'{SK_COL}'")

        # Apply filters (only needed if we used scan)
        if run_all or not listing_ids_to_use:
            base = _apply_test_filters(base, test_listing_ids, test_client_ids, run_all)
            _log(logger, f"[SEARCH_CATALOG] After filters: {base.count()} records")

        # Log which listings we're processing
        found_listings = base.select(LISTING_ID_COL).distinct().rdd.map(lambda r: r[0]).collect()
        _log(logger, f"[SEARCH_CATALOG] Processing {len(found_listings)} unique listings: {found_listings[:10]}" + ("..." if len(found_listings) > 10 else ""))

        # For slow path, we need to read other segments separately
        # We'll use the listing IDs from METADATA to filter
        test_listing_ids = found_listings


    # Read and extract latest rows per segment
    # For fast path: use base (which has all segments)
    # For slow path: read each segment separately with scan filters
    _log(logger, "[SEARCH_CATALOG] Reading and extracting latest version per segment...")

    if can_use_fast_path:
        # Fast path: base has all segments, just filter
        attr  = _latest_per_segment_v2(base, seg_col, "ATTRIBUTES")
        desc  = _latest_per_segment_v2(base, seg_col, "DESCRIPTION")
        price = _latest_per_segment_v2(base, seg_col, "PRICE")
        amen  = _latest_per_segment_v2(base, seg_col, "AMENITIES")
        meta  = _latest_per_segment_v2(base, seg_col, "METADATA")
        state = _latest_per_segment_v2(base, seg_col, "STATE")
    else:
        # Slow path: read each segment separately to avoid CANNOT_MERGE_TYPE
        def read_segment_separately(segment_name):
            # OPTIMIZED: Use BatchGetItem if we have listing IDs
            if test_listing_ids and not run_all:
                _log(logger, f"[SEARCH_CATALOG] Reading {segment_name} using BatchGetItem for {len(test_listing_ids)} listing(s)")
                df_seg = read_segment_targeted(glueContext, source_listings_table, source_region, test_listing_ids, segment_name)
            else:
                # Fallback to scan for run_all mode
                _log(logger, f"[SEARCH_CATALOG] Reading {segment_name} using table scan")
                seg_filter = {
                    "dynamodb.filter.expression": f"{seg_col} = :seg",
                    "dynamodb.filter.expressionAttributeValues": '{":seg":{"S":"SEGMENT#' + segment_name + '"}}'
                }
                dyf_seg = read_ddb_table(glueContext, source_listings_table, source_region, scan_filter=seg_filter)
                df_seg = dyf_seg.toDF()
                # Filter by listing IDs if we have them
                if test_listing_ids:
                    df_seg = df_seg.filter(F.col(LISTING_ID_COL).isin(test_listing_ids))
            return _latest_per_segment_v2(df_seg, seg_col, segment_name)

        attr  = read_segment_separately("ATTRIBUTES")
        desc  = read_segment_separately("DESCRIPTION")
        price = read_segment_separately("PRICE")
        amen  = read_segment_separately("AMENITIES")
        meta  = _latest_per_segment_v2(base, seg_col, "METADATA")  # Already have METADATA from base
        state = read_segment_separately("STATE")

    _log(logger, f"[SEARCH_CATALOG] Segment counts - ATTR:{attr.count()}, DESC:{desc.count()}, PRICE:{price.count()}, AMEN:{amen.count()}, META:{meta.count()}, STATE:{state.count()}")

    # Base columns
    _log(logger, "[SEARCH_CATALOG] Grouping base columns...")

    # Rename client_reference to reference (without country code prefix, unlike catalog)
    if "client_reference" in base.columns:
        base = base.withColumn("reference", F.col("client_reference"))

    # Build list of columns to aggregate - only include columns that exist
    base_cols_to_aggregate = ["client_id", "location_id", "reference", "created_at", "updated_at", "version"]

    # Add first_published_at only if it exists (it's only in METADATA segment)
    if "first_published_at" in base.columns:
        base_cols_to_aggregate.append("first_published_at")

    base_cols = _group_first_non_null_v2(
        base, LISTING_ID_COL,
        base_cols_to_aggregate
    )
    _log(logger, f"[SEARCH_CATALOG] Base columns for {base_cols.count()} listings")

    # ---------- PARSE JSON STRINGS BACK TO STRUCTS ----------
    # Parse stringified complex fields back to their proper types for extraction
    # Each segment has a consistent 'data' type, so we can parse it correctly per segment
    _log(logger, "[SEARCH_CATALOG] Parsing JSON strings back to structs for extraction...")
    from pyspark.sql.types import StringType

    # Parse 'data' field per segment (each segment has consistent data type)
    # Use schema_of_json to infer the correct schema from sample data
    if "data" in attr.columns and isinstance(attr.schema["data"].dataType, StringType):
        # ATTRIBUTES: Infer schema from MULTIPLE samples to capture all fields (e.g., owner_name)
        # Single sample may miss optional fields that aren't present in every record
        import json
        sample_jsons = attr.filter(F.col("data").isNotNull()).select("data").limit(50).collect()
        if sample_jsons:
            # Merge schemas from multiple samples
            merged_schema = {}
            for row in sample_jsons:
                if row[0]:
                    try:
                        obj = json.loads(row[0])
                        if isinstance(obj, dict):
                            # Merge keys - keep any non-null value as example
                            for k, v in obj.items():
                                if k not in merged_schema or merged_schema[k] is None:
                                    merged_schema[k] = v
                    except:
                        pass

            if merged_schema:
                merged_json = json.dumps(merged_schema)
                inferred_schema = F.schema_of_json(F.lit(merged_json))
                _log(logger, f"[SEARCH_CATALOG] ATTRIBUTES data schema inferred from {len(sample_jsons)} samples")
                _log(logger, f"[SEARCH_CATALOG] ATTRIBUTES merged schema keys: {list(merged_schema.keys())}")
                attr = attr.withColumn("data", F.from_json(F.col("data"), inferred_schema))

    if "description" in desc.columns and isinstance(desc.schema["description"].dataType, StringType):
        desc = desc.withColumn("description", F.from_json(F.col("description"), "map<string,string>"))

    if "data" in price.columns and isinstance(price.schema["data"].dataType, StringType):
        # PRICE: Use explicit schema to ensure all amounts fields are captured
        from pyspark.sql.types import StructType, StructField, StringType, LongType, BooleanType, ArrayType
        price_schema = StructType([
            StructField("FilterAmount", LongType(), True),
            StructField("SortAmount", LongType(), True),
            StructField("amount", LongType(), True),
            StructField("amounts", StructType([
                StructField("sale", LongType(), True),
                StructField("sell", LongType(), True),
                StructField("monthly", LongType(), True),
                StructField("yearly", LongType(), True),
            ]), True),
            StructField("downpayment", LongType(), True),
            StructField("minimal_rental_period", StringType(), True),
            StructField("mortgage", StringType(), True),
            StructField("number_of_cheques", LongType(), True),
            StructField("number_of_mortgage_years", LongType(), True),
            StructField("obligation", StringType(), True),
            StructField("on_request", BooleanType(), True),
            StructField("payment_methods", ArrayType(StringType(), True), True),
            StructField("type", StringType(), True),
            StructField("utilities_inclusive", BooleanType(), True),
            StructField("value_affected", StringType(), True),
        ])
        _log(logger, f"[SEARCH_CATALOG] PRICE data parsing with explicit schema including all amounts fields")
        price = price.withColumn("data", F.from_json(F.col("data"), price_schema))

    if "data" in amen.columns and isinstance(amen.schema["data"].dataType, StringType):
        # AMENITIES: data is an array of strings
        amen = amen.withColumn("data", F.from_json(F.col("data"), "array<string>"))

    if "hashes" in meta.columns and isinstance(meta.schema["hashes"].dataType, StringType):
        meta = meta.withColumn("hashes", F.from_json(F.col("hashes"), "map<string,string>"))

    if "web_ids" in meta.columns and isinstance(meta.schema["web_ids"].dataType, StringType):
        meta = meta.withColumn("web_ids", F.from_json(F.col("web_ids"), "array<string>"))

    if "data" in state.columns and isinstance(state.schema["data"].dataType, StringType):
        # STATE: Infer schema from first non-null value
        sample_json = state.filter(F.col("data").isNotNull()).select("data").first()
        if sample_json and sample_json[0]:
            inferred_schema = F.schema_of_json(sample_json[0])
            state = state.withColumn("data", F.from_json(F.col("data"), inferred_schema))

    # ---------- EXTRACT FIELDS USING HELPER FUNCTIONS ----------
    _log(logger, "[SEARCH_CATALOG] Extracting fields from each segment...")
    attr_sel = _extract_attr_fields_v2(attr)
    desc_sel = _extract_description_fields_v2(desc)
    price_sel = _extract_price_fields_v2(price)

    # Extract obligation field if it exists in the price data schema
    if "data" in price.columns:
        from pyspark.sql.types import StructType, StringType
        data_type = price.schema["data"].dataType
        if isinstance(data_type, StructType):
            # Check if obligation field exists in the struct
            field_names = {field.name: field.dataType for field in data_type.fields}
            if "obligation" in field_names:
                _log(logger, "[SEARCH_CATALOG] Obligation field found in price data, extracting...")
                obligation_type = field_names["obligation"]

                if isinstance(obligation_type, StructType):
                    # Obligation is already a struct, extract directly
                    price_sel = price_sel.join(
                        price.select(
                            LISTING_ID_COL,
                            F.when(
                                F.col("data.obligation").isNotNull(),
                                F.struct(
                                    F.col("data.obligation.comment").alias("comment"),
                                    F.col("data.obligation.enabled").alias("enabled")
                                )
                            ).alias("price_obligation_extracted")
                        ),
                        LISTING_ID_COL,
                        "left"
                    )
                elif isinstance(obligation_type, StringType):
                    # Obligation is a JSON string, parse it first
                    _log(logger, "[SEARCH_CATALOG] Obligation is StringType, parsing JSON...")
                    price_sel = price_sel.join(
                        price.select(
                            LISTING_ID_COL,
                            F.when(
                                F.col("data.obligation").isNotNull(),
                                F.from_json(
                                    F.col("data.obligation"),
                                    "struct<comment:string,enabled:boolean>"
                                )
                            ).alias("price_obligation_extracted")
                        ),
                        LISTING_ID_COL,
                        "left"
                    )

                # Replace the null obligation with the extracted one
                price_sel = price_sel.withColumn(
                    "price_obligation",
                    F.coalesce(F.col("price_obligation_extracted"), F.col("price_obligation"))
                ).drop("price_obligation_extracted")

            # Extract mortgage field if it exists
            if "mortgage" in field_names:
                mortgage_type = field_names["mortgage"]
                _log(logger, f"[SEARCH_CATALOG] Mortgage field found in price data, type: {mortgage_type}")

                if isinstance(mortgage_type, StructType):
                    price_sel = price_sel.join(
                        price.select(
                            LISTING_ID_COL,
                            F.when(
                                F.col("data.mortgage").isNotNull(),
                                F.struct(
                                    F.col("data.mortgage.comment").alias("comment"),
                                    F.col("data.mortgage.enabled").alias("enabled")
                                )
                            ).alias("price_mortgage_extracted")
                        ),
                        LISTING_ID_COL,
                        "left"
                    )
                    price_sel = price_sel.withColumn(
                        "price_mortgage",
                        F.coalesce(F.col("price_mortgage_extracted"), F.col("price_mortgage"))
                    ).drop("price_mortgage_extracted")

            # Extract value_affected field if it exists
            if "value_affected" in field_names:
                value_affected_type = field_names["value_affected"]
                _log(logger, f"[SEARCH_CATALOG] Value_affected field found in price data, type: {value_affected_type}")

                if isinstance(value_affected_type, StructType):
                    price_sel = price_sel.join(
                        price.select(
                            LISTING_ID_COL,
                            F.when(
                                F.col("data.value_affected").isNotNull(),
                                F.struct(
                                    F.col("data.value_affected.comment").alias("comment"),
                                    F.col("data.value_affected.enabled").alias("enabled")
                                )
                            ).alias("price_value_affected_extracted")
                        ),
                        LISTING_ID_COL,
                        "left"
                    )
                    price_sel = price_sel.withColumn(
                        "price_value_affected",
                        F.coalesce(F.col("price_value_affected_extracted"), F.col("price_value_affected"))
                    ).drop("price_value_affected_extracted")

    # Debug: Log price extraction results
    _log(logger, "[SEARCH_CATALOG] DEBUG: Checking price amounts extraction...")
    price_sample = price_sel.select("listing_id", "price_amount_sell", "price_amount_monthly", "price_amount_yearly").limit(5).collect()
    for row in price_sample:
        _log(logger, f"[SEARCH_CATALOG] DEBUG: listing_id={row['listing_id']}, sell={row['price_amount_sell']}, monthly={row['price_amount_monthly']}, yearly={row['price_amount_yearly']}")

    amen_sel = _extract_amenities_fields_v2(amen)
    meta_sel = _extract_metadata_fields_v2(meta)
    state_sel = _extract_state_fields_v2(state, STATE_DATA_COL)
    _log(logger, "[SEARCH_CATALOG] Field extraction complete")

    # ---------- JOIN ----------
    _log(logger, "[SEARCH_CATALOG] Joining all segments...")

    # Debug: Check if street exists in base_cols before join
    _log(logger, f"[SEARCH_CATALOG] DEBUG: base_cols columns: {base_cols.columns}")
    _log(logger, f"[SEARCH_CATALOG] DEBUG: attr_sel columns: {attr_sel.columns}")
    _log(logger, f"[SEARCH_CATALOG] DEBUG: 'street' in base_cols: {'street' in base_cols.columns}")
    _log(logger, f"[SEARCH_CATALOG] DEBUG: 'street' in attr_sel: {'street' in attr_sel.columns}")

    df = (base_cols
          .join(attr_sel,  LISTING_ID_COL, "left")
          .join(desc_sel,  LISTING_ID_COL, "left")
          .join(price_sel, LISTING_ID_COL, "left")
          .join(amen_sel,  LISTING_ID_COL, "left")
          .join(meta_sel,  LISTING_ID_COL, "left")
          .join(state_sel, LISTING_ID_COL, "left"))
    _log(logger, f"[SEARCH_CATALOG] Joined dataframe has {df.count()} rows")

    # Debug: Check street struct after join
    _log(logger, "[SEARCH_CATALOG] DEBUG: Checking street struct after join...")
    street_sample = df.select(LISTING_ID_COL, "street").filter(F.col("street").isNotNull()).limit(3).collect()
    for row in street_sample:
        street_obj = row['street']
        if street_obj:
            _log(logger, f"[SEARCH_CATALOG] DEBUG: listing_id={row[LISTING_ID_COL]}, street.width={street_obj.width}, street.direction={street_obj.direction}")
        else:
            _log(logger, f"[SEARCH_CATALOG] DEBUG: listing_id={row[LISTING_ID_COL]}, street=None")

    # ---------- NORMALIZATION ----------
    _log(logger, "[SEARCH_CATALOG] Building normalized data structure...")
    amenities_arr = F.col("amenities_arr")

    # Payment methods - handle both string (JSON) and array types
    # Check if it's already an array or needs parsing
    from pyspark.sql.types import ArrayType, StringType
    pm_col_type = None
    if "price_payment_methods_json" in df.columns:
        pm_col_type = df.schema["price_payment_methods_json"].dataType

    if isinstance(pm_col_type, ArrayType):
        # Already an array, use directly
        pm_arr = F.col("price_payment_methods_json")
    elif isinstance(pm_col_type, StringType):
        # String type, parse as JSON
        pm_arr = F.from_json(F.col("price_payment_methods_json"), "array<string>")
    else:
        # Column doesn't exist or is null, use null
        pm_arr = F.lit(None).cast("array<string>")

    df = (df
        .withColumn("country", F.lit(str(country_code).lower()))
        .withColumn("id", F.col(LISTING_ID_COL))
        .withColumn("availableFrom",
                    F.when(F.col("availableFrom").isNotNull(),
                           F.date_format(F.to_timestamp("availableFrom"), "yyyy-MM-dd")))

        # Numeric fields - extract value from struct or use direct value
        .withColumn("parkingSlots",
                    F.when(F.col("parkingSlots").isNotNull(),
                           F.col("parkingSlots").cast("int")))

        # Size is already unwrapped in _extract_attr_fields_v2 and cast to double, keep as double/float
        .withColumn("size",
                    F.when(F.col("size").isNotNull(),
                           F.col("size").cast("double")))

        # numberOfFloors - preserve null values explicitly
        .withColumn("numberOfFloors",
                    F.when(F.col("numberOfFloors").isNotNull(),
                           F.col("numberOfFloors").cast("int")).otherwise(F.lit(None).cast("int")))

        # plotSize - preserve null values explicitly
        .withColumn("plotSize",
                    F.when(F.col("plotSize").isNotNull(),
                           _safe_extract_numeric("plotSize", df).cast("int")).otherwise(F.lit(None).cast("int")))

        .withColumn("age",
                    F.when(F.col("age").isNotNull(), F.col("age").cast("int")))

        # Boolean fields - explicitly cast to preserve false values
        .withColumn("hasGarden", F.col("hasGarden").cast("boolean"))
        .withColumn("hasKitchen", F.col("hasKitchen").cast("boolean"))
        .withColumn("hasParkingOnSite", F.col("hasParkingOnSite").cast("boolean"))

        .withColumn("state_struct", F.struct(
            F.col("state_stage").alias("stage"),
            F.col("state_type").alias("type"),
            F.when(
                F.col("state_reasons").isNotNull() & (F.size(F.col("state_reasons")) > 0),
                F.col("state_reasons")
            ).alias("reasons")
        ))

        .withColumn("assignedTo_struct",
            F.when(F.col("meta_assigned_to_id").isNotNull(),
                   F.struct(F.col("meta_assigned_to_id").cast("string").alias("id"))))

        .withColumn("createdBy_struct",
            F.when(F.col("meta_created_by_id").isNotNull(),
                   F.struct(F.col("meta_created_by_id").cast("string").alias("id"))))

        .withColumn("updatedBy_struct",
            F.when(F.col("meta_updated_by_id").isNotNull(),
                   F.struct(F.col("meta_updated_by_id").cast("string").alias("id"))))

        .withColumn("client_struct", F.struct(F.col("client_id").cast("string").alias("id")))
        .withColumn("location_struct", F.struct(F.col("location_id").cast("string").alias("id")))
        .withColumn("title_struct", F.struct(F.col("title_ar").alias("ar"), F.col("title_en").alias("en")))
        .withColumn("desc_struct", F.struct(F.col("description_ar").alias("ar"), F.col("description_en").alias("en")))

        # Price struct with all fields
        .withColumn("price_struct", F.struct(
            F.col("price_type").alias("type"),
            F.struct(
                F.col("price_amount_sell").alias("sale"),
                F.col("price_amount_monthly").alias("monthly"),
                F.col("price_amount_yearly").alias("yearly"),
            ).alias("amounts"),
            F.coalesce(F.col("price_downpayment"), F.lit(0).cast("long")).alias("downpayment"),
            F.when(pm_arr.isNotNull(), pm_arr).otherwise(F.array()).alias("paymentMethods"),
            F.when(F.col("price_utilities_inclusive").isNotNull(), F.col("price_utilities_inclusive").cast("boolean")).alias("utilitiesInclusive"),
            F.col("price_number_years").alias("numberOfMortgageYears"),
            F.col("price_number_of_cheques").alias("numberOfCheques"),
            F.when(F.col("price_on_request").isNotNull(), F.col("price_on_request").cast("boolean")).alias("onRequest"),
            F.when(F.col("price_filter_amount").isNotNull(), F.col("price_filter_amount")).alias("filter_amount"),
            F.when(F.col("price_sort_amount").isNotNull(), F.col("price_sort_amount")).alias("sort_amount"),
            F.when(F.col("price_obligation").isNotNull(), F.col("price_obligation")).alias("obligation"),
            F.when(F.col("price_mortgage").isNotNull(), F.col("price_mortgage")).alias("mortgage"),
            F.when(F.col("price_value_affected").isNotNull(), F.col("price_value_affected")).alias("valueAffected"),
        ))

        .withColumn("createdAt", F.col("created_at").cast("string"))
        .withColumn("updatedAt", F.col("updated_at").cast("string"))
    )

    # Add publishedAt and firstPublishedAt BEFORE creating data_struct
    if "meta_published_at" in df.columns and "meta_first_published_at" in df.columns:
        df = df.withColumn("publishedAt", F.coalesce(F.col("meta_published_at"), F.col("meta_first_published_at")))
        df = df.withColumn("firstPublishedAt", F.col("meta_first_published_at"))
    elif "meta_published_at" in df.columns:
        df = df.withColumn("publishedAt", F.col("meta_published_at"))
        df = df.withColumn("firstPublishedAt", F.lit(None).cast("string"))
    elif "meta_first_published_at" in df.columns:
        df = df.withColumn("publishedAt", F.col("meta_first_published_at"))
        df = df.withColumn("firstPublishedAt", F.col("meta_first_published_at"))
    else:
        df = df.withColumn("publishedAt", F.lit(None).cast("string"))
        df = df.withColumn("firstPublishedAt", F.lit(None).cast("string"))

    # ---------- DATA STRUCT ----------
    # Debug: Check street column type before creating data_struct
    _log(logger, f"[SEARCH_CATALOG] DEBUG: Checking street column type before data_struct...")
    _log(logger, f"[SEARCH_CATALOG] DEBUG: street column type: {df.schema['street'].dataType}")
    street_before_struct = df.select(LISTING_ID_COL, "street").filter(F.col("street").isNotNull()).limit(2).collect()
    for row in street_before_struct:
        street_obj = row['street']
        if street_obj:
            _log(logger, f"[SEARCH_CATALOG] DEBUG: Before struct - listing_id={row[LISTING_ID_COL]}, street.width={street_obj.width}, street.direction={street_obj.direction}")

    now_z = F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    eventz = F.coalesce(F.col("updatedAt"), now_z)

    data_struct = F.struct(
        F.struct(eventz.alias("event"), now_z.alias("aggregator")).alias("audit"),
        F.col("age").alias("age"),
        F.when(amenities_arr.isNotNull(), amenities_arr).alias("amenities"),
        F.col("assignedTo_struct").alias("assignedTo"),
        F.col("availableFrom"),
        F.col("bathrooms"),
        F.col("bedrooms"),
        F.col("category"),
        F.col("client_struct").alias("client"),
        F.col("createdAt"),
        F.col("createdBy_struct").alias("createdBy"),
        F.col("desc_struct").alias("description"),
        F.col("developer"),
        F.col("finishingType"),
        F.when(F.col("firstPublishedAt").isNotNull(), F.col("firstPublishedAt")).alias("firstPublishedAt"),
        F.col("floorNumber"),
        F.col("furnishingType"),
        F.col("hasGarden"),
        F.col("hasKitchen"),
        F.col("hasParkingOnSite"),
        F.col("id"),
        F.col("landNumber"),
        F.col("location_struct").alias("location"),
        F.when(F.col("numberOfFloors").isNotNull(), F.col("numberOfFloors")).alias("numberOfFloors"),
        F.col("ownerName"),
        F.col("parkingSlots"),
        F.col("plotNumber"),
        F.when(F.col("plotSize").isNotNull(), F.col("plotSize")).alias("plotSize"),
        F.col("price_struct").alias("price"),
        F.col("projectStatus"),
        F.when(F.col("publishedAt").isNotNull(), F.col("publishedAt")).alias("publishedAt"),
        F.col("street"),
        F.coalesce(F.col("meta_reference"), F.col("id")).alias("reference"),
        F.col("size"),
        F.col("state_struct").alias("state"),
        F.col("title_struct").alias("title"),
        F.col("type"),
        F.col("uaeEmirate"),
        F.col("unitNumber"),
        F.col("updatedAt"),
        F.col("updatedBy_struct").alias("updatedBy")
    )

    # Add webIds if available
    if "meta_web_ids" in df.columns:
        df = df.withColumn("webIds", F.when(F.col("meta_web_ids").isNotNull(), F.col("meta_web_ids")))
    else:
        df = df.withColumn("webIds", F.lit(None).cast("string"))

    # Extract client IDs for logging BEFORE transformation
    _log(logger, "[SEARCH_CATALOG] Extracting client IDs for logging...")
    client_ids_in_batch = df.select("meta_client_id").distinct().rdd.map(lambda r: r[0] if r[0] else None).collect()
    client_ids_in_batch = [c for c in client_ids_in_batch if c is not None]
    _log(logger, f"[SEARCH_CATALOG] Found {len(client_ids_in_batch)} unique clients in this batch")

    # Debug: Check data_struct before JSON serialization
    _log(logger, "[SEARCH_CATALOG] DEBUG: Checking data_struct before JSON serialization...")
    struct_sample = df.withColumn("data_struct_col", data_struct).select(LISTING_ID_COL, "data_struct_col.street").filter(F.col("data_struct_col.street").isNotNull()).limit(2).collect()
    for row in struct_sample:
        street_obj = row[1]
        if street_obj:
            _log(logger, f"[SEARCH_CATALOG] DEBUG: In data_struct - listing_id={row[LISTING_ID_COL]}, street.width={street_obj.width}, street.direction={street_obj.direction}")

    out = (df
        .withColumn("PK", F.col("id"))
        .withColumn("SK", F.lit("SEGMENT#catalog"))
        .withColumn("country", F.col("country"))
        .withColumn("data", F.to_json(data_struct, {"ignoreNullFields": "true"}))
        .withColumn("status", F.lit("listing.updated"))
        .withColumn("updated_at", now_z)
        .withColumn("version", (F.unix_timestamp(F.current_timestamp()) * 1000000000).cast("bigint"))
        .select("PK", "SK", "country", "data", "status", "updated_at", "version"))

    # Debug: Check final JSON output
    _log(logger, "[SEARCH_CATALOG] DEBUG: Checking final JSON output...")
    json_sample = out.select("PK", "data").filter(F.col("data").contains("street")).limit(1).collect()
    for row in json_sample:
        _log(logger, f"[SEARCH_CATALOG] DEBUG: Final JSON for {row['PK']}: {row['data'][:500]}...")

    # Log which listings are being published
    published_count = out.count()
    published_ids = out.select("PK").distinct().rdd.map(lambda r: r[0]).collect()

    _log(logger, f"[SEARCH_CATALOG] Publishing {published_count} catalog records for {len(published_ids)} listings")
    _log(logger, f"[SEARCH_CATALOG] Listing IDs being published: {published_ids[:10]}" + ("..." if len(published_ids) > 10 else ""))
    _log(logger, f"[SEARCH_CATALOG] Client IDs in this batch: {client_ids_in_batch}")

    dyf_out = to_dynamic_frame(glueContext, out)
    dyf_out = DropNullFields.apply(frame=dyf_out)

    _log(logger, f"[SEARCH_CATALOG] Writing to DynamoDB table: {target_search_aggregator_table}")
    write_to_ddb(glueContext, dyf_out, target_search_aggregator_table, target_region)

    _log(logger, f"[SEARCH_CATALOG] ✓ Successfully published {published_count} catalog records to {target_search_aggregator_table}")


# ========================================
# DELETE LISTINGS FROM SEARCH AGGREGATOR
# ========================================

def delete_listings_from_search_aggregator(
    glueContext,
    country_code,
    target_search_aggregator_table,
    target_region,
    listing_ids_to_delete
):
    """
    Delete specific listings from search aggregator and send listing.deleted events.

    NOTE: Only deletes SEGMENT#catalog records. Does NOT delete SEGMENT#media or other segments.

    Args:
        glueContext: Glue context
        country_code: Country code (e.g., "SA", "BH", "QA")
        target_search_aggregator_table: Target search aggregator table name
        target_region: Target region
        listing_ids_to_delete: List of listing IDs to delete
    """
    import pyspark.sql.functions as F
    from pyspark.sql.types import StructType, StructField, StringType

    logger = glueContext.get_logger()

    if not listing_ids_to_delete:
        _log(logger, "[SEARCH_DELETE] No listing IDs provided for deletion")
        return

    _log(logger, f"[SEARCH_DELETE] Deleting {len(listing_ids_to_delete)} listings from {target_search_aggregator_table}")
    _log(logger, f"[SEARCH_DELETE] Listing IDs to delete: {listing_ids_to_delete}")

    # Create deletion records with listing.deleted status
    now_z = F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss'Z'")

    # Create a DataFrame with the listing IDs to delete
    spark = glueContext.spark_session
    schema = StructType([
        StructField("listing_id", StringType(), False)
    ])

    listing_data = [(lid,) for lid in listing_ids_to_delete]
    df = spark.createDataFrame(listing_data, schema)

    # Create deletion records ONLY for SEGMENT#catalog (not media or other segments)
    deletion_records = (df
        .withColumn("PK", F.col("listing_id"))
        .withColumn("SK", F.lit("SEGMENT#catalog"))
        .withColumn("country", F.lit(str(country_code).lower()))
        .withColumn("data", F.lit("{}"))  # Empty data for deletion
        .withColumn("status", F.lit("listing.updated"))
        .withColumn("updated_at", now_z)
        .withColumn("version", (F.unix_timestamp(F.current_timestamp()) * 1000000000).cast("bigint"))
        .select("PK", "SK", "country", "data", "status", "updated_at", "version"))

    deletion_count = deletion_records.count()
    _log(logger, f"[SEARCH_DELETE] Created {deletion_count} deletion records")

    # Step 1: Write deletion events (listing.deleted) to DynamoDB
    dyf_out = to_dynamic_frame(glueContext, deletion_records)
    dyf_out = DropNullFields.apply(frame=dyf_out)

    _log(logger, f"[SEARCH_DELETE] Step 1/2: Writing listing.deleted events to DynamoDB table: {target_search_aggregator_table}")
    write_to_ddb(glueContext, dyf_out, target_search_aggregator_table, target_region)
    _log(logger, f"[SEARCH_DELETE] ✓ Successfully wrote {deletion_count} listing.deleted events")

    # Step 2: Physically delete the records from DynamoDB
    import boto3
    import time

    _log(logger, f"[SEARCH_DELETE] Step 2/2: Physically deleting records from DynamoDB...")

    ddb = boto3.client("dynamodb", region_name=target_region)

    # Batch delete in chunks of 25 (DynamoDB limit)
    batch_size = 25
    total_deleted = 0

    for i in range(0, len(listing_ids_to_delete), batch_size):
        batch = listing_ids_to_delete[i:i + batch_size]

        # Build delete requests
        delete_requests = []
        for listing_id in batch:
            delete_requests.append({
                'DeleteRequest': {
                    'Key': {
                        'PK': {'S': listing_id},
                        'SK': {'S': 'SEGMENT#catalog'}
                    }
                }
            })

        # Execute batch delete
        try:
            response = ddb.batch_write_item(
                RequestItems={
                    target_search_aggregator_table: delete_requests
                }
            )

            # Handle unprocessed items
            unprocessed = response.get('UnprocessedItems', {})
            retry_count = 0
            while unprocessed and retry_count < 5:
                _log(logger, f"[SEARCH_DELETE] Retrying {len(unprocessed.get(target_search_aggregator_table, []))} unprocessed items...")
                time.sleep(2 ** retry_count)  # Exponential backoff
                response = ddb.batch_write_item(RequestItems=unprocessed)
                unprocessed = response.get('UnprocessedItems', {})
                retry_count += 1

            total_deleted += len(batch)
            _log(logger, f"[SEARCH_DELETE] Deleted batch {i // batch_size + 1}: {len(batch)} records - IDs: {batch}")
            _log(logger, f"[SEARCH_DELETE] Progress: {total_deleted}/{len(listing_ids_to_delete)} records deleted")

        except Exception as e:
            _log(logger, f"[SEARCH_DELETE] ERROR deleting batch: {e}")
            raise

    _log(logger, f"[SEARCH_DELETE] ✓ Successfully physically deleted {total_deleted} records from {target_search_aggregator_table}")
    _log(logger, f"[SEARCH_DELETE] All deleted listing IDs: {listing_ids_to_delete}")


# ========================================
# UPDATED MEDIA MIGRATION (NO NULL SIZES)
# ========================================

def migrate_media_to_search_aggregator_v2(
    glueContext,
    country_code,
    source_listings_table,
    source_region,
    target_search_aggregator_table,
    target_region,
    test_listing_ids=[],
    test_client_ids=[],
    run_all=False
):
    """
    Migrates MEDIA segment to Search Aggregator.
    - Keeps all size variants populated (no nulling)
    - Filters out records > 400KB before writing
    - Returns migration statistics
    """
    import pyspark.sql.functions as F
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, ArrayType, MapType
    )

    logger = glueContext.get_logger()

    # Determine if we can use fast path (only works for listing_ids, not client_ids)
    can_use_fast_path = (len(test_listing_ids) > 0 and len(test_client_ids) == 0)
    use_run_all = (len(test_listing_ids) == 0 and len(test_client_ids) == 0)

    if can_use_fast_path:
        # FAST PATH: Query by partition key
        if len(test_listing_ids) == 1:
            df = read_ddb_for_test_listing(
                source_listings_table,
                source_region,
                test_listing_ids[0],
                glueContext
            )
        else:
            df = read_ddb_for_test_listings(
                source_listings_table,
                source_region,
                test_listing_ids,
                glueContext
            )

        if df.rdd.isEmpty():
            return {"migrated": 0, "oversized_filtered": 0}
    else:
        # SLOW PATH: Full table scan
        dyf = read_ddb_table(glueContext, source_listings_table, source_region)
        df = dyf.toDF()

        # Apply filters
        df = _apply_test_filters(df, test_listing_ids, test_client_ids, use_run_all)

    seg_col_name = _choose_segment_col(df)
    if not seg_col_name:
        raise ValueError(f"{source_listings_table} does not have '{SEGMENT_COL}' or '{SK_COL}'")

    df = df.filter(F.col(seg_col_name) == F.lit("SEGMENT#MEDIA"))

    if df.rdd.isEmpty():
        return {"migrated": 0, "oversized_filtered": 0}

    # --- canonical image schema ---
    size_block_schema = StructType([
        StructField("height", IntegerType(), True),
        StructField("width",  IntegerType(), True),
        StructField("jpg",    StructType([StructField("url", StringType(), True)]), True),
        StructField("webp",   StructType([StructField("url", StringType(), True)]), True),
    ])

    image_struct_schema = StructType([
        StructField("original",    StructType([StructField("url", StringType(), True)]), True),
        StructField("watermarked", StructType([StructField("url", StringType(), True)]), True),
        StructField("source",      StructType([StructField("url", StringType(), True)]), True),
        StructField("sizes", StructType([
            StructField("full",      size_block_schema, True),
            StructField("medium",    size_block_schema, True),
            StructField("mobile",    size_block_schema, True),
            StructField("small",     size_block_schema, True),
            StructField("thumbnail", size_block_schema, True),
        ]), True),
    ])
    images_array_schema = ArrayType(image_struct_schema, True)

    # Extract images from data.struct.images
    images_source = F.col("data.struct.images")

    # Transform images - populate ALL size variants
    images_out = F.when(
        images_source.isNull() | (F.size(images_source) == 0),
        F.from_json(F.lit("[]"), images_array_schema)
    ).otherwise(
        F.transform(
            images_source,
            lambda img: F.when(
                img.isNull() | img.getField("original").isNull() | img.getField("original").getField("url").isNull(),
                F.lit(None).cast(image_struct_schema)
            ).otherwise(
                F.struct(
                    F.struct(img.getField("original").getField("url").alias("url")).alias("original"),
                    F.when(
                        img.getField("watermarked").isNotNull() & img.getField("watermarked").getField("url").isNotNull(),
                        F.struct(img.getField("watermarked").getField("url").alias("url"))
                    ).otherwise(
                        F.struct(img.getField("original").getField("url").alias("url"))
                    ).alias("watermarked"),
                    F.when(
                        img.getField("source").isNotNull() & img.getField("source").getField("url").isNotNull(),
                        F.struct(img.getField("source").getField("url").alias("url"))
                    ).otherwise(
                        F.struct(img.getField("original").getField("url").alias("url"))
                    ).alias("source"),
                    F.struct(
                        # Populate all size variants with watermarked/original URL
                        F.struct(
                            img.getField("original").getField("height").cast("int").alias("height"),
                            img.getField("original").getField("width").cast("int").alias("width"),
                            F.struct(F.coalesce(img.getField("watermarked").getField("url"), img.getField("original").getField("url")).alias("url")).alias("jpg"),
                            F.struct(F.coalesce(img.getField("watermarked").getField("url"), img.getField("original").getField("url")).alias("url")).alias("webp"),
                        ).alias("full"),
                        F.struct(
                            img.getField("original").getField("height").cast("int").alias("height"),
                            img.getField("original").getField("width").cast("int").alias("width"),
                            F.struct(F.coalesce(img.getField("watermarked").getField("url"), img.getField("original").getField("url")).alias("url")).alias("jpg"),
                            F.struct(F.coalesce(img.getField("watermarked").getField("url"), img.getField("original").getField("url")).alias("url")).alias("webp"),
                        ).alias("medium"),
                        F.struct(
                            img.getField("original").getField("height").cast("int").alias("height"),
                            img.getField("original").getField("width").cast("int").alias("width"),
                            F.struct(F.coalesce(img.getField("watermarked").getField("url"), img.getField("original").getField("url")).alias("url")).alias("jpg"),
                            F.struct(F.coalesce(img.getField("watermarked").getField("url"), img.getField("original").getField("url")).alias("url")).alias("webp"),
                        ).alias("mobile"),
                        F.struct(
                            img.getField("original").getField("height").cast("int").alias("height"),
                            img.getField("original").getField("width").cast("int").alias("width"),
                            F.struct(F.coalesce(img.getField("watermarked").getField("url"), img.getField("original").getField("url")).alias("url")).alias("jpg"),
                            F.struct(F.coalesce(img.getField("watermarked").getField("url"), img.getField("original").getField("url")).alias("url")).alias("webp"),
                        ).alias("small"),
                        F.struct(
                            img.getField("original").getField("height").cast("int").alias("height"),
                            img.getField("original").getField("width").cast("int").alias("width"),
                            F.struct(F.coalesce(img.getField("watermarked").getField("url"), img.getField("original").getField("url")).alias("url")).alias("jpg"),
                            F.struct(F.coalesce(img.getField("watermarked").getField("url"), img.getField("original").getField("url")).alias("url")).alias("webp"),
                        ).alias("thumbnail"),
                    ).alias("sizes")
                )
            )
        )
    )

    # Videos
    videos_map_type = MapType(StringType(), StringType(), True)
    videos_out = F.map_from_arrays(
        F.array().cast("array<string>"),
        F.array().cast("array<string>")
    ).cast(videos_map_type)

    # Assemble row
    df = (df
        .withColumn("country",  F.lit(str(country_code).lower()))
        .withColumn("id",       F.col(LISTING_ID_COL))
        .withColumn("client_id",F.col("client_id").cast("string"))
        .withColumn("createdAt",F.col("created_at").cast("string"))
        .withColumn("version_in_data",  F.col("version"))
        .withColumn("images",   images_out)
        .withColumn("videos",   videos_out))

    now_z  = F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    eventz = F.coalesce(F.col("createdAt"), now_z)

    data_struct = F.struct(
        F.struct(eventz.alias("event"), now_z.alias("aggregator"), F.expr("uuid()").alias("trace_id")).alias("audit"),
        F.col("client_id"),
        F.col("country"),
        F.col("createdAt"),
        F.col("id"),
        F.col("images"),
        F.col("version_in_data").alias("version"),
        F.col("videos")
    )

    out = (df
        .withColumn("PK", F.col("id"))
        .withColumn("SK", F.lit("SEGMENT#media"))
        .withColumn("country", F.col("country"))
        .withColumn("data", F.to_json(data_struct))
        .withColumn("status", F.lit("listing.media.updated"))
        .withColumn("updated_at", now_z)
        .withColumn("version", (F.unix_timestamp(F.current_timestamp()) * F.lit(1000000000)).cast("bigint"))
        .select("PK","SK","country","data","status","updated_at","version"))

    # Size filtering - only filter out if > 400KB
    out_with_size = out.withColumn("data_size", F.length(F.col("data")))

    # Log statistics
    size_stats = out_with_size.select(
        F.min("data_size").alias("min_size"),
        F.max("data_size").alias("max_size"),
        F.avg("data_size").alias("avg_size"),
        F.count(F.when(F.col("data_size") > 400000, 1)).alias("oversized_count")
    ).collect()[0]

    # Log oversized items
    oversized_count = size_stats['oversized_count']

    # Filter
    out_filtered = out_with_size.filter(F.col("data_size") <= 400000).drop("data_size")
    rows_to_write = _count_safe(out_filtered)

    if out_filtered.rdd.isEmpty():
        return {"migrated": 0, "oversized_filtered": int(oversized_count)}

    dyf_out = to_dynamic_frame(glueContext, out_filtered)
    dyf_out = DropNullFields.apply(frame=dyf_out)
    write_to_ddb(glueContext, dyf_out, target_search_aggregator_table, target_region)

    return {
        "migrated": int(rows_to_write),
        "oversized_filtered": int(oversized_count)
    }



# ========================================
# SEARCH AGGREGATOR VALIDATION FUNCTIONS
# ========================================

def validate_search_media_migration(
    glueContext,
    country_code,
    source_listings_table,
    source_region,
    target_search_aggregator_table,
    target_region,
    test_listing_ids=[],
    test_client_ids=[],
    run_all=False
):
    """
    Validates MEDIA migration to Search Aggregator.

    Returns a JSON-serializable dict with:
    - source_media_records: count of SEGMENT#MEDIA records in source
    - destination_media_records: count of SEGMENT#media records in destination
    - successfully_migrated: count where source listing_id matches destination PK
    - images_migrated: total count of images in destination
    - oversized_filtered_out: count of records filtered due to size > 400KB
    - validation_timestamp: when validation ran
    """
    import pyspark.sql.functions as F
    from datetime import datetime

    logger = glueContext.get_logger()

    # Read source MEDIA segment
    dyf_src = read_ddb_table(glueContext, source_listings_table, source_region)
    df_src = dyf_src.toDF()

    seg_col = _choose_segment_col(df_src)
    if not seg_col:
        raise ValueError(f"{source_listings_table} missing segment/SK column")

    df_src = df_src.filter(F.col(seg_col) == F.lit("SEGMENT#MEDIA"))

    # Apply filters
    df_src = _apply_test_filters(df_src, test_listing_ids, test_client_ids, run_all)

    source_media_count = df_src.count()

    # Extract source listing_ids
    src_ids = df_src.select(
        F.col(LISTING_ID_COL).alias("src_listing_id")
    ).distinct()

    # Read destination media records
    dyf_dst = read_ddb_table(glueContext, target_search_aggregator_table, target_region)
    df_dst = dyf_dst.toDF()

    # Filter for media segment and country
    df_dst = df_dst.filter(
        (F.col("SK") == F.lit("SEGMENT#media")) &
        (F.col("country") == F.lit(country_code.lower()))
    )

    # Apply same filters to destination
    if not run_all:
        if test_listing_ids:
            df_dst = df_dst.filter(F.col("PK").isin(test_listing_ids))
        if test_client_ids:
            # Extract client_id from data JSON
            df_dst = df_dst.withColumn(
                "client_id",
                F.get_json_object(F.col("data"), "$.client_id")
            ).filter(F.col("client_id").isin(test_client_ids))

    destination_media_count = df_dst.count()

    # Extract destination PKs (which should match listing_ids)
    dst_ids = df_dst.select(
        F.col("PK").alias("dst_pk")
    ).distinct()

    # Join to find successfully migrated records
    migrated_join = src_ids.join(
        dst_ids,
        src_ids["src_listing_id"] == dst_ids["dst_pk"],
        "left"
    )

    successfully_migrated = migrated_join.filter(
        F.col("dst_pk").isNotNull()
    ).count()

    # Count total images in destination
    df_dst_images = df_dst.withColumn(
        "images_count",
        F.size(F.from_json(
            F.get_json_object(F.col("data"), "$.images"),
            "array<struct<original:struct<url:string>>>"
        ))
    )

    total_images = df_dst_images.agg(
        F.sum(F.when(F.col("images_count") > 0, F.col("images_count")).otherwise(0))
    ).collect()[0][0] or 0

    # Calculate oversized (filtered out) = source - destination
    oversized_filtered = source_media_count - destination_media_count

    # Sample of missing records (not migrated)
    missing_sample = []
    if successfully_migrated < source_media_count:
        missing = migrated_join.filter(F.col("dst_pk").isNull()) \
            .select("src_listing_id") \
            .limit(100) \
            .collect()
        missing_sample = [r["src_listing_id"] for r in missing]

    # Calculate percentages
    success_percentage = (successfully_migrated / source_media_count * 100) if source_media_count > 0 else 0.0
    failed_percentage = 100.0 - success_percentage

    result = {
        "segment": "MEDIA",
        "validation_timestamp": datetime.utcnow().isoformat() + "Z",
        "source_media_records": int(source_media_count),
        "destination_media_records": int(destination_media_count),
        "successfully_migrated": int(successfully_migrated),
        "success_percentage": round(success_percentage, 2),
        "failed_percentage": round(failed_percentage, 2),
        "images_migrated": int(total_images),
        "oversized_filtered_out": max(0, int(oversized_filtered)),
        "missing_count": int(source_media_count - successfully_migrated),
        "missing_sample": missing_sample[:10] if missing_sample else []
    }

    return result


def validate_search_catalog_migration(
    glueContext,
    country_code,
    source_listings_table,
    source_region,
    target_search_aggregator_table,
    target_region,
    test_listing_ids=[],
    test_client_ids=[],
    run_all=False
):
    """
    Validates CATALOG migration to Search Aggregator.

    Returns a JSON-serializable dict with:
    - source_catalog_records: count of SEGMENT#METADATA in source
    - destination_catalog_records: count of SEGMENT#catalog in destination
    - successfully_migrated: count where source listing_id matches destination PK
    - validation_timestamp: when validation ran
    """
    import pyspark.sql.functions as F
    from datetime import datetime

    logger = glueContext.get_logger()

    # Read source METADATA segment (which becomes CATALOG in search)
    dyf_src = read_ddb_table(glueContext, source_listings_table, source_region)
    df_src = dyf_src.toDF()

    seg_col = _choose_segment_col(df_src)
    if not seg_col:
        raise ValueError(f"{source_listings_table} missing segment/SK column")

    df_src = df_src.filter(F.col(seg_col) == F.lit("SEGMENT#METADATA"))

    # Apply filters
    df_src = _apply_test_filters(df_src, test_listing_ids, test_client_ids, run_all)

    source_catalog_count = df_src.count()

    # Extract source listing_ids
    src_ids = df_src.select(
        F.col(LISTING_ID_COL).alias("src_listing_id")
    ).distinct()

    # Read destination catalog records
    dyf_dst = read_ddb_table(glueContext, target_search_aggregator_table, target_region)
    df_dst = dyf_dst.toDF()

    # Filter for catalog segment and country
    df_dst = df_dst.filter(
        (F.col("SK") == F.lit("SEGMENT#catalog")) &
        (F.col("country") == F.lit(country_code.lower()))
    )

    # Apply same filters to destination
    if not run_all:
        if test_listing_ids:
            df_dst = df_dst.filter(F.col("PK").isin(test_listing_ids))
        if test_client_ids:
            # Extract client.id from data JSON
            df_dst = df_dst.withColumn(
                "client_id",
                F.get_json_object(F.col("data"), "$.client.id")
            ).filter(F.col("client_id").isin(test_client_ids))

    destination_catalog_count = df_dst.count()

    # Extract destination PKs
    dst_ids = df_dst.select(
        F.col("PK").alias("dst_pk")
    ).distinct()

    # Join to find successfully migrated records
    migrated_join = src_ids.join(
        dst_ids,
        src_ids["src_listing_id"] == dst_ids["dst_pk"],
        "left"
    )

    successfully_migrated = migrated_join.filter(
        F.col("dst_pk").isNotNull()
    ).count()

    # Sample of missing records
    missing_sample = []
    if successfully_migrated < source_catalog_count:
        missing = migrated_join.filter(F.col("dst_pk").isNull()) \
            .select("src_listing_id") \
            .limit(100) \
            .collect()
        missing_sample = [r["src_listing_id"] for r in missing]

    # Calculate percentages
    success_percentage = (successfully_migrated / source_catalog_count * 100) if source_catalog_count > 0 else 0.0
    failed_percentage = 100.0 - success_percentage

    result = {
        "segment": "CATALOG",
        "validation_timestamp": datetime.utcnow().isoformat() + "Z",
        "source_catalog_records": int(source_catalog_count),
        "destination_catalog_records": int(destination_catalog_count),
        "successfully_migrated": int(successfully_migrated),
        "success_percentage": round(success_percentage, 2),
        "failed_percentage": round(failed_percentage, 2),
        "missing_count": int(source_catalog_count - successfully_migrated),
        "missing_sample": missing_sample[:10] if missing_sample else []
    }

    return result


def validate_search_aggregator_comprehensive(
    glueContext,
    country_code,
    source_listings_table,
    source_region,
    target_search_aggregator_table,
    target_region,
    test_listing_ids=None,
    test_client_ids=None,
    run_all=False
):
    """
    Comprehensive validation for Search Aggregator in a SINGLE SCAN.

    Validates:
    1. MEDIA segment: presence + field-level accuracy
    2. CATALOG segment: presence + field-level accuracy
    3. Detects stale records (live updates during migration)
    4. Compares critical fields (state, price, timestamps)

    Returns consolidated report with detailed mismatch analysis.
    """
    import pyspark.sql.functions as F
    from datetime import datetime

    logger = glueContext.get_logger()
    _log(logger, "[SEARCH_VALIDATION] Starting comprehensive validation (single scan)...")

    # ============================================
    # STEP 1: READ SOURCE TABLE (ONCE)
    # ============================================
    _log(logger, "[SEARCH_VALIDATION] Reading source table...")
    dyf_src = read_ddb_table(glueContext, source_listings_table, source_region)
    df_src_all = dyf_src.toDF()

    seg_col = _choose_segment_col(df_src_all)
    if not seg_col:
        raise ValueError(f"{source_listings_table} missing segment/SK column")

    # Filter for test listings if needed
    df_src_all = _apply_test_filters(df_src_all, test_listing_ids or [], test_client_ids or [], run_all)

    # Cache source data (we'll use it for both validations)
    df_src_all = df_src_all.cache()

    # Split into MEDIA and relevant segments for CATALOG
    df_src_media = df_src_all.filter(F.col(seg_col) == F.lit("SEGMENT#MEDIA"))
    df_src_catalog_segments = df_src_all.filter(F.col(seg_col).isin([
        "SEGMENT#METADATA", "SEGMENT#STATE", "SEGMENT#PRICE",
        "SEGMENT#ATTRIBUTES", "SEGMENT#DESCRIPTION", "SEGMENT#AMENITIES"
    ]))

    _log(logger, f"[SEARCH_VALIDATION] Source MEDIA rows: {df_src_media.count()}")
    _log(logger, f"[SEARCH_VALIDATION] Source catalog segments rows: {df_src_catalog_segments.count()}")

    # ============================================
    # STEP 2: READ DESTINATION TABLE (ONCE)
    # ============================================
    _log(logger, "[SEARCH_VALIDATION] Reading destination table...")
    dyf_dst = read_ddb_table(glueContext, target_search_aggregator_table, target_region)
    df_dst_all = dyf_dst.toDF()

    # Filter for country and test listings
    df_dst_all = df_dst_all.filter(F.col("country") == F.lit(country_code.lower()))
    if not run_all:
        if test_listing_ids:
            df_dst_all = df_dst_all.filter(F.col("PK").isin(test_listing_ids))
        if test_client_ids:
            # This requires extracting client_id from JSON - expensive but necessary
            df_dst_all = df_dst_all.withColumn(
                "extracted_client_id",
                F.coalesce(
                    F.get_json_object(F.col("data"), "$.client_id"),
                    F.get_json_object(F.col("data"), "$.client.id")
                )
            ).filter(F.col("extracted_client_id").isin(test_client_ids))

    # Cache destination data
    df_dst_all = df_dst_all.cache()

    # Split by segment
    df_dst_media = df_dst_all.filter(F.col("SK") == F.lit("SEGMENT#media"))
    df_dst_catalog = df_dst_all.filter(F.col("SK") == F.lit("SEGMENT#catalog"))

    _log(logger, f"[SEARCH_VALIDATION] Destination MEDIA rows: {df_dst_media.count()}")
    _log(logger, f"[SEARCH_VALIDATION] Destination CATALOG rows: {df_dst_catalog.count()}")

    # ============================================
    # STEP 3: VALIDATE MEDIA SEGMENT
    # ============================================
    media_validation = _validate_media_segment(
        glueContext, df_src_media, df_dst_media, logger
    )

    # ============================================
    # STEP 4: VALIDATE CATALOG SEGMENT (with field-level checks)
    # ============================================
    catalog_validation = _validate_catalog_segment(
        glueContext, df_src_catalog_segments, df_dst_catalog, logger
    )

    # ============================================
    # STEP 5: CONSOLIDATE RESULTS
    # ============================================
    total_source = media_validation["source_media_records"] + catalog_validation["source_catalog_records"]
    total_dest = media_validation["destination_media_records"] + catalog_validation["destination_catalog_records"]
    total_success = media_validation["successfully_migrated"] + catalog_validation["successfully_migrated"]

    # Field-level accuracy
    total_field_mismatches = (
        media_validation.get("field_level_validation", {}).get("field_mismatches", 0) +
        catalog_validation.get("field_level_validation", {}).get("field_mismatches", 0)
    )

    overall_success_percentage = (total_success / total_source * 100) if total_source > 0 else 0.0

    consolidated_report = {
        "validation_type": "search_aggregator_comprehensive",
        "country_code": country_code,
        "validation_timestamp": datetime.utcnow().isoformat() + "Z",
        "scope": {
            "run_all": run_all,
            "test_listing_ids": test_listing_ids if not run_all else None,
            "test_client_ids": test_client_ids if not run_all else None,
            "listing_count": len(test_listing_ids) if test_listing_ids else 0,
            "client_count": len(test_client_ids) if test_client_ids else 0
        },
        "media_validation": media_validation,
        "catalog_validation": catalog_validation,
        "summary": {
            "total_source_records": total_source,
            "total_destination_records": total_dest,
            "total_successfully_migrated": total_success,
            "overall_success_percentage": round(overall_success_percentage, 2),
            "total_field_mismatches": total_field_mismatches,
            "data_accuracy_percentage": round(
                ((total_success - total_field_mismatches) / total_source * 100) if total_source > 0 else 0.0, 2
            ),
            "total_images_migrated": media_validation.get("images_migrated", 0),
            "total_oversized_filtered": media_validation.get("oversized_filtered_out", 0)
        }
    }

    # Unpersist cached dataframes
    df_src_all.unpersist()
    df_dst_all.unpersist()

    # Write to S3
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")
    report_key = f"search-aggregator-validations/{country_code}/{ts}/validation-report.json"
    _s3_put_json(S3_REPORT_BUCKET, report_key, consolidated_report)

    _log(logger, f"[SEARCH_VALIDATION] Report: s3://{S3_REPORT_BUCKET}/{report_key}")
    _log(logger, f"[SEARCH_VALIDATION] Summary: {consolidated_report['summary']}")

    return consolidated_report


def _validate_media_segment(glueContext, df_src_media, df_dst_media, logger):
    """
    Validates MEDIA segment with field-level comparison.

    Checks:
    - Record presence
    - Image count accuracy
    - Timestamp consistency (detects live updates)
    - Oversized filtering
    """
    import pyspark.sql.functions as F
    from datetime import datetime

    source_count = df_src_media.count()
    destination_count = df_dst_media.count()

    # Prepare source data
    src_media = df_src_media.select(
        F.col(LISTING_ID_COL).alias("src_listing_id"),
        F.coalesce(F.col("updated_at"), F.col("created_at")).alias("src_updated_at"),
        F.size(F.coalesce(F.col("data.struct.images"), F.array())).alias("src_image_count")
    ).distinct()

    # Prepare destination data - extract from JSON
    dst_media = df_dst_media.select(
        F.col("PK").alias("dst_listing_id"),
        F.col("updated_at").alias("dst_updated_at"),
        F.col("data")
    )

    # Extract image count from destination JSON
    dst_media = dst_media.withColumn(
        "dst_image_count",
        F.size(F.from_json(
            F.get_json_object(F.col("data"), "$.images"),
            "array<struct<original:struct<url:string>>>"
        ))
    ).select("dst_listing_id", "dst_updated_at", "dst_image_count")

    # Join source and destination
    joined = src_media.join(
        dst_media,
        src_media["src_listing_id"] == dst_media["dst_listing_id"],
        "left"
    )

    # Presence check
    present_in_dest = joined.filter(F.col("dst_listing_id").isNotNull()).count()

    # Field-level validation (only for records present in destination)
    field_comparison = joined.filter(F.col("dst_listing_id").isNotNull()).select(
        "src_listing_id",
        "src_updated_at",
        "dst_updated_at",
        "src_image_count",
        "dst_image_count",
        # Check if timestamps match
        (F.col("src_updated_at") == F.col("dst_updated_at")).alias("timestamp_match"),
        # Check if image counts match
        (F.col("src_image_count") == F.col("dst_image_count")).alias("image_count_match"),
        # Stale record detection (destination older than source = live update missed)
        (F.col("dst_updated_at") < F.col("src_updated_at")).alias("is_stale")
    )

    # Count mismatches
    total_compared = field_comparison.count()
    timestamp_mismatches = field_comparison.filter(~F.col("timestamp_match")).count()
    image_count_mismatches = field_comparison.filter(~F.col("image_count_match")).count()
    stale_records = field_comparison.filter(F.col("is_stale")).count()

    # Any field mismatch
    field_mismatches = field_comparison.filter(
        ~F.col("timestamp_match") | ~F.col("image_count_match")
    ).count()

    # Collect ALL mismatches (no limit)
    mismatch_sample = []
    if field_mismatches > 0:
        mismatches = field_comparison.filter(
            ~F.col("timestamp_match") | ~F.col("image_count_match")
        ).collect()  # REMOVED .limit()

        mismatch_sample = [{
            "listing_id": r["src_listing_id"],
            "source_updated_at": str(r["src_updated_at"]),
            "dest_updated_at": str(r["dst_updated_at"]),
            "source_image_count": int(r["src_image_count"]) if r["src_image_count"] else 0,
            "dest_image_count": int(r["dst_image_count"]) if r["dst_image_count"] else 0,
            "is_stale": bool(r["is_stale"]),
            "requires_resync": True
        } for r in mismatches]  # ALL mismatches

    # Total images
    total_images = dst_media.agg(F.sum("dst_image_count")).collect()[0][0] or 0

    # Oversized filtering
    oversized_filtered = source_count - destination_count

    success_percentage = (present_in_dest / source_count * 100) if source_count > 0 else 0.0
    accuracy_percentage = ((total_compared - field_mismatches) / total_compared * 100) if total_compared > 0 else 0.0

    return {
        "segment": "MEDIA",
        "validation_timestamp": datetime.utcnow().isoformat() + "Z",
        "source_media_records": int(source_count),
        "destination_media_records": int(destination_count),
        "successfully_migrated": int(present_in_dest),
        "success_percentage": round(success_percentage, 2),
        "images_migrated": int(total_images),
        "oversized_filtered_out": max(0, int(oversized_filtered)),

        # Field-level validation
        "field_level_validation": {
            "total_compared": int(total_compared),
            "field_mismatches": int(field_mismatches),
            "accuracy_percentage": round(accuracy_percentage, 2),
            "mismatch_breakdown": {
                "timestamp_mismatches": int(timestamp_mismatches),
                "image_count_mismatches": int(image_count_mismatches),
                "stale_records": int(stale_records)
            }
        },
        "all_mismatches": mismatch_sample,  # Changed from mismatch_sample to all_mismatches
        "missing_count": int(source_count - present_in_dest)
    }


def _validate_catalog_segment(glueContext, df_src_catalog_segments, df_dst_catalog, logger):
    """
    Validates CATALOG segment with comprehensive field-level comparison.

    Checks:
    - Record presence
    - State type accuracy (with mapping validation)
    - Price accuracy
    - Timestamp consistency
    - Critical field presence
    """
    import pyspark.sql.functions as F
    from datetime import datetime
    from pyspark.sql import Window

    # Get latest version of each segment per listing
    def latest_per_segment(df, seg_name):
        sdf = df.filter(F.col("segment") == F.lit(f"SEGMENT#{seg_name}"))
        if sdf.rdd.isEmpty():
            return None
        sdf = sdf.withColumn("_ts", F.coalesce(F.col("updated_at"), F.col("created_at")).cast("timestamp"))
        w = Window.partitionBy(LISTING_ID_COL).orderBy(F.col("_ts").desc_nulls_last())
        return sdf.withColumn("__rn", F.row_number().over(w)).filter(F.col("__rn") == 1).drop("__rn", "_ts")

    # Rename segment column for consistency
    df_src_catalog_segments = df_src_catalog_segments.withColumnRenamed(
        _choose_segment_col(df_src_catalog_segments), "segment"
    )

    # Extract segments
    metadata = latest_per_segment(df_src_catalog_segments, "METADATA")
    state = latest_per_segment(df_src_catalog_segments, "STATE")
    price = latest_per_segment(df_src_catalog_segments, "PRICE")
    attributes = latest_per_segment(df_src_catalog_segments, "ATTRIBUTES")

    source_count = metadata.count() if metadata else 0

    # Build source comparison frame - KEEP listing_id for joins
    src_comparison = metadata.select(
        F.col(LISTING_ID_COL),  # Keep original name for joins
        F.coalesce(F.col("updated_at"), F.col("created_at")).alias("src_updated_at")
    ) if metadata else None

    # Add state info
    if state:
        state_info = state.select(
            F.col(LISTING_ID_COL),
            _extract_state_type_from_src().alias("src_state_raw")
        ).withColumn(
            "src_state_mapped",
            _apply_state_mapping_expr("src_state_raw")
        )
        src_comparison = src_comparison.join(state_info, LISTING_ID_COL, "left") if src_comparison else None

    # Add price info
    if price:
        price_info = price.select(
            F.col(LISTING_ID_COL),
            F.coalesce(
                F.get_json_object(_json_of("data"), "$.struct.amounts.sale"),
                F.get_json_object(_json_of("data"), "$.M.amounts.M.sale.N")
            ).alias("src_price")
        )
        src_comparison = src_comparison.join(price_info, LISTING_ID_COL, "left") if src_comparison else None

    # Add attributes info (category, bedrooms)
    if attributes:
        attr_info = attributes.select(
            F.col(LISTING_ID_COL),
            F.coalesce(
                F.col("category"),
                F.get_json_object(_json_of("data"), "$.struct.category")
            ).alias("src_category")
        )
        src_comparison = src_comparison.join(attr_info, LISTING_ID_COL, "left") if src_comparison else None

    if not src_comparison:
        _log(logger, "[VALIDATE_CATALOG] No source data found")
        return {
            "segment": "CATALOG",
            "source_catalog_records": 0,
            "destination_catalog_records": 0,
            "successfully_migrated": 0
        }

    # NOW rename to src_listing_id AFTER all joins
    src_comparison = src_comparison.withColumnRenamed(LISTING_ID_COL, "src_listing_id")

    # Prepare destination data - extract from JSON
    dst_comparison = df_dst_catalog.select(
        F.col("PK").alias("dst_listing_id"),
        F.col("updated_at").alias("dst_updated_at"),
        F.get_json_object(F.col("data"), "$.state.type").alias("dst_state_type"),
        F.get_json_object(F.col("data"), "$.price.amounts.sell").alias("dst_price"),
        F.get_json_object(F.col("data"), "$.category").alias("dst_category")
    )

    destination_count = dst_comparison.count()

    # Join source and destination
    joined = src_comparison.join(
        dst_comparison,
        src_comparison["src_listing_id"] == dst_comparison["dst_listing_id"],
        "left"
    )

    # Presence check
    present_in_dest = joined.filter(F.col("dst_listing_id").isNotNull()).count()

    # Field-level validation
    field_comparison = joined.filter(F.col("dst_listing_id").isNotNull()).select(
        "src_listing_id",
        "src_updated_at",
        "dst_updated_at",
        "src_state_mapped",
        "dst_state_type",
        "src_price",
        "dst_price",
        "src_category",
        "dst_category",

        # Comparison flags
        (F.col("src_updated_at") == F.col("dst_updated_at")).alias("timestamp_match"),
        (F.lower(F.col("src_state_mapped")) == F.lower(F.col("dst_state_type"))).alias("state_match"),
        (F.col("src_price") == F.col("dst_price")).alias("price_match"),
        (F.col("src_category") == F.col("dst_category")).alias("category_match"),
        (F.col("dst_updated_at") < F.col("src_updated_at")).alias("is_stale")
    )

    # Count mismatches
    total_compared = field_comparison.count()
    timestamp_mismatches = field_comparison.filter(~F.col("timestamp_match")).count()
    state_mismatches = field_comparison.filter(~F.col("state_match")).count()
    price_mismatches = field_comparison.filter(~F.col("price_match")).count()
    category_mismatches = field_comparison.filter(~F.col("category_match")).count()
    stale_records = field_comparison.filter(F.col("is_stale")).count()

    # Any field mismatch
    field_mismatches = field_comparison.filter(
        ~F.col("timestamp_match") | ~F.col("state_match") |
        ~F.col("price_match") | ~F.col("category_match")
    ).count()

    # Collect ALL mismatches (no limit)
    all_mismatches = []
    if field_mismatches > 0:
        mismatches = field_comparison.filter(
            ~F.col("timestamp_match") | ~F.col("state_match") |
            ~F.col("price_match") | ~F.col("category_match")
        ).collect()  # REMOVED .limit()

        all_mismatches = [{
            "listing_id": r["src_listing_id"],
            "source_updated_at": str(r["src_updated_at"]),
            "dest_updated_at": str(r["dst_updated_at"]),
            "source_state": r["src_state_mapped"],
            "dest_state": r["dst_state_type"],
            "source_price": r["src_price"],
            "dest_price": r["dst_price"],
            "source_category": r["src_category"],
            "dest_category": r["dst_category"],
            "is_stale": bool(r["is_stale"]),
            "requires_resync": True
        } for r in mismatches]  # ALL mismatches

    # Collect ALL missing records (no limit)
    all_missing = []
    missing_count = source_count - present_in_dest
    if missing_count > 0:
        missing = joined.filter(F.col("dst_listing_id").isNull()) \
            .select("src_listing_id") \
            .collect()  # REMOVED .limit()
        all_missing = [r["src_listing_id"] for r in missing]  # ALL missing

    success_percentage = (present_in_dest / source_count * 100) if source_count > 0 else 0.0
    accuracy_percentage = ((total_compared - field_mismatches) / total_compared * 100) if total_compared > 0 else 0.0

    return {
        "segment": "CATALOG",
        "validation_timestamp": datetime.utcnow().isoformat() + "Z",
        "source_catalog_records": int(source_count),
        "destination_catalog_records": int(destination_count),
        "successfully_migrated": int(present_in_dest),
        "success_percentage": round(success_percentage, 2),

        # Field-level validation
        "field_level_validation": {
            "total_compared": int(total_compared),
            "field_mismatches": int(field_mismatches),
            "accuracy_percentage": round(accuracy_percentage, 2),
            "mismatch_breakdown": {
                "timestamp_mismatches": int(timestamp_mismatches),
                "state_type_mismatches": int(state_mismatches),
                "price_mismatches": int(price_mismatches),
                "category_mismatches": int(category_mismatches),
                "stale_records": int(stale_records)
            }
        },
        "all_mismatches": all_mismatches,  # Changed from mismatch_sample to all_mismatches
        "missing_count": int(missing_count),
        "all_missing": all_missing  # Changed from missing_sample to all_missing
    }

# Update the main runner to use the new comprehensive validation
def run_search_validations(args, glueContext):
    """
    Main validation runner - now uses single-scan comprehensive validation.
    """
    test_listing_ids = args.get("TEST_LISTING_IDS", [])
    test_client_ids = args.get("TEST_CLIENT_IDS", [])
    return validate_search_aggregator_comprehensive(
        glueContext,
        country_code=args["COUNTRY_CODE"].upper().strip(),
        source_listings_table=args["SOURCE_LISTINGS_TABLE"],
        source_region=args["SOURCE_REGION"],
        target_search_aggregator_table=args.get("TARGET_SEARCH_AGGREGATOR_TABLE", "").strip(),
        target_region=args["TARGET_REGION"],
        test_listing_ids=test_listing_ids,
        test_client_ids=test_client_ids,
        run_all=(len(test_listing_ids) == 0 and len(test_client_ids) == 0)
    )

def _resolve_run_scope(args):
    """
    Returns (run_all: bool, test_listing_ids: list, test_client_ids: list).
    If both lists are empty -> run_all=True (all listings).
    Otherwise -> run_all=False (filtered).
    """
    test_listing_ids = args.get("TEST_LISTING_IDS", [])
    test_client_ids = args.get("TEST_CLIENT_IDS", [])
    run_all = (len(test_listing_ids) == 0 and len(test_client_ids) == 0)
    return run_all, test_listing_ids, test_client_ids




# ---------- Job entry ----------
def init_spark():
    """
    Required:
      JOB_NAME, COUNTRY_CODE, SOURCE_REGION, TARGET_REGION,
      SOURCE_UTIL_TABLE, SOURCE_LISTINGS_TABLE, TARGET_UTIL_TABLE, TARGET_CATALOG_TABLE, MODE
    Optional:
      TEST_LISTING_IDS  -> comma-separated list of listing IDs to migrate
      TEST_CLIENT_IDS   -> comma-separated list of client IDs to migrate
      RUN_ALL           -> "true"/"false" (default "false"). If true, migrates everything.
      ONLY_STATE        -> "true"/"false" (default "false"). If true, only run STATE migration.
    """
    base_args = [
        "JOB_NAME",
        "COUNTRY_CODE",
        "SOURCE_REGION",
        "TARGET_REGION",
        "SOURCE_UTIL_TABLE",
        "SOURCE_LISTINGS_TABLE",
        "TARGET_SEARCH_AGGREGATOR_TABLE",
        "TARGET_UTIL_TABLE",
        "TARGET_CATALOG_TABLE",
        "MODE",
    ]
    args = getResolvedOptions(sys.argv, base_args)

    # Support both single ID and comma-separated list for listing IDs
    test_ids_str = _resolve_optional(sys.argv, "TEST_LISTING_IDS", "")
    args["TEST_LISTING_IDS"] = [id.strip() for id in test_ids_str.split(",") if id.strip()] if test_ids_str else []

    # Support comma-separated list for client IDs
    test_client_ids_str = _resolve_optional(sys.argv, "TEST_CLIENT_IDS", "")
    args["TEST_CLIENT_IDS"] = [id.strip() for id in test_client_ids_str.split(",") if id.strip()] if test_client_ids_str else []

    args["RUN_ALL"] = _resolve_optional(sys.argv, "RUN_ALL", "false")
    args["ONLY_STATE"] = _resolve_optional(sys.argv, "ONLY_STATE", "false")

    # SEARCH_AGGREGATOR_PARAMETERS:
    args["SEARCH_AGGREGATOR_ONLY"] = _resolve_optional(sys.argv, "SEARCH_AGGREGATOR_ONLY", "false")
    args["TARGET_SEARCH_AGGREGATOR_TABLE"] = _resolve_optional(sys.argv, "TARGET_SEARCH_AGGREGATOR_TABLE", "")

    sc = SparkContext()
    glueContext = GlueContext(sc)
    logger = glueContext.get_logger()
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)
    return spark, glueContext, job, args, logger


# ---------- Debug helpers ----------
def _log(logger, msg):
    """
    Log to stdout for Glue output logs.
    Note: logger.info() goes to driver/executor logs, print() goes to output logs.
    """
    msg_str = str(msg)
    # Print to stdout - this appears in Glue output logs
    print(msg_str)
    # Also log via logger for CloudWatch (appears in driver/executor logs)
    try:
        logger.info(msg_str)
    except Exception:
        pass

def _head_json(df, n=3):
    try:
        return [r for r in df.limit(n).toJSON().collect()]
    except Exception:
        return ["<unavailable>"]

def _count_safe(df):
    try:
        return df.count()
    except Exception:
        return -1




def run_search_only(args, glueContext):
    logger = glueContext.get_logger()

    _log(logger, "="*80)
    _log(logger, "[SEARCH] Starting Search Aggregator Migration")
    _log(logger, "="*80)

    country_code = args["COUNTRY_CODE"].upper().strip()
    source_region = args["SOURCE_REGION"]
    target_region = args["TARGET_REGION"]
    source_listings_table = args["SOURCE_LISTINGS_TABLE"]
    target_search_agg_table = (args.get("TARGET_SEARCH_AGGREGATOR_TABLE") or "").strip()

    _log(logger, f"[SEARCH] Configuration:")
    _log(logger, f"[SEARCH]   Country: {country_code}")
    _log(logger, f"[SEARCH]   Source Region: {source_region}")
    _log(logger, f"[SEARCH]   Target Region: {target_region}")
    _log(logger, f"[SEARCH]   Source Table: {source_listings_table}")
    _log(logger, f"[SEARCH]   Target Table: {target_search_agg_table}")

    if not target_search_agg_table:
        raise ValueError("MODE=search requires --TARGET_SEARCH_AGGREGATOR_TABLE <table>")

    # Scope
    test_listing_ids = args.get("TEST_LISTING_IDS", [])
    test_client_ids = args.get("TEST_CLIENT_IDS", [])

    # If client IDs provided, look up listing IDs from METADATA segment
    if test_client_ids and not test_listing_ids:
        _log(logger, "[SEARCH] Client IDs provided, looking up listing IDs from METADATA...")
        test_listing_ids = get_listing_ids_for_clients(
            glueContext, logger, country_code, source_listings_table, source_region, test_client_ids
        )
        if not test_listing_ids:
            _log(logger, "[SEARCH] No listings found for the specified client IDs, exiting")
            return
        _log(logger, f"[SEARCH] Will process {len(test_listing_ids)} listings for the specified clients")

    run_all = (len(test_listing_ids) == 0 and len(test_client_ids) == 0)

    _log(logger, f"[SEARCH v2] Resolved scope -> run_all={run_all}, listing_count={len(test_listing_ids)}, client_count={len(test_client_ids)}")
    _log(logger, f"[SEARCH v2] Country: {country_code}, Source: {source_listings_table}, Target: {target_search_agg_table}")

    # MEDIA
    # migrate_media_to_search_aggregator_v2(
    #     glueContext,
    #     country_code=country_code,
    #     source_listings_table=source_listings_table,
    #     source_region=source_region,
    #     target_search_aggregator_table=target_search_agg_table,
    #     target_region=target_region,
    #     test_listing_ids=test_listing_ids,
    #     test_client_ids=test_client_ids,
    #     run_all=run_all,
    # )

    # CATALOG
    _log(logger, "\n[SEARCH] Step 1/1: Migrating CATALOG segment to search aggregator...")
    migrate_catalog_segment_to_search_aggregator_v2(
        glueContext,
        country_code=country_code,
        source_listings_table=source_listings_table,
        source_region=source_region,
        target_search_aggregator_table=target_search_agg_table,
        target_region=target_region,
        test_listing_ids=test_listing_ids,
        test_client_ids=test_client_ids,
    )

    _log(logger, "="*80)
    _log(logger, "[SEARCH] ✓ Search Aggregator Migration Completed Successfully")
    _log(logger, "="*80)


def run_search_delete(args, glueContext):
    """
    Delete specific listings from search aggregator and send listing.deleted events.

    Usage:
        --MODE search-delete
        --COUNTRY_CODE SA
        --TARGET_SEARCH_AGGREGATOR_TABLE listing-aggregated
        --TARGET_REGION ap-southeast-1
        --SOURCE_LISTINGS_TABLE pf-listings-sa
        --SOURCE_REGION me-central-1
        --TEST_LISTING_IDS "id1,id2,id3" OR --TEST_CLIENT_IDS "123,456"
    """
    logger = glueContext.get_logger()

    _log(logger, "="*80)
    _log(logger, "[SEARCH_DELETE] Starting Search Aggregator Deletion")
    _log(logger, "="*80)

    country_code = args["COUNTRY_CODE"].lower().strip()
    source_region = args["SOURCE_REGION"]
    source_listings_table = args["SOURCE_LISTINGS_TABLE"]
    target_region = args["TARGET_REGION"]
    target_search_agg_table = (args.get("TARGET_SEARCH_AGGREGATOR_TABLE") or "").strip()

    _log(logger, f"[SEARCH_DELETE] Configuration:")
    _log(logger, f"[SEARCH_DELETE]   Country: {country_code}")
    _log(logger, f"[SEARCH_DELETE]   Target Table: {target_search_agg_table}")
    _log(logger, f"[SEARCH_DELETE]   Target Region: {target_region}")

    if not target_search_agg_table:
        raise ValueError("MODE=search-delete requires --TARGET_SEARCH_AGGREGATOR_TABLE <table>")

    # Get listing IDs to delete
    test_listing_ids = args.get("TEST_LISTING_IDS", [])
    test_client_ids = args.get("TEST_CLIENT_IDS", [])

    # If client IDs provided, look up listing IDs from METADATA segment
    if test_client_ids and not test_listing_ids:
        _log(logger, "[SEARCH_DELETE] Client IDs provided, looking up listing IDs from METADATA...")
        test_listing_ids = get_listing_ids_for_clients(
            glueContext, logger, country_code, source_listings_table, source_region, test_client_ids
        )
        if not test_listing_ids:
            _log(logger, "[SEARCH_DELETE] No listings found for the specified client IDs, exiting")
            return
        _log(logger, f"[SEARCH_DELETE] Found {len(test_listing_ids)} listings for the specified clients")

    # If no listing IDs provided, delete ALL listings for the country
    if not test_listing_ids:
        _log(logger, "[SEARCH_DELETE] WARNING: No TEST_LISTING_IDS or TEST_CLIENT_IDS provided")
        _log(logger, f"[SEARCH_DELETE] Will delete ALL listings for country: {country_code}")
        _log(logger, "[SEARCH_DELETE] Reading all listing IDs from search aggregator...")

        # Read all listing IDs for this country from search aggregator
        dyf_all = read_ddb_table(
            glueContext, target_search_agg_table, target_region
        )
        df_all = dyf_all.toDF()

        # Filter by country and SEGMENT#catalog
        df_all = df_all.filter(
            (F.col("country") == country_code.lower()) &
            (F.col("SK") == "SEGMENT#catalog")
        )

        test_listing_ids = df_all.select("PK").distinct().rdd.map(lambda r: r[0]).collect()
        _log(logger, f"[SEARCH_DELETE] Found {len(test_listing_ids)} listings to delete for country {country_code}")

        if not test_listing_ids:
            _log(logger, "[SEARCH_DELETE] No listings found for this country, exiting")
            return
    else:
        _log(logger, f"[SEARCH_DELETE] Will delete {len(test_listing_ids)} listings")

    # Delete listings
    delete_listings_from_search_aggregator(
        glueContext,
        country_code=country_code,
        target_search_aggregator_table=target_search_agg_table,
        target_region=target_region,
        listing_ids_to_delete=test_listing_ids
    )

    _log(logger, "="*80)
    _log(logger, "[SEARCH_DELETE] ✓ Search Aggregator Deletion Completed Successfully")
    _log(logger, "="*80)


def run_delete(args):
    target_region = args["TARGET_REGION"]
    target_search_aggregator_table = args.get("TARGET_SEARCH_AGGREGATOR_TABLE", "")

    if not target_search_aggregator_table:
        raise ValueError("TARGET_SEARCH_AGGREGATOR_TABLE is required for delete operation")

    search_keys = get_table_key_attrs(target_search_aggregator_table, target_region)

    # Delete only records where country = "sa"
    delete_country_items(
        target_search_aggregator_table,
        target_region,
        search_keys,
        country_code="sa",
        total_segments=4
    )


def delete_country_items(table_name, region, key_schema, country_code, total_segments=4):
    """Delete only items matching a specific country code"""
    ddb = boto3.client("dynamodb", region_name=region)

    while True:
        deleted_this_pass = 0
        for seg in range(total_segments):
            deleted_this_pass += _scan_and_delete_country_filtered(
                ddb, table_name, key_schema, seg, total_segments,
                country_code
            )
        if deleted_this_pass == 0:
            # Double check with filtered scan
            if not _table_has_country_items(ddb, table_name, country_code):
                break
            time.sleep(2)

def _scan_and_delete_country_filtered(ddb, table_name, key_schema, segment, total_segments, country_code):
    """Scan and delete items matching a specific country code"""
    expr_names = {f"#k{i}": k for i, k in enumerate(key_schema)}
    expr_names["#country"] = "country"

    projection_expr = ",".join(expr_names.keys())

    paginator = ddb.get_paginator("scan")
    deleted = 0

    for page in paginator.paginate(
        TableName=table_name,
        ProjectionExpression=projection_expr,
        FilterExpression="#country = :countryval",
        ExpressionAttributeNames=expr_names,
        ExpressionAttributeValues={":countryval": {"S": country_code.lower()}},
        ConsistentRead=True,
        Segment=segment,
        TotalSegments=total_segments
    ):
        items = page.get("Items", [])
        if not items:
            continue

        keys = [{k: item.get(k) for k in key_schema if k in item} for item in items]

        # De-duplicate
        uniq, seen = [], set()
        for k in keys:
            t = tuple(sorted((kk, tuple(v.items()) if isinstance(v, dict) and v is not None else str(v)) for kk, v in k.items()))
            if t not in seen:
                uniq.append(k)
                seen.add(t)

        for i in range(0, len(uniq), 25):
            batch_keys = uniq[i:i+25]
            requests = [{"DeleteRequest": {"Key": k}} for k in batch_keys]
            deleted += _delete_batch(ddb, table_name, requests)

    return deleted

def _table_has_country_items(ddb, table_name, country_code):
    """Check if table has any items with the specified country code"""
    try:
        resp = ddb.scan(
            TableName=table_name,
            FilterExpression="#country = :countryval",
            ExpressionAttributeNames={"#country": "country"},
            ExpressionAttributeValues={":countryval": {"S": country_code.lower()}},
            Limit=1
        )
        return bool(resp.get("Items"))
    except ClientError:
        return True



# ---------- Validation / Reporting ----------
import json, datetime, boto3

S3_REPORT_BUCKET = "pf-b2b-validations-staging"      # where all reports are written
REPORT_PREFIX = "migration-validations"          # s3 folder prefix per run

# These are the only segments we care to validate / compare
WANTED_SEGMENTS = [f"SEGMENT#{s}" for s in ["AMENITIES","ATTRIBUTES","DESCRIPTION","PRICE","METADATA","STATE"]]

def _ts():
    # UTC timestamp used to version each report set
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H-%M-%SZ")

def _s3_put_json(bucket, key, obj):
    """
    Persist a small JSON object to S3.
    Used for: summary, counts, state mapping results, shape checks, mismatch samples.
    """
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(obj, default=str, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json; charset=utf-8",
    )

def _read_src_listings(glueContext, table, region, country_code, run_all, test_listing_ids, test_client_ids):
    """
    Read source pf-listings-{country}:
      - Keep only target segments
      - If test mode, apply filters
    """
    dyf = read_ddb_table(glueContext, table, region)
    df = dyf.toDF()
    seg_col = _choose_segment_col(df)
    if not seg_col:
        raise ValueError(f"{table} missing segment/SK")
    df = df.filter(F.col(seg_col).isin(WANTED_SEGMENTS)).withColumnRenamed(seg_col, SEGMENT_COL)
    df = _apply_test_filters(df, test_listing_ids, test_client_ids, run_all)
    return df

def _read_dst_catalog(glueContext, table, region, country_code, run_all, test_listing_ids, test_client_ids):
    """
    Read destination pf-catalog:
      - Keep only rows where catalog_id starts with <COUNTRY#>
      - Keep only target segments
      - If test mode, apply filters
    """
    dyf = read_ddb_table(glueContext, table, region)
    df = dyf.toDF()
    df = df.filter(F.col(CATALOG_ID_COL).startswith(country_code + "#")) \
           .filter(F.col(SEGMENT_COL).isin(WANTED_SEGMENTS))

    if not run_all:
        if test_listing_ids:
            expected_ids = [f"{country_code}#{lid}" for lid in test_listing_ids]
            df = df.filter(F.col(CATALOG_ID_COL).isin(expected_ids))
        if test_client_ids and "client_id" in df.columns:
            df = df.filter(F.col("client_id").isin(test_client_ids))

    return df

# --- JSON helpers for STATE & METADATA checks (schema-agnostic) ---

def _extract_state_type_from_src():
    """
    Best-effort extraction of state.data.type from source:
      - supports both plain structs and 'AttributeValue'-like wrapped shapes
      - returns lowercase string column 'src_state_type'
    """
    j = _json_of(STATE_DATA_COL)
    c1 = F.get_json_object(j, "$.type")
    c2 = F.get_json_object(j, "$.M.type.S")
    c3 = F.get_json_object(j, "$.M.type")
    c4 = F.get_json_object(j, "$.type.S")
    c5 = F.get_json_object(j, "$.S")
    return F.lower(F.coalesce(c1,c2,c3,c4,c5)).alias("src_state_type")

def _metadata_hashes_present_expr(col=HASHES_COL):
    """
    True-ish (1) if hashes object exists at all.
    We later separately assert excluded keys (media, quality_score, compliance) are NOT present.
    """
    j = _json_of(col)
    has_obj = F.get_json_object(j, "$").isNotNull()
    return has_obj.cast("int").alias("hashes_present")

def _expected_catalog_id(country, col=LISTING_ID_COL):
    # Used for ID mapping validation: <COUNTRY>#<LISTING_ID>
    return F.concat_ws("#", F.lit(country), F.col(col)).alias("expected_catalog_id")

def _apply_state_mapping_expr(state_col="src_state_type"):
    """
    Apply agreed mapping from Listing Service -> Catalog Service state_type.
    If unknown, fall back to source value.
    """
    mapping_map = F.create_map(*sum([[F.lit(k), F.lit(v)] for k, v in STATE_TYPE_MAP.items()], []))
    s = F.col(state_col)
    return F.coalesce(mapping_map.getItem(s), s).alias("mapped_state_type")

def _presence(col):
    # Convenience reducer: returns 1 if any non-null exists for column in the group/window
    return F.max(F.when(F.col(col).isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias(col)

# ---------- Main validator ----------
def run_validations(args, glueContext, spark, logger):
    """
    Validations included (each writes a JSON artifact to S3):
      1) Counts: total rows and per-segment counts for source & destination
      2) ID mapping: verifies listing_id -> <COUNTRY>#listing_id for all segments
         - also emits a small sample of missing pairs
      3) STATE mapping: verifies state_type mapping correctness and frequency
         - produces per-type counts (source vs destination) and a sample of mismatches
      4) Per-segment "shape" checks:
         - AMENITIES: data present (array)
         - ATTRIBUTES: data present (struct), 'type' present
         - PRICE: data present (struct), 'price_type' present
         - DESCRIPTION: 'description' present (note: data can be null by design)
         - METADATA: hashes present AND excluded keys (media, quality_score, compliance) are NOT present
         - STATE: state_type present and equals data.type
    """
    country = args["COUNTRY_CODE"].upper().strip()
    src_region = args["SOURCE_REGION"]
    dst_region = args["TARGET_REGION"]
    src_listings_table = args["SOURCE_LISTINGS_TABLE"]
    dst_catalog_table = args["TARGET_CATALOG_TABLE"]

    test_listing_ids = args.get("TEST_LISTING_IDS", [])
    test_client_ids = args.get("TEST_CLIENT_IDS", [])
    validate_run_all = (len(test_listing_ids) == 0 and len(test_client_ids) == 0)

    ts = _ts()
    base_key = f"{REPORT_PREFIX}/{country}/{ts}/"

    # -------- Read input frames (restricted to scope) and cache to avoid re-scans
    src_df = _read_src_listings(glueContext, src_listings_table, src_region,
                                country, validate_run_all, test_listing_ids, test_client_ids).cache()
    dst_df = _read_dst_catalog(glueContext, dst_catalog_table, dst_region,
                                country, validate_run_all, test_listing_ids, test_client_ids).cache()

    # ---------- 1) Counts & per-segment counts ----------
    src_total = src_df.count()
    dst_total = dst_df.count()

    src_by_seg = {r[SEGMENT_COL]: r["cnt"]
                  for r in src_df.groupBy(SEGMENT_COL).count()
                               .withColumnRenamed("count","cnt").collect()}
    dst_by_seg = {r[SEGMENT_COL]: r["cnt"]
                  for r in dst_df.groupBy(SEGMENT_COL).count()
                               .withColumnRenamed("count","cnt").collect()}

    summary = {
        "country": country,
        "timestamp_utc": ts,
        "mode": "validate",
        "scope": {
            "run_all": validate_run_all,
            "test_listing_ids": test_listing_ids if not validate_run_all else None,
            "test_client_ids": test_client_ids if not validate_run_all else None
        },
        "counts": {
            "source_total": int(src_total),
            "destination_total": int(dst_total)
        }
    }
    _s3_put_json(S3_REPORT_BUCKET, base_key + "summary.json", summary)
    _s3_put_json(S3_REPORT_BUCKET, base_key + "counts-by-segment.json",
                 {"source": src_by_seg, "destination": dst_by_seg})

    # ---------- 2) ID mapping check ----------
    src_ids = (
        src_df
        .select(
            F.col(LISTING_ID_COL),
            F.col(SEGMENT_COL).alias("src_seg"),
            _expected_catalog_id(country).alias("expected_catalog_id")
        )
        .dropDuplicates([LISTING_ID_COL, "src_seg"])
    )

    dst_ids = (
        dst_df
        .select(
            F.col(CATALOG_ID_COL).alias("dst_catalog_id"),
            F.col(SEGMENT_COL).alias("dst_seg")
        )
        .dropDuplicates(["dst_catalog_id", "dst_seg"])
    )

    id_join = (
        src_ids.join(
            dst_ids,
            (src_ids["src_seg"] == dst_ids["dst_seg"]) &
            (dst_ids["dst_catalog_id"] == src_ids["expected_catalog_id"]),
            "left"
        )
        .withColumn("mapped_ok", F.when(F.col("dst_catalog_id").isNotNull(), F.lit(1)).otherwise(F.lit(0)))
        .withColumn("segment", F.col("src_seg"))
        .drop("src_seg", "dst_seg")
    )

    id_ok = id_join.agg(F.sum("mapped_ok").alias("ok")).collect()[0]["ok"] or 0
    id_total = src_ids.count()

    _s3_put_json(S3_REPORT_BUCKET, base_key + "id-mapping.json", {
        "expected_pairs": int(id_total),
        "present_in_destination": int(id_ok),
        "missing_pairs": int(id_total - id_ok)
    })

    # Sample missing (catalog_id, segment) pairs for triage
    missing_pairs_df = (
        id_join.filter(F.col("mapped_ok") == 0)
               .select(F.col("expected_catalog_id").alias("catalog_id"),
                       F.col("segment"))
               .limit(500)
    )
    missing_pairs = [ {"catalog_id": r["catalog_id"], "segment": r["segment"]} for r in missing_pairs_df.collect() ]
    if missing_pairs:
        _s3_put_json(S3_REPORT_BUCKET, base_key + "missing-pairs-sample.json", missing_pairs)

    # ---------- 3) STATE mapping correctness + frequency ----------
    src_state = (
        src_df.filter(F.col(SEGMENT_COL) == "SEGMENT#STATE")
              .select(LISTING_ID_COL, SEGMENT_COL, _extract_state_type_from_src())
              .withColumn("mapped_state_type", _apply_state_mapping_expr("src_state_type"))
    )

    dst_state = (dst_df.filter(F.col(SEGMENT_COL) == "SEGMENT#STATE")
                      .select(CATALOG_ID_COL, SEGMENT_COL,
                              F.col("state_type").alias("dst_state_type")))

    state_cmp = (
        src_state
         .withColumn("expected_catalog_id",
                     F.concat_ws("#", F.lit(country), F.col(LISTING_ID_COL)))
         .join(dst_state, dst_state[CATALOG_ID_COL] == F.col("expected_catalog_id"), "left")
         .withColumn("match",
                     F.when((F.col("dst_state_type").isNotNull()) &
                            (F.lower(F.col("dst_state_type")) ==
                             F.lower(F.col("mapped_state_type"))), 1).otherwise(0))
    )

    # Per-state-type counts in source (post mapping) vs destination
    state_counts_src = (src_state.groupBy("mapped_state_type")
                        .count().withColumnRenamed("count","source_count"))
    state_counts_dst = (dst_state.groupBy(F.lower(F.col("dst_state_type"))
                        .alias("mapped_state_type"))
                        .count().withColumnRenamed("count","destination_count"))

    state_counts = (state_counts_src.join(state_counts_dst, ["mapped_state_type"], "full")
                                    .na.fill(0, subset=["source_count","destination_count"])
                                    .orderBy("mapped_state_type")
                                    .limit(5000)
                                    .toPandas().to_dict(orient="records"))
    _s3_put_json(S3_REPORT_BUCKET, base_key + "state-mapping-counts.json", state_counts)

    # Sample mismatches
    mismatches = (state_cmp.filter(F.col("match") == 0)
                          .select(LISTING_ID_COL, "src_state_type",
                                  "mapped_state_type", "dst_state_type")
                          .limit(200)
                          .toPandas().to_dict(orient="records"))
    if mismatches:
        _s3_put_json(S3_REPORT_BUCKET, base_key + "state-mapping-mismatches.json", mismatches)

    # ---------- 4) Per-segment "shape" validations ----------
    checks = {}

    # AMENITIES
    dst_amen = dst_df.filter(F.col(SEGMENT_COL) == "SEGMENT#AMENITIES")
    checks["AMENITIES"] = {
        "rows": dst_amen.count(),
        "data_present_rows": dst_amen.filter(F.col(STATE_DATA_COL).isNotNull()).count()
    }

    # ATTRIBUTES
    dst_attr = dst_df.filter(F.col(SEGMENT_COL) == "SEGMENT#ATTRIBUTES")
    checks["ATTRIBUTES"] = {
        "rows": dst_attr.count(),
        "data_present_rows": dst_attr.filter(F.col(STATE_DATA_COL).isNotNull()).count(),
        "type_present_rows": dst_attr.filter(F.col("type").isNotNull()).count()
    }

    # PRICE
    dst_price = dst_df.filter(F.col(SEGMENT_COL) == "SEGMENT#PRICE")
    checks["PRICE"] = {
        "rows": dst_price.count(),
        "data_present_rows": dst_price.filter(F.col(STATE_DATA_COL).isNotNull()).count(),
        "price_type_present_rows": dst_price.filter(F.col("price_type").isNotNull()).count()
    }

    # DESCRIPTION
    dst_desc = dst_df.filter(F.col(SEGMENT_COL) == "SEGMENT#DESCRIPTION")
    checks["DESCRIPTION"] = {
        "rows": dst_desc.count(),
        "description_present_rows": dst_desc.filter(F.col("description").isNotNull()).count()
    }

    # METADATA
    dst_meta = dst_df.filter(F.col(SEGMENT_COL) == "SEGMENT#METADATA")
    hashes_present_rows = (dst_meta
                           .withColumn("hashes_present", _metadata_hashes_present_expr(HASHES_COL))
                           .filter(F.col("hashes_present") == 1)
                           .count())
    bad_excl = dst_meta.filter(
        F.coalesce(
            F.get_json_object(_json_of(HASHES_COL), "$.media"),
            F.get_json_object(_json_of(HASHES_COL), "$.quality_score"),
            F.get_json_object(_json_of(HASHES_COL), "$.compliance")
        ).isNotNull()
    ).count()
    checks["METADATA"] = {
        "rows": dst_meta.count(),
        "hashes_present_rows": hashes_present_rows,
        "excluded_keys_present_rows": bad_excl
    }

    # STATE
    dst_state_shape = dst_df.filter(F.col(SEGMENT_COL) == "SEGMENT#STATE")
    state_shape_ok = dst_state_shape.filter(
        (F.col("state_type").isNotNull()) &
        (F.lower(F.col("state_type")) == F.lower(F.col(f"{STATE_DATA_COL}.type")))
    ).count()
    checks["STATE"] = {
        "rows": dst_state_shape.count(),
        "state_type_present_rows": dst_state_shape.filter(F.col("state_type").isNotNull()).count(),
        "state_type_equals_data_type_rows": state_shape_ok
    }

    _s3_put_json(S3_REPORT_BUCKET, base_key + "per-segment-shape.json", checks)
    logger.info(f"[VALIDATION] SRC={src_total}, DST={dst_total} -> s3://{S3_REPORT_BUCKET}/{base_key}")


# ---------- Main ----------
if __name__ == "__main__":
    spark, glueContext, job, args, logger = init_spark()
    mode = args.get("MODE", "migrate").lower()

    if mode == "delete":
        run_delete(args)
    elif mode == "validate":
        # run validations only
        run_validations(args, glueContext, spark, logger)
    elif mode == "search":
        # only write Search Aggregator artifacts (MEDIA + CATALOG)
        run_search_only(args, glueContext)
    elif mode == "search-delete":
        # Delete listings from search aggregator with listing.deleted events
        run_search_delete(args, glueContext)
    elif mode == "search-validate":
        # Only run search validation (no migration)
        run_search_validations(args, glueContext)
    else:
        # default: migrate
        run_migration(args, glueContext, logger)
        # also publish a report right after migration
        try:
            run_validations(args, glueContext, spark, logger)
        except Exception as e:
            logger.error(f"Validation failed:")

    job.commit()
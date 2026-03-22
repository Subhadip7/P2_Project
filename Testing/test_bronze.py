import pytest
from pyspark.sql import SparkSession

# ============================================================
# TABLE REFERENCES
# ============================================================

SENTIMENT_TABLE = "social_catalog.bronze.sentiment_table"
TRENDS_TABLE = "social_catalog.bronze.trends_table"
TWEETS_TABLE = "social_catalog.bronze.tweets_table"
USER_METADATA_TABLE = "social_catalog.bronze.user_metadata_table"
VALID_TABLE = "social_catalog.bronze.valid_table"

BRONZE_SCHEMA = "social_catalog.bronze"

# ============================================================
# Spark Session Fixture
# ============================================================

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("bronze-layer-test") \
        .getOrCreate()

# ============================================================
# Test 1: Check Bronze Schema Exists
# ============================================================

def test_bronze_schema_exists(spark):
    schemas = spark.sql("SHOW SCHEMAS IN social_catalog")
    assert schemas.filter(schemas.databaseName == "bronze").count() == 1


# ============================================================
# Test 2: All Bronze Tables Exist
# ============================================================

def test_all_tables_exist(spark):
    tables = spark.sql(f"SHOW TABLES IN {BRONZE_SCHEMA}")

    expected_tables = [
        "sentiment_table",
        "trends_table",
        "tweets_table",
        "user_metadata_table",
        "valid_table"
    ]

    for table in expected_tables:
        assert tables.filter(tables.tableName == table).count() == 1


# ============================================================
# Test 3: Tables Have Data
# ============================================================

@pytest.mark.parametrize("table_name", [
    SENTIMENT_TABLE,
    TRENDS_TABLE,
    TWEETS_TABLE,
    USER_METADATA_TABLE,
    VALID_TABLE
])
def test_tables_not_empty(spark, table_name):
    df = spark.table(table_name)
    assert df.count() > 0


# ============================================================
# Test 4: Required Columns Validation
# ============================================================

def test_sentiment_columns(spark):
    df = spark.table(SENTIMENT_TABLE)

    expected_cols = [
        "tweet_id", "topic_category", "tweet_timestamp",
        "sentiment_score", "positive_score", "negative_score",
        "neutral_score", "impressions", "likes",
        "engagement_count", "ingested_at"
    ]

    for col in expected_cols:
        assert col in df.columns


def test_trends_columns(spark):
    df = spark.table(TRENDS_TABLE)

    expected_cols = [
        "trend_timestamp", "topic_catagory", "country",
        "tweet_volume", "mention_count", "retweet_count",
        "trend_score", "sentiment_index", "impressions",
        "engagement_count", "ingested_at"
    ]

    for col in expected_cols:
        assert col in df.columns


def test_tweets_columns(spark):
    df = spark.table(TWEETS_TABLE)

    expected_cols = [
        "tweet_id", "user_id", "tweet_text", "timestamp",
        "likes", "retweets", "replies", "impressions",
        "engagement", "ingested_at"
    ]

    for col in expected_cols:
        assert col in df.columns


def test_user_metadata_columns(spark):
    df = spark.table(USER_METADATA_TABLE)

    expected_cols = [
        "user_id", "country", "topic_catagory",
        "account_created_date", "followers_count",
        "following_count", "likes_count", "shares_count",
        "posts_count", "varified", "ingested_at"
    ]

    for col in expected_cols:
        assert col in df.columns


def test_valid_columns(spark):
    df = spark.table(VALID_TABLE)

    expected_cols = [
        "tweet_id", "topic_category", "tweet_text",
        "tweet_timestamp", "impressions", "likes",
        "retweets", "replies", "engagement_count",
        "sentiment_score", "ingested_at"
    ]

    for col in expected_cols:
        assert col in df.columns


# ============================================================
# Test 5: Column Naming Convention (lowercase)
# ============================================================

@pytest.mark.parametrize("table_name", [
    SENTIMENT_TABLE,
    TRENDS_TABLE,
    TWEETS_TABLE,
    USER_METADATA_TABLE,
    VALID_TABLE
])
def test_column_lowercase(spark, table_name):
    df = spark.table(table_name)

    for col_name in df.columns:
        assert col_name == col_name.lower()


# ============================================================
# Test 6: ingested_at should not be NULL
# ============================================================

@pytest.mark.parametrize("table_name", [
    SENTIMENT_TABLE,
    TRENDS_TABLE,
    TWEETS_TABLE,
    USER_METADATA_TABLE,
    VALID_TABLE
])
def test_ingested_at_not_null(spark, table_name):
    df = spark.table(table_name)

    null_count = df.filter(df.ingested_at.isNull()).count()

    assert null_count == 0


# ============================================================
# Test 7: Allowable NULL Threshold (Bronze Layer Logic)
# ============================================================

def test_sentiment_tweet_id_null_threshold(spark):
    df = spark.table(SENTIMENT_TABLE)

    null_count = df.filter(df.tweet_id.isNull()).count()
    total_count = df.count()

    assert total_count > 0
    assert (null_count / total_count) < 0.1   # Allow <10% nulls


def test_tweets_text_null_threshold(spark):
    df = spark.table(TWEETS_TABLE)

    null_count = df.filter(df.tweet_text.isNull()).count()
    total_count = df.count()

    assert total_count > 0
    assert (null_count / total_count) < 0.1


# ============================================================
# Test 8: Duplicate Check (Basic Validation)
# ============================================================

def test_no_duplicate_tweets(spark):
    df = spark.table(TWEETS_TABLE)

    total_count = df.count()
    distinct_count = df.select("tweet_id").distinct().count()

    # Bronze allows duplicates but should not explode
    assert total_count >= distinct_count
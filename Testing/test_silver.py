# ============================================================
# ONE-SHOT PYTEST RUNNER (Databricks)
# ============================================================

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim

# ============================================================
# TABLE REFERENCES
# ============================================================

TWEETS_TABLE = "social_catalog.silver.tweets_table"
SENTIMENT_TABLE = "social_catalog.silver.sentiment_table"
TRENDS_TABLE = "social_catalog.silver.trends_table"
USER_METADATA_TABLE = "social_catalog.silver.user_metadata_table"
VALID_TABLE = "social_catalog.silver.valid_table"

SILVER_SCHEMA = "social_catalog.silver"

# ============================================================
# Spark Session Fixture
# ============================================================

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("silver-layer-test").getOrCreate()

# ============================================================
# TESTS
# ============================================================

def test_silver_schema_exists(spark):
    schemas = spark.sql("SHOW SCHEMAS IN social_catalog")
    assert schemas.filter(schemas.databaseName == "silver").count() == 1


def test_all_tables_exist(spark):
    tables = spark.sql(f"SHOW TABLES IN {SILVER_SCHEMA}")

    expected_tables = [
        "tweets_table",
        "sentiment_table",
        "trends_table",
        "user_metadata_table",
        "valid_table"
    ]

    for table in expected_tables:
        assert tables.filter(tables.tableName == table).count() == 1


@pytest.mark.parametrize("table_name", [
    TWEETS_TABLE,
    SENTIMENT_TABLE,
    TRENDS_TABLE,
    USER_METADATA_TABLE,
    VALID_TABLE
])
def test_tables_not_empty(spark, table_name):
    df = spark.table(table_name)
    assert df.count() > 0


def test_tweets_not_null(spark):
    df = spark.table(TWEETS_TABLE)
    assert df.filter(col("tweet_id").isNull()).count() == 0
    assert df.filter(col("tweet_text").isNull()).count() == 0


def test_users_not_null(spark):
    df = spark.table(USER_METADATA_TABLE)
    assert df.filter(col("user_id").isNull()).count() == 0


# 🔥 STRICT DUPLICATE CHECK (FINAL FIX)
def test_no_duplicate_tweet_id(spark):
    df = spark.table(TWEETS_TABLE)

    duplicates = df.groupBy("tweet_id") \
                   .count() \
                   .filter(col("count") > 1) \
                   .count()

    assert duplicates == 0


def test_no_duplicate_users(spark):
    df = spark.table(USER_METADATA_TABLE)

    duplicates = df.groupBy("user_id") \
                   .count() \
                   .filter(col("count") > 1) \
                   .count()

    assert duplicates == 0


def test_tweet_text_cleaned(spark):
    df = spark.table(TWEETS_TABLE)

    invalid = df.filter(
        (trim(col("tweet_text")) == "") |
        (lower(trim(col("tweet_text"))) == "null")
    ).count()

    assert invalid == 0


def test_numeric_columns_filled(spark):
    df = spark.table(TWEETS_TABLE)

    assert df.filter(col("likes").isNull()).count() == 0
    assert df.filter(col("retweets").isNull()).count() == 0
    assert df.filter(col("replies").isNull()).count() == 0


def test_timestamp_valid(spark):
    df = spark.table(TWEETS_TABLE)
    assert df.filter(col("timestamp").isNull()).count() == 0


def test_sentiment_range(spark):
    df = spark.table(SENTIMENT_TABLE)

    invalid = df.filter(
        (col("sentiment_score") < -1) | (col("sentiment_score") > 1)
    ).count()

    assert invalid == 0


def test_engagement_non_negative(spark):
    df = spark.table(TWEETS_TABLE)
    assert df.filter(col("engagement") < 0).count() == 0


# ============================================================
# RUN TESTS (IMPORTANT)
# ============================================================

pytest.main(["-v"])
import sys
import requests
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, trim

# Init Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# ── Config ──────────────────────────────────────────────
S3_CURATED = "s3://ecom-curated-zone-kishore/orders/"
TODAY = datetime.utcnow().strftime("%Y-%m-%d")

# ── Step 1: Fetch from API ───────────────────────────────
def fetch_orders():
    products = {p["id"]: p for p in requests.get(
        "https://dummyjson.com/products?limit=200").json()["products"]}
    users = {u["id"]: u for u in requests.get(
        "https://dummyjson.com/users?limit=200").json()["users"]}
    carts = requests.get(
        "https://dummyjson.com/carts?limit=200").json()["carts"]

    orders = []
    for cart in carts:
        user = users.get(cart["userId"], {})
        for item in cart["products"]:
            product = products.get(item["id"], {})
            unit_price = product.get("price", 0)
            cost_price = round(unit_price * 0.6, 2)
            quantity = item.get("quantity", 1)
            revenue = round(unit_price * quantity, 2)
            profit = round(revenue - cost_price * quantity, 2)
            orders.append({
                "order_id":         f"ORD-{cart['id']}-{item['id']}",
                "order_timestamp":  datetime.utcnow().isoformat(),
                "customer_id":      cart["userId"],
                "customer_name":    f"{user.get('firstName','')} {user.get('lastName','')}".strip(),
                "email":            user.get("email", ""),
                "country":          user.get("address", {}).get("country", ""),
                "product_id":       item["id"],
                "product_name":     item.get("title", ""),
                "category":         product.get("category", ""),
                "unit_price":       unit_price,
                "cost_price":       cost_price,
                "quantity":         quantity,
                "revenue":          revenue,
                "profit":           profit,
                "status":           "completed"
            })
    return orders

# ── Step 2: Clean ────────────────────────────────────────
def clean(df):
    df = df.dropDuplicates(["order_id"])
    df = df.filter(col("unit_price") > 0)
    df = df.withColumn("category",
            when(trim(col("category")) == "", "Unknown")
            .otherwise(col("category")))
    df = df.withColumn("customer_name",
            when(trim(col("customer_name")) == "", "Unknown")
            .otherwise(col("customer_name")))
    df = df.withColumn("email",
            when(trim(col("email")) == "", "no-email@unknown.com")
            .otherwise(col("email")))
    df = df.withColumn("country",
            when(trim(col("country")) == "", "Unknown")
            .otherwise(col("country")))
    return df

# ── Step 3: Run ──────────────────────────────────────────
orders = fetch_orders()
df = spark.createDataFrame(orders)
df = clean(df)

# Write partitioned Parquet to S3
df.write.mode("overwrite") \
    .partitionBy("order_timestamp") \
    .parquet(f"{S3_CURATED}order_date={TODAY}/")

print(f"✅ Written {df.count()} orders to {S3_CURATED}")
job.commit()

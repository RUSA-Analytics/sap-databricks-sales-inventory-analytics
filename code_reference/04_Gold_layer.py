# RUSA Analytics - Day 4
# Gold Layer - CORRECTED
#
# Corrections:
# 1. Removed invalid "stock_quantity",- expression.
# 2. PySpark round is aliased as spark_round.
# 3. Sales period is calculated from the actual order_date range.
# 4. Inventory-risk Gold table is explicitly saved as:
#    rusa_analytics_gold.inventory_risk
# 5. Removed obsolete/repeated Gold logic and consolidated validation.
#
# Databricks prerequisite:
# Silver tables must already exist.

print("RUSA Sales & Inventory Analytics")
print("Day 4 - Gold Layer - CORRECTED")
print("=" * 50)

spark.sql("""
CREATE SCHEMA IF NOT EXISTS rusa_analytics_gold
""")
print("Gold schema created successfully!")

from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg,
    min as spark_min, max as spark_max,
    datediff, when, round as spark_round, desc
)

# ---------------------------------------------------------------------
# Load Silver tables
# ---------------------------------------------------------------------
customers = spark.table("rusa_analytics_silver.customers")
products = spark.table("rusa_analytics_silver.products")
sales_orders = spark.table("rusa_analytics_silver.sales_orders")
inventory = spark.table("rusa_analytics_silver.inventory")
deliveries = spark.table("rusa_analytics_silver.deliveries")
billing = spark.table("rusa_analytics_silver.billing")

print("Silver tables loaded successfully!")
print("Customers:", customers.count())
print("Products:", products.count())
print("Sales Orders:", sales_orders.count())
print("Inventory:", inventory.count())
print("Deliveries:", deliveries.count())
print("Billing:", billing.count())

# ---------------------------------------------------------------------
# Dynamic sales period
# ---------------------------------------------------------------------
date_range = (
    sales_orders
    .select(
        spark_min("order_date").alias("start_date"),
        spark_max("order_date").alias("end_date")
    )
    .collect()[0]
)

start_date = date_range["start_date"]
end_date = date_range["end_date"]
sales_period_days = (end_date - start_date).days + 1

print("Sales period:", start_date, "to", end_date)
print("Number of days:", sales_period_days)

# ---------------------------------------------------------------------
# Sales Gold
# ---------------------------------------------------------------------
sales_summary = (
    sales_orders
    .join(
        products.select(
            "product_id", "product_name",
            "category", "unit_price"
        ),
        on="product_id",
        how="left"
    )
    .groupBy(
        "product_id", "product_name", "category"
    )
    .agg(
        spark_sum("quantity").alias("total_quantity_sold"),
        count("order_id").alias("total_orders"),
        spark_round(avg("quantity"), 2).alias("avg_quantity_per_order"),
        spark_round(spark_sum("sales_amount"), 2).alias("total_sales")
    )
    .orderBy(desc("total_sales"))
)

sales_summary.write.mode("overwrite").format("delta").saveAsTable(
    "rusa_analytics_gold.sales_summary"
)
print("Sales Gold table created successfully!")

# ---------------------------------------------------------------------
# Inventory Gold
# ---------------------------------------------------------------------
product_demand = (
    sales_orders
    .groupBy("product_id")
    .agg(
        spark_sum("quantity").alias("total_quantity_sold"),
        count("order_id").alias("number_of_orders")
    )
)

inventory_final = (
    inventory
    .join(product_demand, on="product_id", how="left")
    .join(
        products.select(
            "product_id", "product_name", "category",
            "unit_price", "lead_time_days"
        ),
        on="product_id",
        how="left"
    )
    .withColumn(
        "avg_daily_demand",
        spark_round(col("total_quantity_sold") / sales_period_days, 2)
    )
    .withColumn(
        "stock_coverage_days",
        spark_round(
            col("stock_quantity") / col("avg_daily_demand"),
            2
        )
    )
    .withColumn(
        "inventory_status",
        when(
            col("stock_coverage_days") < col("lead_time_days"),
            "Stockout Risk"
        )
        .when(
            col("stock_quantity") < col("reorder_level"),
            "Reorder Required"
        )
        .otherwise("Healthy")
    )
    .withColumn(
        "reorder_quantity",
        when(
            col("stock_quantity") < col("reorder_level"),
            col("reorder_level") - col("stock_quantity")
        ).otherwise(0)
    )
)

inventory_summary = (
    inventory_final
    .withColumn(
        "inventory_value",
        spark_round(col("stock_quantity") * col("unit_price"), 2)
    )
    .select(
        "product_id",
        "product_name",
        "category",
        "warehouse_id",
        "stock_quantity",
        "reorder_level",
        "reorder_quantity",
        "lead_time_days",
        "avg_daily_demand",
        "stock_coverage_days",
        "inventory_status",
        "inventory_value",
        "last_restock_date"
    )
    .orderBy("product_id")
)

inventory_summary.write.mode("overwrite").format("delta").saveAsTable(
    "rusa_analytics_gold.inventory_summary"
)
print("Inventory Gold table created successfully!")

# ---------------------------------------------------------------------
# Inventory Risk Gold - authoritative table name
# ---------------------------------------------------------------------
inventory_risk_gold = (
    inventory_summary
    .select(
        "product_id",
        "product_name",
        "category",
        "warehouse_id",
        "stock_quantity",
        "reorder_level",
        "reorder_quantity",
        "lead_time_days",
        "avg_daily_demand",
        "stock_coverage_days",
        "inventory_status",
        "inventory_value"
    )
    .orderBy("inventory_status", "product_id")
)

inventory_risk_gold.write.mode("overwrite").format("delta").saveAsTable(
    "rusa_analytics_gold.inventory_risk"
)
print("Inventory Risk Gold table created successfully!")

# ---------------------------------------------------------------------
# Fulfilment Gold
# ---------------------------------------------------------------------
fulfilment_summary = (
    sales_orders
    .join(
        deliveries.select(
            "order_id", "delivery_date",
            "expected_delivery_date", "delivery_status"
        ),
        on="order_id",
        how="left"
    )
    .join(
        billing.select(
            "order_id", "invoice_date",
            "invoice_amount", "payment_status"
        ),
        on="order_id",
        how="left"
    )
    .withColumn(
        "fulfilment_days",
        datediff(col("delivery_date"), col("order_date"))
    )
    .withColumn(
        "delivery_performance",
        when(
            col("delivery_date") <= col("expected_delivery_date"),
            "On Time"
        ).otherwise("Delayed")
    )
    .withColumn(
        "billing_status",
        when(col("invoice_date").isNotNull(), "Billed")
        .otherwise("Not Billed")
    )
)

fulfilment_summary.write.mode("overwrite").format("delta").saveAsTable(
    "rusa_analytics_gold.fulfilment_summary"
)
print("Fulfilment Gold table created successfully!")

# ---------------------------------------------------------------------
# Slow-moving Inventory Gold
# ---------------------------------------------------------------------
slow_moving_gold = (
    inventory_final
    .withColumn(
        "sales_frequency",
        spark_round(
            col("number_of_orders") / sales_period_days,
            3
        )
    )
    .withColumn(
        "movement_category",
        when(
            col("avg_daily_demand") < 2.1,
            "Low Movement"
        ).otherwise("Normal Movement")
    )
    .withColumn(
        "inventory_value",
        spark_round(col("stock_quantity") * col("unit_price"), 2)
    )
    .select(
        "product_id",
        "product_name",
        "category",
        "warehouse_id",
        "stock_quantity",
        "avg_daily_demand",
        "stock_coverage_days",
        "inventory_value",
        "movement_category"
    )
    .orderBy("avg_daily_demand")
)

slow_moving_gold.write.mode("overwrite").format("delta").saveAsTable(
    "rusa_analytics_gold.slow_moving_inventory"
)
print("Slow-Moving Inventory Gold table created successfully!")

# ---------------------------------------------------------------------
# Gold-layer validation
# ---------------------------------------------------------------------
gold_tables = [
    "sales_summary",
    "inventory_summary",
    "fulfilment_summary",
    "inventory_risk",
    "slow_moving_inventory"
]

print("=== GOLD LAYER VALIDATION ===")
for table in gold_tables:
    df = spark.table(f"rusa_analytics_gold.{table}")
    print(f"{table}: {df.count()} records")

print("\nGold Layer validation completed successfully.")

# Expected row counts for the supplied synthetic dataset:
# sales_summary: 10
# inventory_summary: 10
# fulfilment_summary: 1000
# inventory_risk: 10
# slow_moving_inventory: 10

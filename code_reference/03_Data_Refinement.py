# RUSA Analytics - Day 3
# Data Refinement - CORRECTED
#
# Corrections:
# 1. PySpark round is aliased as spark_round so Python scalar round() is not shadowed.
# 2. Sales period is calculated from the actual order_date range.
# 3. Slow-moving sales frequency uses the calculated number of days.
# 4. Delivery and billing validation use Python round() on collected scalar values.
#
# Databricks prerequisite:
# Silver tables must already exist:
# rusa_analytics_silver.customers
# rusa_analytics_silver.products
# rusa_analytics_silver.sales_orders
# rusa_analytics_silver.inventory
# rusa_analytics_silver.deliveries
# rusa_analytics_silver.billing

print("RUSA Sales & Inventory Analytics")
print("Day 3 - Synthetic Data Refinement - CORRECTED")
print("=" * 60)

tables = ["customers", "products", "sales_orders", "inventory", "deliveries", "billing"]

print("\nSilver Tables:")
for table in tables:
    count = spark.table(f"rusa_analytics_silver.{table}").count()
    print(f"{table}: {count} records")
print("\nDay 3 environment check completed!")

# ---------------------------------------------------------------------
# Product demand
# ---------------------------------------------------------------------
from pyspark.sql.functions import (
    col, sum as spark_sum, avg, count, min as spark_min,
    max as spark_max, desc, datediff, when, round as spark_round
)

sales = spark.table("rusa_analytics_silver.sales_orders")
products = spark.table("rusa_analytics_silver.products")

product_demand = (
    sales.groupBy("product_id")
    .agg(
        spark_sum("quantity").alias("total_quantity_sold"),
        avg("quantity").alias("avg_quantity_per_order"),
        count("order_id").alias("number_of_orders"),
        spark_sum("sales_amount").alias("total_sales")
    )
    .join(
        products.select(
            "product_id", "product_name", "category",
            "unit_price", "lead_time_days", "reorder_level"
        ),
        on="product_id",
        how="left"
    )
    .orderBy(desc("total_quantity_sold"))
)

product_demand.show(20, truncate=False)

# ---------------------------------------------------------------------
# Inventory analysis
# ---------------------------------------------------------------------
inventory = spark.table("rusa_analytics_silver.inventory")

inventory_analysis = (
    product_demand
    .select(
        "product_id", "product_name", "total_quantity_sold",
        "number_of_orders", "lead_time_days", "reorder_level"
    )
    .join(
        inventory.select(
            "product_id", "warehouse_id",
            "stock_quantity", "last_restock_date"
        ),
        on="product_id",
        how="left"
    )
    .orderBy("product_id")
)

inventory_analysis.show(20, truncate=False)

# ---------------------------------------------------------------------
# Dynamic sales period and inventory risk
# ---------------------------------------------------------------------
date_range = (
    sales.select(
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

inventory_risk = (
    inventory_analysis
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
        "below_reorder_level",
        col("stock_quantity") < col("reorder_level")
    )
    .orderBy("product_id")
)

inventory_risk.select(
    "product_id", "product_name", "total_quantity_sold",
    "stock_quantity", "reorder_level", "lead_time_days",
    "avg_daily_demand", "stock_coverage_days",
    "below_reorder_level"
).show(20, truncate=False)

# ---------------------------------------------------------------------
# Inventory classification
# ---------------------------------------------------------------------
inventory_risk_classified = (
    inventory_risk
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
)

inventory_risk_classified.select(
    "product_id", "product_name", "stock_quantity",
    "reorder_level", "lead_time_days", "avg_daily_demand",
    "stock_coverage_days", "below_reorder_level",
    "inventory_status"
).show(20, truncate=False)

# ---------------------------------------------------------------------
# Reorder quantity
# ---------------------------------------------------------------------
inventory_final = (
    inventory_risk_classified
    .withColumn(
        "reorder_quantity",
        when(
            col("stock_quantity") < col("reorder_level"),
            col("reorder_level") - col("stock_quantity")
        ).otherwise(0)
    )
)

inventory_final.select(
    "product_id", "product_name", "stock_quantity",
    "reorder_level", "reorder_quantity", "inventory_status"
).show(20, truncate=False)

# ---------------------------------------------------------------------
# Slow-moving inventory
# ---------------------------------------------------------------------
slow_moving = (
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
)

slow_moving.select(
    "product_id", "product_name", "total_quantity_sold",
    "number_of_orders", "avg_daily_demand", "stock_quantity",
    "stock_coverage_days", "movement_category"
).orderBy("avg_daily_demand").show(20, truncate=False)

# ---------------------------------------------------------------------
# Delivery validation
# ---------------------------------------------------------------------
deliveries = spark.table("rusa_analytics_silver.deliveries")

delivery_validation = (
    deliveries
    .join(
        sales.select("order_id", "order_date"),
        on="order_id",
        how="left"
    )
    .withColumn(
        "delivery_days",
        datediff(col("delivery_date"), col("order_date"))
    )
)

print("=== DELIVERY VALIDATION ===")
print(
    "Missing matching orders:",
    delivery_validation.filter(col("order_date").isNull()).count()
)
print(
    "Missing delivery dates:",
    delivery_validation.filter(col("delivery_date").isNull()).count()
)
print(
    "Deliveries before order date:",
    delivery_validation.filter(
        col("delivery_date") < col("order_date")
    ).count()
)
print(
    "Negative delivery duration:",
    delivery_validation.filter(col("delivery_days") < 0).count()
)

avg_delivery_days = (
    delivery_validation
    .filter(col("delivery_days") >= 0)
    .agg(avg("delivery_days"))
    .collect()[0][0]
)
print(
    "Average delivery days:",
    round(float(avg_delivery_days), 2) if avg_delivery_days is not None else None
)
print("Delivery validation complete!")

# ---------------------------------------------------------------------
# Billing validation
# ---------------------------------------------------------------------
billing = spark.table("rusa_analytics_silver.billing")

billing_validation = (
    billing
    .join(
        sales.select("order_id", "order_date", "sales_amount"),
        on="order_id",
        how="left"
    )
)

print("=== BILLING VALIDATION ===")
print(
    "Missing matching orders:",
    billing_validation.filter(col("order_date").isNull()).count()
)
print(
    "Missing invoice dates:",
    billing_validation.filter(col("invoice_date").isNull()).count()
)
print(
    "Invalid invoice amounts:",
    billing_validation.filter(col("invoice_amount") < 0).count()
)
print(
    "Invoices before order date:",
    billing_validation.filter(
        col("invoice_date") < col("order_date")
    ).count()
)

avg_invoice_amount = (
    billing_validation
    .filter(col("invoice_amount") >= 0)
    .agg(avg("invoice_amount"))
    .collect()[0][0]
)
print(
    "Average invoice amount:",
    round(float(avg_invoice_amount), 2) if avg_invoice_amount is not None else None
)
print("Billing validation complete!")

print("\nDay 3 corrected refinement completed successfully.")

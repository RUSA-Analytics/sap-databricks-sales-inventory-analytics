# RUSA Data Dictionary

## customers
| Column | Description | Type | Key |
|---|---|---|---|
| customer_id | Unique customer identifier | string | PK |
| customer_name | Customer name | string | |
| region | Customer region | string | |
| city | Customer city | string | |
| customer_segment | Customer business segment | string | |

## products
| Column | Description | Type | Key |
|---|---|---|---|
| product_id | Unique product identifier | string | PK |
| product_name | Product name | string | |
| category | Product category | string | |
| unit_price | Selling price per unit | numeric | |
| supplier | Product supplier | string | |
| lead_time_days | Supplier replenishment lead time | integer | |
| reorder_level | Minimum stock level for replenishment | integer | |

## sales_orders
| Column | Description | Type | Key |
|---|---|---|---|
| order_id | Unique sales order identifier | string | PK |
| order_date | Date when order was placed | date | |
| customer_id | Customer placing the order | string | FK → customers |
| product_id | Product ordered | string | FK → products |
| quantity | Quantity ordered | integer | |
| order_status | Current order status | string | |
| unit_price | Price per unit at order time | numeric | |
| sales_amount | Quantity × unit price | numeric | |

## inventory
| Column | Description | Type | Key |
|---|---|---|---|
| inventory_id | Unique inventory record identifier | string | PK |
| product_id | Product stored in inventory | string | FK → products |
| warehouse_id | Warehouse identifier | string | |
| stock_quantity | Current available stock | integer | |
| reorder_level | Stock level at which replenishment is required | integer | |
| last_restock_date | Most recent restocking date | date | |

## deliveries
| Column | Description | Type | Key |
|---|---|---|---|
| delivery_id | Unique delivery record | string | PK |
| order_id | Related sales order | string | FK → sales_orders |
| delivery_date | Actual delivery date | date | |
| expected_delivery_date | Planned delivery date | date | |
| delivery_status | Current delivery status | string | |

## billing
| Column | Description | Type | Key |
|---|---|---|---|
| invoice_id | Unique invoice identifier | string | PK |
| order_id | Related sales order | string | FK → sales_orders |
| invoice_date | Invoice creation date | date | |
| invoice_amount | Billed amount | numeric | |
| payment_status | Current payment status | string | |

## Gold objects
- `sales_summary`: product-level sales aggregation.
- `inventory_summary`: product/warehouse inventory, demand, coverage, status and value.
- `fulfilment_summary`: order, delivery, fulfilment and billing information.
- `inventory_risk`: authoritative inventory-risk dataset with risk priority.
- `slow_moving_inventory`: movement classification using the documented 2.1 units/day threshold.
- `rejected_records`: validation/audit records containing failed record, rule, reason and timestamp.

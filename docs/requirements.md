---

# Project Requirements

This document defines the functional and technical requirements for the **SQL Server Data Warehouse & Analytics Project**.

---

## 🎯 Project Goals

* Build a modern **SQL Server data warehouse** following the **Bronze–Silver–Gold (Medallion) architecture**.
* Consolidate **ERP** and **CRM** data into a single, analytics-ready platform.
* Deliver actionable business insights through SQL-based analytics and reporting.
* Provide documentation and testing to ensure data quality and maintainability.

---

## 📂 Data Sources

1. **ERP System** (CSV files)

   * Sales transactions and product details.
   * Example fields: `transaction_id`, `product_id`, `customer_id`, `quantity`, `price`, `transaction_date`.

2. **CRM System** (CSV files)

   * Customer information and demographics.
   * Example fields: `customer_id`, `name`, `region`, `industry`, `signup_date`.

---

## 🏗️ Data Warehouse Design

### Bronze Layer

* Ingest raw CSV files into staging tables.
* Preserve source structure with minimal transformations.

### Silver Layer

* Apply data cleaning and standardization.
* Resolve duplicates, null values, and inconsistent formats.
* Join ERP and CRM data to create unified views.

### Gold Layer

* Create **business-friendly star schema** for analytics.
* Tables include:

  * `fact_sales` → transactional sales measures.
  * `dim_customers` → customer attributes.
  * `dim_products` → product details.
  * `dim_date` → time dimension for trend analysis.

---

## ⚙️ Technical Requirements

* **Platform:** SQL Server 2019/2022 (Express edition supported).
* **ETL:** T-SQL scripts (organized by Bronze, Silver, Gold).
* **Documentation:** ERD diagrams, data catalog, and naming conventions.
* **Testing:** Row count checks, referential integrity, and data quality validations.

---

## 📊 Analytics & Reporting

The solution should support SQL queries and dashboards that provide:

* **Customer Insights:**

  * Top regions and industries by revenue.
  * Customer acquisition trends.

* **Product Performance:**

  * Best- and worst-selling products.
  * Revenue by product line and category.

* **Sales Trends:**

  * Monthly/quarterly growth.
  * Seasonal or regional sales patterns.

---

## ✅ Deliverables

* SQL scripts for schema creation and ETL pipelines.
* Fully populated **Bronze, Silver, and Gold layers** in SQL Server.
* Star schema model with fact and dimension tables.
* Test scripts for validation.
* Documentation: ERD, data catalog, ETL flows, and naming standards.

---

## 🔒 Out of Scope

* Historical data loading (focus is on the most recent dataset only).
* Real-time or streaming ingestion.
* Advanced ML or predictive modeling.

---

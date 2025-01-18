import polars as pl
import duckdb
import numpy as np
from datetime import datetime, timedelta
from time import time
import os


def create_large_dataset(rows=100_000_000):
    """Create a large dataset for performance testing"""
    np.random.seed(42)

    # Generate dates
    start_date = datetime(2023, 1, 1)
    dates = [start_date + timedelta(seconds=x) for x in range(rows)]

    data = {
        'timestamp': dates,
        'store_id': np.random.randint(1, 100, size=rows),
        'product_id': np.random.randint(1, 1000, size=rows),
        'quantity': np.random.randint(1, 50, size=rows),
        'price': np.random.uniform(10, 1000, size=rows).round(2),
        'customer_id': np.random.randint(1, 10000, size=rows),
        'promotion': np.random.choice(['None', 'discount_10', 'discount_20', 'buy_one_get_one'], size=rows)
    }

    # Create Polars DataFrame and save as Parquet
    df = pl.DataFrame(data)
    df.write_parquet("sales_data.parquet")
    return df


def time_operation(func):
    """Decorator to measure execution time"""

    def wrapper(*args, **kwargs):
        start = time()
        result = func(*args, **kwargs)
        end = time()
        return result, end - start

    return wrapper


@time_operation
def polars_operations():
    """Perform data operations with Polars"""
    df = pl.read_parquet("sales_data.parquet")
    # Calculate revenue
    df = df.with_columns(
        (pl.col("quantity") * pl.col("price")).alias("revenue")
    )

    # 1. Store metrics
    store_metrics = df.group_by("store_id").agg([
        pl.col("revenue").sum().alias("total_revenue"),
        pl.col("revenue").mean().alias("avg_revenue"),
        pl.col("quantity").sum().alias("total_quantity"),
        pl.col("customer_id").n_unique().alias("unique_customers")
    ]).sort("total_revenue", descending=True)

    # 2. Hourly revenue
    hourly_revenue = df.group_by_dynamic(
        "timestamp",
        every="1h"
    ).agg([
        pl.col("revenue").sum().alias("hourly_revenue"),
        pl.col("quantity").sum().alias("hourly_quantity")
    ])

    # 3. Customer segmentation
    customer_metrics = df.group_by("customer_id").agg([
        pl.col("revenue").sum().alias("total_spend"),
        pl.col("quantity").sum().alias("total_items"),
        pl.len().alias("visit_count")
    ]).with_columns([
        pl.col("total_spend").mean().alias("avg_transaction_value")
    ]).sort("total_spend", descending=True)

    # 4. Promotion analysis
    promo_metrics = df.group_by("promotion").agg([
        pl.col("revenue").sum().alias("total_revenue"),
        pl.col("revenue").mean().alias("avg_revenue"),
        pl.len().alias("transaction_count")
    ])

    return {
        "store_metrics": store_metrics,
        "hourly_revenue": hourly_revenue,
        "customer_metrics": customer_metrics,
        "promo_metrics": promo_metrics
    }


@time_operation
def duckdb_operations():
    """Perform the same operations with DuckDB"""
    con = duckdb.connect()

    # Register the parquet file
    con.execute("CREATE OR REPLACE VIEW sales AS SELECT * FROM 'sales_data.parquet'")

    # Calculate revenue in a CTE
    revenue_cte = """
    WITH sales_revenue AS (
        SELECT *, quantity * price as revenue
        FROM sales
    )
    """

    # 1. Store metrics
    store_metrics = con.execute(f"""
        {revenue_cte}
        SELECT 
            store_id,
            SUM(revenue) as total_revenue,
            AVG(revenue) as avg_revenue,
            SUM(quantity) as total_quantity,
            COUNT(DISTINCT customer_id) as unique_customers
        FROM sales_revenue
        GROUP BY store_id
        ORDER BY total_revenue DESC
    """).df()

    # 2. Hourly revenue
    hourly_revenue = con.execute(f"""
        {revenue_cte}
        SELECT 
            date_trunc('hour', timestamp) as hour,
            SUM(revenue) as hourly_revenue,
            SUM(quantity) as hourly_quantity
        FROM sales_revenue
        GROUP BY date_trunc('hour', timestamp)
        ORDER BY hour
    """).df()

    # 3. Customer segmentation
    customer_metrics = con.execute(f"""
        {revenue_cte}
        SELECT 
            customer_id,
            SUM(revenue) as total_spend,
            SUM(quantity) as total_items,
            COUNT(*) as visit_count,
            AVG(revenue) as avg_transaction_value
        FROM sales_revenue
        GROUP BY customer_id
        ORDER BY total_spend DESC
    """).df()

    # 4. Promotion analysis
    promo_metrics = con.execute(f"""
        {revenue_cte}
        SELECT 
            promotion,
            SUM(revenue) as total_revenue,
            AVG(revenue) as avg_revenue,
            COUNT(*) as transaction_count
        FROM sales_revenue
        GROUP BY promotion
    """).df()

    con.close()

    return {
        "store_metrics": store_metrics,
        "hourly_revenue": hourly_revenue,
        "customer_metrics": customer_metrics,
        "promo_metrics": promo_metrics
    }


def run_comparison(rows=1_000_000):
    """Run and compare both implementations"""
    print(f"Creating dataset with {rows:,} rows...")
    df = create_large_dataset(rows)

    print("\nRunning Polars implementation...")
    polars_results, polars_time = polars_operations()

    print("\nRunning DuckDB implementation...")
    duckdb_results, duckdb_time = duckdb_operations()

    # Print sample results
    print("\nSample Results Comparison")
    print("\nTop 5 Stores by Revenue (Polars):")
    print(polars_results["store_metrics"].head())
    print("\nTop 5 Stores by Revenue (DuckDB):")
    print(duckdb_results["store_metrics"].head())

    # Print performance comparison
    print("\nPerformance Results:")
    print(f"{'Operation':20} {'Time (seconds)':>15}")
    print("-" * 35)
    print(f"{'Polars':20} {polars_time:>15.3f}")
    print(f"{'DuckDB':20} {duckdb_time:>15.3f}")
    print(f"{'Ratio':20} {polars_time / duckdb_time:>15.2f}x")

    # Cleanup
    if os.path.exists("sales_data.parquet"):
        os.remove("sales_data.parquet")


if __name__ == "__main__":
    run_comparison()

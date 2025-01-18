import pandas as pd
import polars as pl
import numpy as np
from time import time
from datetime import datetime, timedelta


def create_large_dataset(rows=1_000_000):
    """Create a large dataset for performance testing"""
    np.random.seed(42)

    # Generate dates
    start_date = datetime(2023, 1, 1)
    dates = [start_date + timedelta(seconds=x) for x in range(rows)]

    # Generate data
    data = {
        "timestamp": dates,
        "store_id": np.random.randint(1, 100, size=rows),
        "product_id": np.random.randint(1, 1000, size=rows),
        "quantity": np.random.randint(1, 50, size=rows),
        "price": np.random.uniform(10, 1000, size=rows).round(2),
        "customer_id": np.random.randint(1, 10000, size=rows),
        "promotion": np.random.choice(
            ["None", "discount_10", "discount_20", "buy_one_get_one"], size=rows
        ),
    }

    return data


def time_operation(func):
    """Decorator to measure execution time"""

    def wrapper(*args, **kwargs):
        start = time()
        result = func(*args, **kwargs)
        end = time()
        return result, end - start

    return wrapper


@time_operation
def pandas_operations(df):
    """Perform common data operations with pandas"""
    # Ensure timestamp is datetime
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    # Calculate revenue
    df["revenue"] = df["quantity"] * df["price"]

    # Group by store and calculate metrics
    store_metrics = (
        df.groupby("store_id")
        .agg({"revenue": ["sum", "mean"], "quantity": "sum", "customer_id": "nunique"})
        .round(2)
    )

    # Calculate daily revenue using timestamp index
    daily_revenue = df.groupby(df["timestamp"].dt.date)["revenue"].sum()

    # Filter and sort
    top_customers = (
        df.groupby("customer_id")["revenue"].sum().sort_values(ascending=False).head(10)
    )

    # Calculate promotion effectiveness
    promo_impact = df.groupby("promotion")["revenue"].agg(["mean", "count", "sum"])

    return {
        "store_metrics": store_metrics,
        "daily_revenue": daily_revenue,
        "top_customers": top_customers,
        "promo_impact": promo_impact,
    }


@time_operation
def polars_operations(df):
    """Perform the same operations with polars"""
    # Calculate revenue
    df = df.with_columns((pl.col("quantity") * pl.col("price")).alias("revenue"))

    # Group by store and calculate metrics
    store_metrics = df.group_by("store_id").agg(
        [
            pl.col("revenue").sum().round(2).alias("revenue_sum"),
            pl.col("revenue").mean().round(2).alias("revenue_mean"),
            pl.col("quantity").sum().alias("quantity_sum"),
            pl.col("customer_id").n_unique().alias("unique_customers"),
        ]
    )

    # Calculate daily revenue
    daily_revenue = df.group_by_dynamic("timestamp", every="1d").agg(
        pl.col("revenue").sum().alias("daily_revenue")
    )

    # Filter and sort
    top_customers = (
        df.group_by("customer_id")
        .agg(pl.col("revenue").sum().alias("total_revenue"))
        .sort("total_revenue", descending=True)
        .head(10)
    )

    # Calculate promotion effectiveness
    promo_impact = df.group_by("promotion").agg(
        [
            pl.col("revenue").mean().alias("mean"),
            pl.col("revenue").count().alias("count"),
            pl.col("revenue").sum().alias("sum"),
        ]
    )

    return {
        "store_metrics": store_metrics,
        "daily_revenue": daily_revenue,
        "top_customers": top_customers,
        "promo_impact": promo_impact,
    }


def run_comparison():
    """Run and compare both implementations"""
    # Create dataset
    print("Creating dataset...")
    data = create_large_dataset()

    # Pandas implementation
    print("\nRunning Pandas implementation...")
    df_pd = pd.DataFrame(data)
    pd_results, pd_time = pandas_operations(df_pd)

    # Polars implementation
    print("\nRunning Polars implementation...")
    df_pl = pl.DataFrame(data)
    pl_results, pl_time = polars_operations(df_pl)

    # Print results
    print("\nPerformance Results:")
    print(f"{'Operation':20} {'Time (seconds)':>15}")
    print("-" * 35)
    print(f"{'Pandas':20} {pd_time:>15.3f}")
    print(f"{'Polars':20} {pl_time:>15.3f}")
    print(f"{'Speedup':20} {pd_time / pl_time:>15.1f}x")


if __name__ == "__main__":
    run_comparison()

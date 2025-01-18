# Exercise: Data Processing with Pandas and Polars
# Time: 10 minutes
from pathlib import Path

import pandas as pd
import polars as pl


def pandas_analysis():
    """
    Exercise tasks:
    1. Read the sales_data.csv file
    2. Calculate total revenue (quantity * price) for each product
    3. Find the day with the highest sales
    """
    # TODO: Complete the analysis using pandas
    pass


def polars_analysis():
    """
    Exercise tasks:
    Perform the same analysis using Polars:
    1. Read the CSV file
    2. Calculate total revenue per product
    3. Find the day with the highest sales
    """
    # TODO: Complete the analysis using polars
    # 1.
    df = pl.read_csv(data_dir / 'sales_data.csv')

    # 2.
    product_revenue = df.with_columns(
       ( pl.col('quantity') * pl.col('price')).alias('revenue')
    ).group_by('product').agg(
        pl.col('revenue').sum().round(2).alias('total_revenue')
    )
    print("\nTotal Revenue by Product:")
    print(product_revenue)

    # 3.
    daily_sales = df.with_columns(
        (pl.col('quantity') * pl.col('price')).alias('revenue')
    ).group_by('date').agg(
        pl.col('revenue').sum().alias('daily_revenue')
    ).sort('daily_revenue', descending=True)

    best_day = daily_sales.head(1)
    print(f"\nBest Sales Day: {best_day['date'][0]} "
          f"(${best_day['daily_revenue'][0]})")


if __name__ == "__main__":
    print("Pandas Analysis:")
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)

    pandas_analysis()
    print("\nPolars Analysis:")
    polars_analysis()

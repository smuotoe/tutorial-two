# Solution: DuckDB Analysis using CSV data

import duckdb
from pathlib import Path


def duckdb_analysis():
    """Analyze sales data from CSV using DuckDB"""
    # Create connection
    con = duckdb.connect()

    # 1. Calculate total revenue by product
    print("\nTotal Revenue by Product:")
    data_dir = Path("data")
    product_revenue = con.execute("""
        WITH sales AS (
            SELECT * FROM read_csv_auto('data/sales_data.csv')
        )
        SELECT 
            product,
            ROUND(SUM(quantity * price), 2) as total_revenue
        FROM sales
        GROUP BY product
        ORDER BY total_revenue DESC
    """).df()
    print(product_revenue)

    # 2. Find day with highest sales
    print("\nBest Sales Day:")
    best_day = con.execute("""
        WITH sales AS (
            SELECT * FROM read_csv_auto('data/sales_data.csv')
        ),
        daily_sales AS (
            SELECT 
                date,
                ROUND(SUM(quantity * price), 2) as daily_revenue
            FROM sales
            GROUP BY date
        )
        SELECT *
        FROM daily_sales
        WHERE daily_revenue = (SELECT MAX(daily_revenue) FROM daily_sales)
    """).df()
    print(best_day)

    con.close()

    return product_revenue, best_day, weekly_avg


def verify_data_file():
    """Verify that the data file exists and show sample"""
    file_path = Path('data/sales_data.csv')
    if not file_path.exists():
        raise FileNotFoundError(f"Data file not found at {file_path}")

    con = duckdb.connect()
    print("First 5 rows of data:")
    sample = con.execute("""
        SELECT * 
        FROM read_csv_auto('data/sales_data.csv')
        LIMIT 5
    """).df()
    print(sample)
    con.close()


if __name__ == "__main__":
    try:
        # Verify data file exists and show sample
        verify_data_file()

        # Run analysis
        print("\nRunning analysis...")
        product_revenue, best_day, weekly_avg = duckdb_analysis()

    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please ensure the sales data file exists in the data directory.")
    except Exception as e:
        print(f"An error occurred: {e}")
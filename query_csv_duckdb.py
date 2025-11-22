"""
Simple utility script to run SQL queries against a CSV file using DuckDB.

Usage examples:
    # Show first rows
    python query_csv_duckdb.py --csv "c:/Users/Sajid/Desktop/Data analysis/2024_LoL_esports_match_data_from_OraclesElixir.csv"

    # Show columns & detected schema
    python query_csv_duckdb.py --csv "c:/Users/Sajid/Desktop/Data analysis/2024_LoL_esports_match_data_from_OraclesElixir.csv" --show-columns

    # Show sanitized SQL-safe column mapping
    python query_csv_duckdb.py --csv "c:/Users/Sajid/Desktop/Data analysis/2024_LoL_esports_match_data_from_OraclesElixir.csv" --show-safe

    # Count rows
    python query_csv_duckdb.py --csv "{csv}" --count

    # Example SQL: use existing columns from the CSV
    python query_csv_duckdb.py --csv "{csv}" --query "SELECT gameid, league, teamname, playername FROM read_csv_auto('{csv}') LIMIT 10"

    # If column names contain spaces or punctuation, use --sanitize and reference the sanitized alias
    python query_csv_duckdb.py --csv "{csv}" --sanitize --query "SELECT data.gameid, data.teamname FROM read_csv_auto('{csv}') AS data LIMIT 5"

The script uses DuckDB's read_csv_auto function, which detects column types and seeds the SQL table from the CSV directly.
"""

import argparse
import sys
import duckdb
import pprint
import re


def run_query(csv_path: str, query: str):
    # Connect to an in-memory DuckDB instance
    conn = duckdb.connect(database=':memory:')
    try:
        # Replace placeholder {csv} with the escaped path in SQL
        escaped = csv_path.replace("\\","\\\\")
        sql = query.format(csv=escaped)
        result_df = conn.execute(sql).df()
        return result_df
    finally:
        conn.close()


def show_columns(csv_path: str):
    conn = duckdb.connect(database=':memory:')
    try:
        # Run a LIMIT 0 query to obtain columns and types
        escaped = csv_path.replace('\\', '\\\\')
        sql = f"SELECT * FROM read_csv_auto('{escaped}') LIMIT 0"
        try:
            df = conn.execute(sql).df()
        except duckdb.Error:
            # If file is unreadable or DuckDB throws, return None
            return None
        # Build a DataFrame-like description of columns and dtypes
        import pandas as pd
        schema = pd.DataFrame({'column': df.columns, 'dtype': [str(dt) for dt in df.dtypes.values]})
        return schema
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description='Run SQL on a CSV file using DuckDB')
    parser.add_argument('--csv', required=True, help='Path to CSV file')
    parser.add_argument('--query', help='SQL query to run. Use read_csv_auto("{csv}") to load the CSV in your query')
    parser.add_argument('--count', action='store_true', help='Run a row count (SELECT COUNT(*) FROM read_csv_auto({csv}))')
    parser.add_argument('--show-safe', action='store_true', help='Show sanitized SQL-safe column name mapping for querying')
    parser.add_argument('--sanitize', action='store_true', help='Run the provided SQL query against a sanitized alias where columns are converted to SQL-safe names')
    parser.add_argument('--show-columns', action='store_true', help='Show column names and types')
    args = parser.parse_args()

    csv_path = args.csv
    escaped = csv_path.replace('\\','\\\\')

    if args.show_columns:
        df = show_columns(csv_path)
        if df is None or df.empty:
            print("No columns/empty file or unable to read columns.")
            sys.exit(1)
        print("Columns / Schema:")
        pprint.pprint(df.to_dict(orient='records'))
        return

    if args.show_safe:
        # Print sanitized name mapping
        from typing import Dict

        def sanitize_identifier(name: str) -> str:
            s = name.strip()
            # replace non-alphanumeric with underscores
            s = re.sub(r'[^0-9A-Za-z]+', '_', s)
            # collapse multiple underscores
            s = re.sub(r'__+', '_', s)
            s = s.strip('_').lower()
            if s == '':
                s = 'col'
            # leading digit -> prefix
            if re.match(r'^[0-9]', s):
                s = 'c_' + s
            return s

        schema = show_columns(csv_path)
        if schema is None or schema.empty:
            print('No columns/empty file or unable to read columns.')
            sys.exit(1)
        mapping = []
        for orig in schema['column'].tolist():
            safe = sanitize_identifier(orig)
            mapping.append({'original': orig, 'safe': safe})
        print('Column name mapping (original -> safe SQL identifier):')
        pprint.pprint(mapping)
        print('\nExample sanitized SQL and usage:')
        # build example SQL list select
        safe_cols = ', '.join([f'"{r["original"]}" as {r["safe"]}' for r in mapping[:20]])
        escaped_path = csv_path.replace('\\', '\\\\')
        snippet = f"(SELECT {safe_cols} FROM read_csv_auto('{escaped_path}')) AS data"
        print(f"Use this subquery in queries: {snippet}")
        print("Then run: SELECT data.safe_column FROM {0} LIMIT 5".format(snippet))
        return

    if args.count:
        query = "SELECT COUNT(*) as row_count FROM read_csv_auto('{csv}')"
        df = run_query(csv_path, query)
        print(df.to_string(index=False))
        return

    if args.query:
        # allow usage of {csv} placeholder in the query
        query_text = args.query
        if args.sanitize:
            # Build sanitized alias and replace read_csv_auto('{csv}') with a sanitized subselect
            schema = show_columns(csv_path)
            if schema is None or schema.empty:
                print('No columns/empty file or unable to read columns.')
                sys.exit(1)

            def sanitize_identifier(name: str) -> str:
                s = name.strip()
                s = re.sub(r'[^0-9A-Za-z]+', '_', s)
                s = re.sub(r'__+', '_', s)
                s = s.strip('_').lower()
                if s == '':
                    s = 'col'
                if re.match(r'^[0-9]', s):
                    s = 'c_' + s
                return s

            pairs = []
            for orig in schema['column'].tolist():
                safe = sanitize_identifier(orig)
                pairs.append((orig, safe))
            select_list = ', '.join([f'"{o}" AS {s}' for (o, s) in pairs])
            subquery = f"(SELECT {select_list} FROM read_csv_auto('{{csv}}'))"
            query_text = query_text.replace("read_csv_auto('{csv}')", subquery)

        df = run_query(csv_path, query_text)
        if df is None or df.empty:
            print("No rows returned or unable to read file; check your query and CSV path.")
            return
        print(df.head(50).to_string(index=False))
        return

    # Default behavior: show first few rows
    df = run_query(csv_path, "SELECT * FROM read_csv_auto('{csv}') LIMIT 10")
    print(df.to_string(index=False))


if __name__ == '__main__':
    main()

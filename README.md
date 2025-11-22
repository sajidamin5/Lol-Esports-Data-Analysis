# Query CSV files with SQL (DuckDB)

Use DuckDB (fast, serverless) to query CSV files with SQL directly — no import or ETL required.

## Setup (Windows PowerShell)

1. Create a Python virtual environment and activate it:

```powershell
python -m venv venv; .\venv\Scripts\Activate.ps1
```

2. Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

## Usage

Basic: show the first 10 rows
``
```powershell
python query_csv_duckdb.py --csv "c:/Users/Sajid/Desktop/Lol Esports Data Analysis/2024_LoL_esports_match_data_from_OraclesElixir.csv"
```

Show columns & detected schema

```powershell
python query_csv_duckdb.py --csv "c:/Users/Sajid/Desktop/Lol Esports Data Analysis/2024_LoL_esports_match_data_from_OraclesElixir.csv" --show-columns
```

Count rows

```powershell
python query_csv_duckdb.py --csv "c:/Users/Sajid/Desktop/Lol Esports Data Analysis/2024_LoL_esports_match_data_from_OraclesElixir.csv" --count
```

Run an arbitrary SQL query (use read_csv_auto('{csv}') to load CSV):

```powershell
python query_csv_duckdb.py --csv "c:/Users/Sajid/Desktop/Lol Esports Data Analysis/2024_LoL_esports_match_data_from_OraclesElixir.csv" --query "SELECT gameid, league, teamname, playername, champion, gamelength, result FROM read_csv_auto('{csv}') LIMIT 10"

If your CSV contains columns with spaces, punctuation or unusual names, use the `--show-safe` option to see a mapping of original names to SQL-safe identifiers:

```powershell
python query_csv_duckdb.py --csv "c:/Users/Sajid/Desktop/Lol Esports Data Analysis/2024_LoL_esports_match_data_from_OraclesElixir.csv" --show-safe
```

To run queries using SQL-safe column names, use `--sanitize` along with `--query`. The script will alias original columns to sanitized identifiers for you and embed them into the query. Example:

```powershell
python query_csv_duckdb.py --csv "c:/Users/Sajid/Desktop/Lol Esports Data Analysis/2024_LoL_esports_match_data_from_OraclesElixir.csv" --sanitize --query "SELECT data.gameid, data.league, data.playername FROM read_csv_auto('{csv}') AS data LIMIT 10"
```
```

## Notes & tips

- DuckDB's `read_csv_auto()` infers types automatically. For very large CSVs, DuckDB streams the file efficiently.
- If you prefer SQLite, pandasql, or a GUI, I can provide alternative instructions.

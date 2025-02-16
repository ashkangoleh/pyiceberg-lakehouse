import sys
import duckdb
from pyiceberg.catalog import load_catalog

def main():
    config = {"uri": "sqlite:///catalog.db", "warehouse": "iceberg_warehouse"}
    catalog = load_catalog("sqlite", **config)
    table_identifier = "default.my_iceberg_table"
    try:
        table = catalog.load_table(table_identifier)
    except Exception as e:
        print(f"Failed to load table '{table_identifier}': {e}")
        sys.exit(1)
    metadata_file = table.metadata_location
    if not metadata_file:
        print("No metadata file found. Ensure that data was appended to the table.")
        sys.exit(1)
    print("Latest metadata file:", metadata_file)
    metadata_file_duckdb = metadata_file.replace("\\", "/")
    print("Using metadata file for scan:", metadata_file_duckdb)
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL iceberg;")
    con.execute("LOAD iceberg;")
    query = f"""
        SELECT *
        FROM iceberg_scan('{metadata_file_duckdb}') limit 10;
    """
    try:
        df = con.execute(query).fetchdf()
        print("Iceberg Table Query Result:")
        print(df)
    except Exception as e:
        print("Error executing query:", e)
        sys.exit(1)

if __name__ == "__main__":
    main()
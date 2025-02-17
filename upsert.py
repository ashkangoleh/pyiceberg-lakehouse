# upsert.py
import os
import glob
import shutil
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from pyiceberg.catalog import load_catalog

# Define a forced schema that matches your new data.
READ_SCHEMA = pa.schema([
    pa.field("id", pa.int32(), nullable=False),
    pa.field("group", pa.string(), nullable=False),
    pa.field("value1", pa.float32(), nullable=False),
    pa.field("value2", pa.int32(), nullable=False)
])

def read_existing_data(data_dir):
    """
    Reads all Parquet files under data_dir and returns a concatenated PyArrow Table.
    For each file, the "group" column is force-converted to a plain string.
    Returns None if no files are found.
    """
    files = glob.glob(os.path.join(data_dir, "**", "*.parquet"), recursive=True)
    if not files:
        return None
    tables = []
    for f in files:
        try:
            table = pq.read_table(f)
            # Force-convert the "group" column to a plain string.
            idx = table.schema.get_field_index("group")
            group_pylist = table["group"].to_pylist()
            new_group_col = pa.array(group_pylist, type=pa.string())
            table = table.set_column(idx, "group", new_group_col)
            table = table.cast(READ_SCHEMA)
            tables.append(table)
        except Exception as e:
            print(f"Error reading or casting file {f}: {e}")
    if not tables:
        return None
    return pa.concat_tables(tables)

def merge_tables(existing_table: pa.Table, new_table: pa.Table, key: str) -> pa.Table:
    """
    Merge existing_table and new_table using the specified key column.
    For duplicate keys, the new_table row will override the existing one.
    This implementation converts tables to pandas DataFrames for simplicity.
    """
    new_df = new_table.to_pandas()
    if existing_table is not None:
        existing_df = existing_table.to_pandas()
        merged_df = pd.concat([existing_df, new_df]).drop_duplicates(subset=[key], keep="last")
    else:
        merged_df = new_df
    return pa.Table.from_pandas(merged_df, preserve_index=False)

def upsert_new_data(new_data: pa.Table, catalog_config: dict, namespace: str,
                    table_name: str, data_dir: str, key: str) -> None:
    """
    Performs an upsert of new_data into the Iceberg table.
    Reads existing data from data_dir, merges with new_data on the given key,
    and overwrites the table with the merged result.
    """
    # Load the catalog and the existing table.
    cat_type = catalog_config.pop("catalog_type", "sql")
    catalog = load_catalog(cat_type, **catalog_config)
    table_identifier = f"{namespace}.{table_name}"
    table = catalog.load_table(table_identifier)
    
    # Read existing data using the forced schema conversion.
    existing_data = read_existing_data(data_dir)
    merged_data = merge_tables(existing_data, new_data, key)
    
    # Write the merged data to a temporary directory.
    temp_dir = data_dir + "_upsert_temp"
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir, exist_ok=True)
    output_file = os.path.join(temp_dir, "data.parquet")
    pq.write_table(merged_data, output_file)
    
    # Overwrite the table by passing the merged Arrow Table.
    table.overwrite(merged_data)
    print(f"Upsert committed. Merged data now contains {merged_data.num_rows} rows.")
    shutil.rmtree(temp_dir)
if __name__ == "__main__":
    # Catalog configuration: adjust the URI, credentials, and warehouse as needed.
    catalog_config = {
        "catalog_type": "sqlite",
        "uri": "sqlite:///catalog.db",
        "warehouse": "iceberg_warehouse",
        "init_catalog_tables": "true"
    }
    namespace = "default"
    table_name = "my_iceberg_table"
    data_dir = "./output_partitioned_parquet"  # Same directory used in your pipeline
    key = "id"  # Key column for upsert

    # For demonstration, create a dummy new_data table.
    schema = pa.schema([
        pa.field("id", pa.int32(), nullable=False),
        pa.field("group", pa.string(), nullable=False),
        pa.field("value1", pa.float32(), nullable=False),
        pa.field("value2", pa.int32(), nullable=False)
    ])
    new_data = pa.Table.from_pydict({
        "id": [1, 2, 3, 4],
        "group": ["A", "B", "A", "C"],
        "value1": [15.5, 25.5, 35.0, 45.0],
        "value2": [111, 230, 410, 410]
    }, schema=schema)
    
    upsert_new_data(new_data, catalog_config, namespace, table_name, data_dir, key)

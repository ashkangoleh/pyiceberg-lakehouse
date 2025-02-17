import os
import glob
import shutil
import sys
import pyarrow.parquet as pq
import pyarrow as pa
import pandas as pd
from pyiceberg.catalog import load_catalog

READ_SCHEMA = pa.schema([
    pa.field("id", pa.int32(), nullable=False),
    pa.field("group", pa.string(), nullable=False),
    pa.field("value1", pa.float32(), nullable=False),
    pa.field("value2", pa.int32(), nullable=False)
])

def read_existing_data(data_dir):
    files = glob.glob(os.path.join(data_dir, "**", "*.parquet"), recursive=True)
    if not files:
        return None
    tables = []
    for f in files:
        try:
            table = pq.read_table(f)
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

def read_new_data(new_file):
    try:
        table = pq.read_table(new_file)
        idx = table.schema.get_field_index("group")
        group_pylist = table["group"].to_pylist()
        new_group_col = pa.array(group_pylist, type=pa.string())
        return table.set_column(idx, "group", new_group_col).cast(READ_SCHEMA)
    except Exception as e:
        print(f"Error reading new file {new_file}: {e}")
        sys.exit(1)

def merge_tables(existing_table, new_table, key):
    new_df = new_table.to_pandas()
    if existing_table is not None:
        existing_df = existing_table.to_pandas()
        merged_df = pd.concat([existing_df, new_df]).drop_duplicates(subset=[key], keep="last")
    else:
        merged_df = new_df
    return pa.Table.from_pandas(merged_df, preserve_index=False)

def upsert_parquet(new_file, catalog_config, namespace, table_name, data_dir, key):
    cat_type = catalog_config.pop("catalog_type", "sql")
    catalog = load_catalog(cat_type, **catalog_config)
    table_identifier = f"{namespace}.{table_name}"
    table = catalog.load_table(table_identifier)
    existing_data = read_existing_data(data_dir)
    new_data = read_new_data(new_file)
    merged_data = merge_tables(existing_data, new_data, key)
    temp_dir = data_dir + "_upsert_temp"
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
    os.makedirs(temp_dir, exist_ok=True)
    output_file = os.path.join(temp_dir, "data.parquet")
    pq.write_table(merged_data, output_file)
    table.overwrite(merged_data)
    print(f"Upsert committed. Merged data now contains {merged_data.num_rows} rows.")
    shutil.rmtree(temp_dir)

if __name__ == "__main__":
    new_parquet_file = './new_large_dataset.parquet'
    catalog_config = {
        "catalog_type": "sqlite",
        "uri": "sqlite:///catalog.db",
        "warehouse": "iceberg_warehouse",
        "init_catalog_tables": "true"
    }
    namespace = "default"
    table_name = "my_iceberg_table"
    data_dir = "./output_partitioned_parquet"
    key = "id"
    upsert_parquet(new_parquet_file, catalog_config, namespace, table_name, data_dir, key)

import os
import sys
import polars as pl
from pyiceberg.catalog import load_catalog

def read_existing_data_lazy(data_dir):
    """
    Read existing data from a partitioned Parquet dataset stored in a directory
    tree rooted at `data_dir`.

    Returns a lazy DataFrame with columns cast to the types used in the
    Iceberg table created in the main pipeline.

    Parameters
    ----------
    data_dir : str
        Directory containing the partitioned Parquet dataset

    Returns
    -------
    lf : polars.LazyFrame
        A lazy DataFrame containing the existing data
    """

    pattern = os.path.join(data_dir, "**", "*.parquet")
    lf = pl.scan_parquet(pattern)
    return lf.with_columns([
        pl.col("id").cast(pl.Int32),
        pl.col("group").cast(pl.Utf8),
        pl.col("value1").cast(pl.Float32),
        pl.col("value2").cast(pl.Int32)
    ])

def read_new_data_lazy(new_file):
    """
    Read new data from a Parquet file and return a lazy DataFrame with columns
    cast to the types used in the Iceberg table created in the main pipeline.

    Parameters
    ----------
    new_file : str
        Path to the new Parquet file

    Returns
    -------
    lf : polars.LazyFrame
        A lazy DataFrame containing the new data
    """
    if not os.path.exists(new_file):
        print(f"Error: New file {new_file} not found.")
        sys.exit(1)
    lf = pl.scan_parquet(new_file)
    return lf.with_columns([
        pl.col("id").cast(pl.Int32),
        pl.col("group").cast(pl.Utf8),
        pl.col("value1").cast(pl.Float32),
        pl.col("value2").cast(pl.Int32)
    ])

def merge_lazy(existing_lf, new_lf, key):
    """
    Merge two lazy DataFrames into one, taking the last value for each key
    when there are duplicate keys.

    Parameters
    ----------
    existing_lf : polars.LazyFrame or None
        The existing data as a lazy DataFrame
    new_lf : polars.LazyFrame
        The new data as a lazy DataFrame
    key : str
        The key to group by

    Returns
    -------
    merged_lf : polars.LazyFrame
        The merged lazy DataFrame
    """
    if existing_lf is None:
        return new_lf
    merged_lf = pl.concat([existing_lf, new_lf])
    merged_lf = merged_lf.group_by(key).agg(pl.all().last())
    return merged_lf

def upsert_parquet_lazy(new_file, catalog_config, namespace, table_name, data_dir, key):
    """
    Performs an upsert of new data from a Parquet file into the existing Iceberg table.
    Reads existing data from data_dir, merges with new data on the given key, and overwrites
    the table with the merged result.

    Parameters
    ----------
    new_file : str
        Path to the new Parquet file
    catalog_config : dict
        Catalog configuration
    namespace : str
        Namespace of the Iceberg table
    table_name : str
        Name of the Iceberg table
    data_dir : str
        Directory containing partitioned Parquet files
    key : str
        Key to group by when merging

    Returns
    -------
    None
    """
    # Load the catalog and the existing table.
    cat_type = catalog_config.pop("catalog_type", "sqlite")
    catalog = load_catalog(cat_type, **catalog_config)

    # Read existing data using the forced schema conversion.
    table_identifier = f"{namespace}.{table_name}"
    table = catalog.load_table(table_identifier)
    try:
        # Read existing data using the forced schema conversion.
        existing_lf = read_existing_data_lazy(data_dir)
    except Exception as e:
        print(f"Error reading existing data: {e}")
        existing_lf = None
    # Merge and overwrite the table.
    new_lf = read_new_data_lazy(new_file)
    merged_lazy = merge_lazy(existing_lf, new_lf, key)
    merged_df = merged_lazy.collect()
    merged_arrow = merged_df.to_arrow()
    table.overwrite(merged_arrow)
    print(f"Upsert committed. Merged data now contains {merged_arrow.num_rows} rows.")

if __name__ == "__main__":
    new_parquet_file = 'data/new3_large_dataset.parquet'
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
    upsert_parquet_lazy(new_parquet_file, catalog_config, namespace, table_name, data_dir, key)

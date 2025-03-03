"""
This module implements an IcebergPipeline that partitions data in memory based on a partition column.
Instead of writing partitioned Parquet files to an output directory, it filters the data in memory and
appends each partition’s data directly to an Iceberg table using a defined partition spec.
This avoids the overhead of writing partitioned files and speeds up the process.
"""

import os
import time
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField, IntegerType, StringType, FloatType
from pyiceberg.partitioning import PartitionSpec, PartitionField, INITIAL_PARTITION_SPEC_ID, PARTITION_FIELD_ID_START
from pyiceberg.transforms import IdentityTransform

class IcebergPipeline:
    def __init__(self, parquet_input="./data/new_large_dataset.parquet",
                 catalog_config=None,
                 namespace="default", 
                 table_name="my_iceberg_table",
                 partition_field_name="group"):
        """
        Initialize an IcebergPipeline.

        Parameters
        ----------
        parquet_input : str, optional
            Input Parquet file (default is "./data/large_dataset.parquet")
        catalog_config : dict, optional
            Configuration for the Iceberg catalog (default uses preset configuration)
        namespace : str, optional
            Namespace for the Iceberg catalog (default is "default")
        table_name : str, optional
            Name of the Iceberg table (default is "my_iceberg_table")
        partition_field_name : str, optional
            Name of the field to partition on (default is "group")
        """
        self.parquet_input = parquet_input
        self.namespace = namespace
        self.table_name = table_name
        self.partition_field_name = partition_field_name
        if catalog_config is None:
            self.catalog_config = {
                "catalog_type": "sqlite",
                "uri": "sqlite:///catalog.db",
                "warehouse": "iceberg_warehouse"
            }
        else:
            self.catalog_config = catalog_config

    def get_catalog(self):
        """
        Retrieve the Iceberg catalog based on the provided configuration.

        Returns
        -------
        catalog : pyiceberg.catalog.Catalog
            An Iceberg catalog object.
        """
        config = self.catalog_config.copy()
        catalog_type = config.pop("catalog_type", "sqlite")
        return load_catalog(catalog_type, **config)
    
    def infer_schema_from_table(self, arrow_table: pa.Table) -> Schema:
        """
        Infer the Iceberg schema from an in-memory Arrow table.

        Parameters
        ----------
        arrow_table : pa.Table
            The input Arrow table read from the Parquet file.

        Returns
        -------
        schema : pyiceberg.schema.Schema
            An inferred Iceberg schema.
        """
        fields = []
        fid = 1
        for field in arrow_table.schema:
            if pa.types.is_integer(field.type):
                t = IntegerType
            elif pa.types.is_floating(field.type):
                t = FloatType
            elif pa.types.is_string(field.type):
                t = StringType
            else:
                t = StringType
            fields.append(NestedField(fid, field.name, t(), required=not field.nullable))
            fid += 1
        return Schema(*fields)
    
    def run_pipeline_in_memory(self):
        """
        Run the full pipeline in memory:
        1. Read the input Parquet file using PyArrow.
        2. Partition the data by filtering the in-memory table based on the partition column.
        3. Create the Iceberg catalog, namespace, and table with a partition specification.
        4. Append each partition's data directly to the Iceberg table without writing partitioned files to disk.
        """
        # Read the entire input Parquet file into an Arrow table
        arrow_table = pq.read_table(self.parquet_input)
        partition_col = self.partition_field_name

        # Get unique partition values from the partition column
        unique_vals = arrow_table.column(partition_col).unique().to_pylist()
        print(f"Found unique partition values: {unique_vals}")
        
        # Load or create the Iceberg catalog and ensure the warehouse directory exists
        catalog = self.get_catalog()
        os.makedirs(self.catalog_config["warehouse"], exist_ok=True)
        
        # Infer the schema from the in-memory Arrow table
        schema = self.infer_schema_from_table(arrow_table)
        pfid = None
        for field in schema.fields:
            if field.name == partition_col:
                pfid = field.field_id
                break
        if pfid is None:
            raise ValueError(f"Partition field '{partition_col}' not found in schema.")
        
        # Create partition field and partition specification using Identity transform
        partition_field = PartitionField(
            name=partition_col,
            source_id=pfid,
            field_id=PARTITION_FIELD_ID_START,
            transform=IdentityTransform()
        )
        spec = PartitionSpec(spec_id=INITIAL_PARTITION_SPEC_ID, fields=(partition_field,))
        
        # Create the namespace and table
        try:
            catalog.create_namespace(self.namespace)
            print(f"Namespace '{self.namespace}' created.")
        except Exception as e:
            print(f"Namespace '{self.namespace}' may already exist: {e}")
        table_identifier = f"{self.namespace}.{self.table_name}"
        table = catalog.create_table(table_identifier, schema=schema, partition_spec=spec)
        print(f"Iceberg table {table_identifier} created with partition spec on '{partition_col}'.")
        
        # Define a forced PyArrow schema that matches the Iceberg table's expected types.
        # Adjust the types as needed to resolve mismatches (e.g., int32 vs int64, float32 vs float64).
        forced_schema = pa.schema([
            pa.field("id", pa.int32(), nullable=False),
            pa.field("group", pa.string(), nullable=False),
            pa.field("value1", pa.float32(), nullable=False),
            pa.field("value2", pa.int32(), nullable=False)
        ])
        
        # Append each partition's data to the table directly in memory
        for val in unique_vals:
            # Build filter condition for the partition column
            condition = pc.equal(arrow_table[partition_col], pa.scalar(val))
            # Filter the table in memory
            filtered_table = pc.filter(arrow_table, condition)
            # Cast the filtered table to the forced schema to resolve type mismatches
            filtered_table = filtered_table.cast(forced_schema)
            table.append(filtered_table)
            print(f"Appended partition {partition_col} = {val} with {filtered_table.num_rows} rows.")
        
        print("\n--- Iceberg Table Metadata ---")
        print("Schema:")
        print(table.schema())
        self.table = table
        self.catalog = catalog

    def print_snapshot_history(self):
        """
        Print the snapshot history of the Iceberg table.
        """
        if not hasattr(self, "table"):
            print("Table not loaded. Run run_pipeline_in_memory() first.")
            return
        print("\nSnapshot History:")
        for snap in self.table.history():
            print(snap)

if __name__ == "__main__":
    start_time = time.time()
    pipeline = IcebergPipeline()
    pipeline.run_pipeline_in_memory()
    pipeline.print_snapshot_history()
    elapsed_time = time.time() - start_time
    print(f"\nTotal load time: {elapsed_time:.2f} seconds")

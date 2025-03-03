"""
This module implements an IcebergPipeline that partitions a Parquet file using an unstructured folder approach.
Instead of creating separate folders for each partition value, all partitioned files are saved in a single output
directory with filenames that include the partition value. The partition information is maintained in the metadata
of the Iceberg table.
"""

import os
import glob
import pyarrow.parquet as pq
import pyarrow as pa
import polars as pl  # using Polars instead of Daft
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField, IntegerType, StringType, FloatType
from pyiceberg.partitioning import PartitionSpec, PartitionField, INITIAL_PARTITION_SPEC_ID, PARTITION_FIELD_ID_START
from pyiceberg.transforms import IdentityTransform

class IcebergPipeline:
    def __init__(self, parquet_input="./data/large_dataset.parquet",
                 output_dir="./output_partitioned_parquet",
                 catalog_config=None,
                 namespace="default", 
                 table_name="my_iceberg_table",
                 partition_field_name="group"):
        """
        Initialize an IcebergPipeline.

        Parameters
        ----------
        parquet_input : str, optional
            Input Parquet file (default is "large_dataset.parquet")
        output_dir : str, optional
            Directory where the partitioned Parquet files will be saved (default is "./output_partitioned_parquet")
        catalog_config : dict, optional
            Configuration for the Iceberg catalog (default uses pre-set configuration)
        namespace : str, optional
            Namespace for the Iceberg catalog (default is "default")
        table_name : str, optional
            Name of the Iceberg table (default is "my_iceberg_table")
        partition_field_name : str, optional
            Name of the field to partition on (default is "group")
        """
        self.parquet_input = parquet_input
        self.output_dir = output_dir
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
            An Iceberg catalog object
        """
        config = self.catalog_config.copy()
        catalog_type = config.pop("catalog_type", "sqlite")
        return load_catalog(catalog_type, **config)
    
    def infer_schema(self):
        """
        Infer the Iceberg schema from one of the partitioned output files.

        Returns
        -------
        schema : pyiceberg.schema.Schema
            An inferred Iceberg schema based on the Parquet file sample.
        """
        # Search for Parquet files in the output directory
        sample_files = glob.glob(os.path.join(self.output_dir, "*.parquet"))
        if not sample_files:
            raise ValueError("No Parquet files found.")
        sample_schema = pq.read_schema(sample_files[0])
        fields = []
        fid = 1
        for field in sample_schema:
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
    
    def run_pipeline(self):
        """
        Run the full pipeline:
        1. Read the input Parquet file using Polars.
        2. Partition the data by the specified column and save files in a single directory with unique filenames.
        3. Load or create the Iceberg catalog and infer the schema.
        4. Create the namespace and Iceberg table with the inferred schema and partition specification.
        5. Read the partitioned Parquet files, apply a forced schema, and append the data to the Iceberg table.
        """
        # Read the input Parquet file
        df = pl.read_parquet(self.parquet_input)
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Get unique values from the partition column (e.g., "group")
        unique_vals = df.select(self.partition_field_name).unique().to_series().to_list()
        
        # Save partition files in a single directory with unique filenames including partition value
        for val in unique_vals:
            partition_df = df.filter(pl.col(self.partition_field_name) == val)
            output_file = os.path.join(self.output_dir, f"data_{self.partition_field_name}_{val}.parquet")
            partition_df.write_parquet(output_file)
        
        print(f"Parquet files have been saved in directory: {self.output_dir}")
        
        # Load or create the catalog
        catalog = self.get_catalog()
        os.makedirs(self.catalog_config["warehouse"], exist_ok=True)
        schema = self.infer_schema()
        pfid = None
        for field in schema.fields:
            if field.name == self.partition_field_name:
                pfid = field.field_id
                break
        if pfid is None:
            raise ValueError(f"Partition field '{self.partition_field_name}' not found in the schema.")
        
        # Create partition field and partition specification
        partition_field = PartitionField(
            name=self.partition_field_name,
            source_id=pfid,
            field_id=PARTITION_FIELD_ID_START,
            transform=IdentityTransform()
        )
        spec = PartitionSpec(spec_id=INITIAL_PARTITION_SPEC_ID, fields=(partition_field,))

        try:
            catalog.create_namespace(self.namespace)
            print(f"Namespace '{self.namespace}' created.")
        except Exception as e:
            print(f"Namespace '{self.namespace}' may already exist: {e}")

        table_identifier = f"{self.namespace}.{self.table_name}"
        table = catalog.create_table(table_identifier, schema=schema, partition_spec=spec)
        print(f"Iceberg table {table_identifier} created with partition spec on '{self.partition_field_name}'.")

        # Search for Parquet files in the output directory
        parquet_files = glob.glob(os.path.join(self.output_dir, "*.parquet"))
        print(f"Found {len(parquet_files)} Parquet files to register.")

        # Define a forced schema for reading files
        read_schema = pa.schema([
            pa.field("id", pa.int32(), nullable=False),
            pa.field("group", pa.string(), nullable=False),
            pa.field("value1", pa.float32(), nullable=False),
            pa.field("value2", pa.int32(), nullable=False)
        ])
        # Read the Parquet files and append data to the Iceberg table
        for file_path in parquet_files:
            try:
                pf = pq.ParquetFile(file_path)
                arrow_table = pf.read()
                # Check and decode if the partition field is a dictionary type
                gf = arrow_table.schema.field(self.partition_field_name)
                if pa.types.is_dictionary(gf.type):
                    idx = arrow_table.schema.get_field_index(self.partition_field_name)
                    arrow_table = arrow_table.set_column(
                        idx, 
                        self.partition_field_name,
                        arrow_table[self.partition_field_name].dictionary_decode()
                    )
                arrow_table = arrow_table.cast(read_schema)
            except Exception as e:
                print(f"Error reading or casting file {file_path}: {e}")
                continue
            table.append(arrow_table)
            print(f"Appended data from file: {file_path} (rows: {arrow_table.num_rows}, size: {os.path.getsize(file_path)} bytes)")

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
            print("Table not loaded. Run run_pipeline() first.")
            return
        print("\nSnapshot History:")
        for snap in self.table.history():
            print(snap)

if __name__ == "__main__":
    pipeline = IcebergPipeline()
    pipeline.run_pipeline()
    pipeline.print_snapshot_history()

import os
import glob
import pyarrow.parquet as pq
import pyarrow as pa
import polars as pl
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField, IntegerType, StringType, FloatType
from pyiceberg.partitioning import PartitionSpec, PartitionField, INITIAL_PARTITION_SPEC_ID, PARTITION_FIELD_ID_START
from pyiceberg.transforms import IdentityTransform

def infer_polars_schema(parquet_pattern):
    """
    Infer a Polars schema for the table from one of the partitioned output files.

    Returns
    -------
    schema : dict
        A dictionary mapping column names to Polars types
    """
    # Sample files by globbing for Parquet files under the output directory.
    files = glob.glob(parquet_pattern)
    if not files:
        raise ValueError(f"No files found matching {parquet_pattern}")
    sample_schema = pq.read_schema(files[0])
    mapping = {}
    # Loop over the fields in the sample schema and create a NestedField for each one.
    for field in sample_schema:
        if pa.types.is_integer(field.type):
            # Check bit width to decide whether to use Int32 or Int64
            if field.type.bit_width == 32:
                mapping[field.name] = pl.Int32
            elif field.type.bit_width == 64:
                mapping[field.name] = pl.Int64
            else:
                mapping[field.name] = pl.Int64  # fallback
        elif pa.types.is_floating(field.type):
            # Use float32 if possible, otherwise float64
            mapping[field.name] = pl.Float32 if field.type == pa.float32() else pl.Float64
        elif pa.types.is_string(field.type):
            mapping[field.name] = pl.Utf8
        else:
            mapping[field.name] = pl.Utf8  # fallback for unsupported types
    # Create a dictionary mapping column names to Polars types
    return mapping

class IcebergPipeline:
    def __init__(self, parquet_input="data/*.parquet",  # supports glob pattern for multiple files
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
            Input Parquet file pattern, supports glob pattern for multiple files, 
            by default "data/*.parquet"
        output_dir : str, optional
            Output directory for partitioned Parquet files, by default "./output_partitioned_parquet"
        catalog_config : dict, optional
            Configuration for the Iceberg catalog, by default None (uses default values)
        namespace : str, optional
            Namespace for the Iceberg catalog, by default "default"
        table_name : str, optional
            Name of the Iceberg table, by default "my_iceberg_table"
        partition_field_name : str, optional
            Name of the field to partition on, by default "group"
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
        Get an Iceberg catalog object from the configuration stored in the `catalog_config` field.

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
        Infer a PyIceberg schema for the table from one of the partitioned output files.

        Returns
        -------
        schema : pyiceberg.schema.Schema
            An Iceberg schema object
        """
        # Sample files by globbing for Parquet files under the output directory.
        sample_files = glob.glob(os.path.join(self.output_dir, "**", "*.parquet"), recursive=True)
        if not sample_files:
            raise ValueError("No parquet files found for schema inference.")
        sample_schema = pq.read_schema(sample_files[0])
        fields = []
        fid = 1
        for field in sample_schema:
            # For integers, we assume the sample file's bit width is what we want (here we use IntegerType, which is 32-bit)
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
        Run the pipeline to create an Iceberg table from the input Parquet file.

        The pipeline consists of the following steps:

        1. Dynamically infer the target Polars schema mapping from input files.
        2. Create a lazy frame for each file individually, enforcing the inferred cast.
        3. Concatenate all lazy frames.
        4. Determine unique partition values in a memory-efficient way.
        5. Write partitioned files for each partition value.
        6. Load or create the Iceberg catalog.
        7. Create the namespace (if it doesn't exist already).
        8. Create the Iceberg table with the inferred schema and partition spec.
        9. Register the partitioned files with the Iceberg table.

        After running this pipeline, the `table` and `catalog` instance variables are set.
        """
        # Dynamically infer the target Polars schema mapping from input files
        schema_mapping = infer_polars_schema(self.parquet_input)
        
        # Create a lazy frame for each file individually, enforcing the inferred cast,
        # then concatenate all lazy frames.
        files = glob.glob(self.parquet_input)
        lazy_frames = []
        # Loop over the files and create a lazy frame for each one
        for f in files:
            # Read the Parquet file as a lazy frame
            lf = pl.scan_parquet(f)
            # Enforce the inferred cast
            for col, dtype in schema_mapping.items():
                # Cast the column to the inferred type
                lf = lf.with_columns(pl.col(col).cast(dtype))
            lazy_frames.append(lf)
        # Concatenate all lazy frames
        df_lazy = pl.concat(lazy_frames)
        
        # Determine unique partition values in a memory-efficient way
        unique_vals = df_lazy.select(self.partition_field_name).unique().collect().to_series().to_list()
        os.makedirs(self.output_dir, exist_ok=True)
        # Write partitioned files for each partition value
        for val in unique_vals:
            partition_dir = os.path.join(self.output_dir, f"{self.partition_field_name}={val}")
            os.makedirs(partition_dir, exist_ok=True)
            # Lazily filter and then collect only the partitioned subset
            partition_df = df_lazy.filter(pl.col(self.partition_field_name) == val).collect()
            output_file = os.path.join(partition_dir, "data.parquet")
            partition_df.write_parquet(output_file)
            print(f"Wrote partition for {self.partition_field_name}={val} to {output_file}")

        # Load or create the Iceberg catalog
        catalog = self.get_catalog()
        os.makedirs(self.catalog_config["warehouse"], exist_ok=True)
        schema = self.infer_schema()
        pfid = None
        for field in schema.fields:
            if field.name == self.partition_field_name:
                pfid = field.field_id
                break
        if pfid is None:
            raise ValueError(f"Partition field '{self.partition_field_name}' not found in inferred schema.")
        # Create the partition spec
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

        parquet_files = glob.glob(os.path.join(self.output_dir, "**", "*.parquet"), recursive=True)
        print(f"Found {len(parquet_files)} Parquet files to register.")

        # Define a forced read schema for pyarrow that now matches the inferred types.
        # Note: For "value2", if the sample file inferred it as Int32, then we use pa.int32().
        read_schema = pa.schema([
            pa.field("id", pa.int32(), nullable=False),
            pa.field("group", pa.string(), nullable=False),
            pa.field("value1", pa.float32(), nullable=False),
            pa.field("value2", pa.int32(), nullable=False)  # changed from int64 to int32
        ])
        
        for file_path in parquet_files:
            try:
                pf = pq.ParquetFile(file_path)
                arrow_table = pf.read()
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
                print(f"Error processing file {file_path}: {e}")
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

        This method is used to inspect the historical snapshots of the
        Iceberg table. The snapshot history is a list of Snapshot objects
        that describe the state of the table at different points in time.

        If the `table` attribute is not set, it prints a message and does
        nothing. Call `run_pipeline()` first to set the `table` attribute.
        """
        if not hasattr(self, "table"):
            print("Table not loaded. Run run_pipeline() first.")
            return
        print("\nSnapshot History:")
        # Loop over the snapshots and print them.
        for snap in self.table.history():
            print(snap)

if __name__ == "__main__":
    pipeline = IcebergPipeline()
    pipeline.run_pipeline()
    pipeline.print_snapshot_history()

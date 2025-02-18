import os
import glob
import shutil
import pyarrow.parquet as pq
import pyarrow as pa
import daft  # type: ignore
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField, IntegerType, StringType, FloatType
from pyiceberg.partitioning import PartitionSpec, PartitionField, INITIAL_PARTITION_SPEC_ID, PARTITION_FIELD_ID_START
from pyiceberg.transforms import IdentityTransform
import ray

# Initialize Ray and set the Daft runner address.
daft.context.set_runner_ray(address="ray://localhost:10001")
# daft.context.set_runner_ray("ray://ray-head:10001") # container based

class IcebergPipeline:
    def __init__(self, parquet_input="large_dataset.parquet",
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
            Input Parquet file, by default "large_dataset.parquet"
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
            raise ValueError("No parquet files found.")
        # Read the schema from one of the sample files.
        sample_schema = pq.read_schema(sample_files[0])
        fields = []
        fid = 1
        # Loop over the fields in the sample schema and create a NestedField for each one.
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
        Run the pipeline to create an Iceberg table from the input Parquet file.

        The pipeline consists of the following steps:

        1. Clear the output directory to avoid duplicate processing.
        2. Use Daft to read the input Parquet file and write partitioned files.
        3. Load or create the catalog.
        4. Infer a PyIceberg schema for the table from one of the partitioned output files.
        5. Determine the field id for the partition field.
        6. Create the namespace (if it doesn't exist already).
        7. Create the Iceberg table with the inferred schema and partition spec.
        8. Read the partitioned files and register them with the Iceberg table.

        The Iceberg table is saved as an instance variable `table` and the catalog is saved as an instance variable `catalog`.
        """
        # Clear the output directory to avoid duplicate processing.
        if os.path.exists(self.output_dir):
            shutil.rmtree(self.output_dir)
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Use Daft to read the input Parquet file and write partitioned files.
        df = daft.read_parquet(self.parquet_input)
        df.write_parquet(self.output_dir, partition_cols=[self.partition_field_name], compression="zstd")
        print(f"Partitioned Parquet files written to: {self.output_dir}")
        
        # Load or create the catalog.
        catalog = self.get_catalog()
        # Create the warehouse directory if it doesn't exist.
        os.makedirs(self.catalog_config["warehouse"], exist_ok=True)
        # Infer a PyIceberg schema for the table from one of the partitioned output files.
        schema = self.infer_schema()
        
        # Determine the field id for the partition field.
        pfid = None
        # Loop over the fields in the inferred schema and find the one with the same name as the partition field.
        for field in schema.fields:
            if field.name == self.partition_field_name:
                pfid = field.field_id
                break
        # If the partition field is not found, raise an error.
        if pfid is None:
            raise ValueError(f"Partition field '{self.partition_field_name}' not found in inferred schema.")
        # Create the partition field and partition spec.
        partition_field = PartitionField(
            name=self.partition_field_name,
            source_id=pfid,
            field_id=PARTITION_FIELD_ID_START,
            transform=IdentityTransform()
        )
        # Create the partition spec.
        spec = PartitionSpec(spec_id=INITIAL_PARTITION_SPEC_ID, fields=(partition_field,))
        
        # Create the namespace (if it doesn't exist already).
        try:
            catalog.create_namespace(self.namespace)
            print(f"Namespace '{self.namespace}' created.")
        except Exception as e:
            print(f"Namespace '{self.namespace}' may already exist: {e}")
        # Create the Iceberg table with the inferred schema and partition spec.
        table_identifier = f"{self.namespace}.{self.table_name}"
        # Create the Iceberg table with the inferred schema and partition spec.
        table = catalog.create_table(
            table_identifier,
            schema=schema, 
            partition_spec=spec,
            properties={"write.target-file-size-bytes": "536870912"}  # target file size 512MB
        )
        print(f"Iceberg table {table_identifier} created with partition spec on '{self.partition_field_name}'.")
        # Read the partitioned files and register them with the Iceberg table.
        parquet_files = glob.glob(os.path.join(self.output_dir, "**", "*.parquet"), recursive=True)
        print(f"Found {len(parquet_files)} Parquet files to register.")
        
        # Define the schema for reading the Parquet files.
        read_schema = pa.schema([
            pa.field("id", pa.int32(), nullable=False),
            pa.field("group", pa.string(), nullable=False),
            pa.field("value1", pa.float32(), nullable=False),
            pa.field("value2", pa.int32(), nullable=False)
        ])
        # Loop over the Parquet files and register them with the Iceberg table.
        for file_path in parquet_files:
            try:
                # Read the Parquet file and cast it to the read schema.
                pf = pq.ParquetFile(file_path)
                # Read the Parquet file and cast it to the read schema.
                arrow_table = pf.read()
                # Cast the Parquet file to the read schema.
                gf = arrow_table.schema.field(self.partition_field_name)
                # If the partition field is a dictionary, decode it.
                if pa.types.is_dictionary(gf.type):
                    # Get the index of the partition field.
                    idx = arrow_table.schema.get_field_index(self.partition_field_name)
                    # Decode the partition field.
                    arrow_table = arrow_table.set_column(
                        idx, 
                        self.partition_field_name,
                        arrow_table[self.partition_field_name].dictionary_decode()
                    )
                # Cast the Parquet file to the read schema.
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

@ray.remote
def run_pipeline_remote():
    """
    Run the pipeline remotely using Ray.

    This function creates an instance of IcebergPipeline and calls its
    run_pipeline() method to execute the pipeline. After execution, it calls
    print_snapshot_history() to print the snapshot history of the Iceberg
    table. Finally, it returns a message indicating that the pipeline
    execution is complete.

    Returns
    -------
    str
        A message indicating that the pipeline execution is complete.
    """
    pipeline = IcebergPipeline()
    pipeline.run_pipeline()
    pipeline.print_snapshot_history()
    return "Pipeline execution complete."

if __name__ == "__main__":
    # Execute the pipeline remotely.
    result = ray.get(run_pipeline_remote.remote())
    print(result)


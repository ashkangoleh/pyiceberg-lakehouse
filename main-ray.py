import os
import glob
import pyarrow.parquet as pq
import pyarrow as pa
import ray
from functools import partial
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField, IntegerType, StringType, FloatType
from pyiceberg.partitioning import PartitionSpec, PartitionField, INITIAL_PARTITION_SPEC_ID, PARTITION_FIELD_ID_START
from pyiceberg.transforms import IdentityTransform

# Top-level function for filtering by partition value.
def filter_by_partition(row, partition_field, target_value):
    return row[partition_field] == target_value

@ray.remote
class IcebergPipelineRay:
    def __init__(self, parquet_input="data/large_dataset.parquet",
                 output_dir="./output_partitioned_parquet",
                 catalog_config=None,
                 namespace="default",
                 table_name="my_iceberg_table",
                 partition_field_name="group"):
        self.parquet_input = parquet_input
        self.output_dir = output_dir
        self.namespace = namespace
        self.table_name = table_name
        self.partition_field_name = partition_field_name
        self.catalog_config = catalog_config or {
            "catalog_type": "sqlite",
            "uri": "sqlite:///catalog.db",
            "warehouse": "iceberg_warehouse"
        }

    def get_catalog(self):
        config = self.catalog_config.copy()
        catalog_type = config.pop("catalog_type", "sqlite")
        return load_catalog(catalog_type, **config)

    def infer_schema(self):
        sample_files = glob.glob(os.path.join(self.output_dir, "**", "*.parquet"), recursive=True)
        if not sample_files:
            raise ValueError("No parquet files found.")
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
        # Read the input Parquet file using Ray Dataset.
        dataset = ray.data.read_parquet(self.parquet_input, override_num_blocks=4)
        os.makedirs(self.output_dir, exist_ok=True)

        # Partition the dataset by unique values.
        unique_vals = dataset.unique(self.partition_field_name)
        print(f"Unique partition values found: {unique_vals}")
        for val in unique_vals:
            # Use a top-level function (with partial) instead of an inline lambda.
            filtered_ds = dataset.filter(
                partial(filter_by_partition, partition_field=self.partition_field_name, target_value=val)
            )
            partition_dir = os.path.join(self.output_dir, f"{self.partition_field_name}={val}")
            os.makedirs(partition_dir, exist_ok=True)
            filtered_ds.write_parquet(partition_dir)
            print(f"Partition '{val}' written to {partition_dir}")
        print(f"Partitioned Parquet files written to: {self.output_dir}")

        # Load or create the catalog.
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

        # Register all Parquet files to the Iceberg table using the new add_files API.
        parquet_files = glob.glob(os.path.join(self.output_dir, "**", "*.parquet"), recursive=True)
        print(f"Found {len(parquet_files)} Parquet files to register.")

        batch_size = 100  # Adjust the batch size as needed.
        for i in range(0, len(parquet_files), batch_size):
            batch_files = parquet_files[i:i+batch_size]
            print(f"Adding files batch from index {i} to {i + len(batch_files) - 1}")
            try:
                table.add_files(batch_files)
                print("Batch added successfully.")
            except Exception as e:
                print(f"Error adding batch: {e}")

        print("\n--- Iceberg Table Metadata ---")
        print("Schema:")
        print(table.schema())
        self.table = table
        self.catalog = catalog

    def print_snapshot_history(self):
        if not hasattr(self, "table"):
            print("Table not loaded. Run run_pipeline() first.")
            return
        print("\nSnapshot History:")
        for snap in self.table.history():
            print(snap)

@ray.remote
def run_pipeline_remote():
    pipeline = IcebergPipelineRay.remote()
    ray.get(pipeline.run_pipeline.remote())
    ray.get(pipeline.print_snapshot_history.remote())
    return "Pipeline execution complete."

if __name__ == "__main__":
    ray.init(address="ray://ray-head:10001")
    result = ray.get(run_pipeline_remote.remote(), timeout=1200)
    print(result)

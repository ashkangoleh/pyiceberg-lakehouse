import os
import glob
import pyarrow.parquet as pq
import pyarrow as pa
import polars as pl  # using Polars instead of Daft
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField, IntegerType, StringType, FloatType
from pyiceberg.partitioning import PartitionSpec, PartitionField, INITIAL_PARTITION_SPEC_ID, PARTITION_FIELD_ID_START
from pyiceberg.transforms import IdentityTransform
import ray


class IcebergPipeline:
    def __init__(self, parquet_input="new_large_dataset.parquet",
                 output_dir="./output_partitioned_parquet",
                 catalog_config=None,
                 namespace="default", 
                 table_name="my_iceberg_table",
                 partition_field_name="group"
                ):
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
        # Read the input Parquet file using Polars
        df = pl.read_parquet(self.parquet_input)
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Partition the data by the specified column (e.g. "group")
        unique_vals = df.select(self.partition_field_name).unique().to_series().to_list()
        for val in unique_vals:
            partition_df = df.filter(pl.col(self.partition_field_name) == val)
            partition_dir = os.path.join(self.output_dir, f"{self.partition_field_name}={val}")
            os.makedirs(partition_dir, exist_ok=True)
            output_file = os.path.join(partition_dir, "data.parquet")
            partition_df.write_parquet(output_file)

        print(f"Partitioned Parquet files written to: {self.output_dir}")

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

        parquet_files = glob.glob(os.path.join(self.output_dir, "**", "*.parquet"), recursive=True)
        print(f"Found {len(parquet_files)} Parquet files to register.")

        # Define the forced schema for reading files
        read_schema = pa.schema([
            pa.field("id", pa.int32(), nullable=False),
            pa.field("group", pa.string(), nullable=False),
            pa.field("value1", pa.float32(), nullable=False),
            pa.field("value2", pa.int32(), nullable=False)
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
        if not hasattr(self, "table"):
            print("Table not loaded. Run run_pipeline() first.")
            return
        print("\nSnapshot History:")
        for snap in self.table.history():
            print(snap)
@ray.remote
def run_pipeline_remote():
    pipeline = IcebergPipeline()
    pipeline.run_pipeline()
    pipeline.print_snapshot_history()
    return "Pipeline execution complete."

if __name__ == "__main__":
    ray.init(address="ray://localhost:10001",runtime_env={
        "env_vars": {
            # RAY_NUM_SERVER_CALL_THREAD limits the number of threads that Ray uses
            # internally for handling server calls (e.g. gRPC calls between workers and the driver).
            # Setting this to "1" helps reduce thread contention and excessive resource usage,
            # which can be especially important when running on clusters with many cores.
            "RAY_NUM_SERVER_CALL_THREAD": "1",
        },
        "setup_commands": [
            # OPENBLAS_NUM_THREADS: Many numerical libraries (like OpenBLAS) use multi-threading for
            # linear algebra operations. Setting this to 1 forces OpenBLAS to use only a single thread,
            # which helps prevent oversubscription of CPU cores and reduces virtual memory overhead.
            "export OPENBLAS_NUM_THREADS=1",
            
            # OMP_NUM_THREADS: OpenMP controls the number of threads for parallel regions in many C/C++ libraries.
            # By limiting it to 1, you ensure that parallel loops within these libraries do not spawn extra threads,
            # which can otherwise lead to performance degradation or excessive resource usage.
            "export OMP_NUM_THREADS=1",
            
            # MKL_NUM_THREADS: Intel's Math Kernel Library (MKL) also supports multi-threading for
            # optimized numerical computations. Restricting it to a single thread avoids potential
            # conflicts with Ray's parallelism and helps control overall thread count.
            "export MKL_NUM_THREADS=1",
            
            # TF_NUM_INTEROP_THREADS: TensorFlow uses inter-op parallelism to distribute independent operations
            # across multiple threads. Limiting this to 1 reduces the risk of creating too many threads,
            # which could lead to inefficient CPU usage or resource exhaustion in a distributed environment.
            "export TF_NUM_INTEROP_THREADS=1",
            
            # TF_NUM_INTRAOP_THREADS: TensorFlow also uses intra-op parallelism for splitting individual operations
            # across multiple threads. Setting this to 1 ensures that each operation is executed in a single thread,
            # which is useful when you want to keep thread counts predictable and avoid interference with Ray's scheduling.
            "export TF_NUM_INTRAOP_THREADS=1"
        ]
    }, object_store_memory=10 * 1024 * 1024 * 1024
    )
    # Execute the pipeline remotely.
    result = ray.get(run_pipeline_remote.remote(), timeout=600)
    print(result)

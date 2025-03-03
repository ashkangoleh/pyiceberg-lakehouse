"""
Fast Single-Append Pipeline using PyArrow directly.
This approach uses PyArrow's memory mapping and multi-threading capabilities
to read the entire Parquet file into an Arrow table quickly, casts to a forced schema,
and appends in a single operation (producing one snapshot).
"""

import os
import time
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField, IntegerType, StringType, FloatType
from pyiceberg.partitioning import PartitionSpec, PartitionField, INITIAL_PARTITION_SPEC_ID, PARTITION_FIELD_ID_START
from pyiceberg.transforms import IdentityTransform

class FastSingleAppendPipeline:
    def __init__(self, parquet_input="new_large_dataset.parquet",
                 catalog_config=None,
                 namespace="default",
                 table_name="my_iceberg_table",
                 partition_field_name="group"):
        """
        Initialize the pipeline.

        Parameters
        ----------
        parquet_input : str
            Path to the input Parquet file.
        catalog_config : dict, optional
            Configuration for the Iceberg catalog.
        namespace : str, optional
            Namespace for the Iceberg catalog.
        table_name : str, optional
            Name of the Iceberg table.
        partition_field_name : str, optional
            Name of the field to partition on.
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
        config = self.catalog_config.copy()
        catalog_type = config.pop("catalog_type", "sqlite")
        return load_catalog(catalog_type, **config)

    def infer_schema_from_table(self, arrow_table: pa.Table) -> Schema:
        """
        Infer the Iceberg schema from an Arrow table.
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

    def run_fast_single_append(self):
        # 1) Verify file existence.
        if not os.path.exists(self.parquet_input):
            raise FileNotFoundError(f"Input Parquet file not found: {self.parquet_input}")

        # 2) Read entire Parquet file using PyArrow with memory mapping and threads.
        print("Reading entire dataset using PyArrow with memory mapping...")
        arrow_table = pq.read_table(self.parquet_input, memory_map=True, use_threads=True)
        print(f"Read {arrow_table.num_rows} rows and {arrow_table.num_columns} columns.")

        # 3) Create or load the Iceberg catalog.
        catalog = self.get_catalog()
        os.makedirs(self.catalog_config["warehouse"], exist_ok=True)

        # 4) Infer schema from the Arrow table.
        schema = self.infer_schema_from_table(arrow_table)
        partition_col = self.partition_field_name
        pfid = None
        for field in schema.fields:
            if field.name == partition_col:
                pfid = field.field_id
                break
        if pfid is None:
            raise ValueError(f"Partition field '{partition_col}' not found in schema.")

        # 5) Create partition field and specification.
        partition_field = PartitionField(
            name=partition_col,
            source_id=pfid,
            field_id=PARTITION_FIELD_ID_START,
            transform=IdentityTransform()
        )
        spec = PartitionSpec(spec_id=INITIAL_PARTITION_SPEC_ID, fields=(partition_field,))

        # 6) Create namespace and table.
        try:
            catalog.create_namespace(self.namespace)
            print(f"Namespace '{self.namespace}' created.")
        except Exception as e:
            print(f"Namespace '{self.namespace}' may already exist: {e}")
        table_identifier = f"{self.namespace}.{self.table_name}"
        table = catalog.create_table(table_identifier, schema=schema, partition_spec=spec)
        print(f"Iceberg table {table_identifier} created with partition spec on '{partition_col}'.")

        # 7) Define a forced schema matching the Iceberg table.
        forced_schema = pa.schema([
            pa.field("id", pa.int32(), nullable=False),
            pa.field("group", pa.string(), nullable=False),
            pa.field("value1", pa.float32(), nullable=False),
            pa.field("value2", pa.int32(), nullable=False)
        ])

        # 8) Cast the data to the forced schema.
        arrow_table = arrow_table.cast(forced_schema)

        # 9) Append the entire dataset in one go.
        start_append = time.time()
        table.append(arrow_table)
        end_append = time.time()
        print(f"Appended entire dataset in a single operation in {end_append - start_append:.2f} seconds (one snapshot).")

        self.table = table
        self.catalog = catalog

    def print_snapshot_history(self):
        if not hasattr(self, "table"):
            print("Table not loaded. Run run_fast_single_append() first.")
            return
        print("\nSnapshot History:")
        for snap in self.table.history():
            print(snap)

if __name__ == "__main__":
    start_time = time.time()
    pipeline = FastSingleAppendPipeline(
        parquet_input="data/large_dataset.parquet",
        partition_field_name="group"
    )
    pipeline.run_fast_single_append()
    pipeline.print_snapshot_history()
    elapsed_time = time.time() - start_time
    print(f"\nTotal load time: {elapsed_time:.2f} seconds")

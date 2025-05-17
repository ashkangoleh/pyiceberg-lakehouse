"""
Multi-processing pipeline that reads a large Parquet file in parallel by row groups,
processes each chunk concurrently, concatenates the results, and appends the entire data
to an Iceberg table in one snapshot.
"""

import os
import time
import pyarrow as pa
import pyarrow.parquet as pq
from concurrent.futures import ProcessPoolExecutor
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField, IntegerType, StringType, FloatType
from pyiceberg.partitioning import PartitionSpec, PartitionField, INITIAL_PARTITION_SPEC_ID, PARTITION_FIELD_ID_START
from pyiceberg.transforms import IdentityTransform

def process_row_group(parquet_input, row_group_index):
    pf = pq.ParquetFile(parquet_input)
    table = pf.read_row_group(row_group_index)
    return table

class MultiProcessPipeline:
    def __init__(self, parquet_input="new_large_dataset.parquet",
                 catalog_config=None,
                 namespace="default",
                 table_name="my_iceberg_table",
                 partition_field_name="group"):
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

    def run_multiprocess(self):
        if not os.path.exists(self.parquet_input):
            raise FileNotFoundError(f"Input file not found: {self.parquet_input}")

        print("Reading Parquet file with multi-processing...")
        pf = pq.ParquetFile(self.parquet_input)
        num_row_groups = pf.num_row_groups
        with ProcessPoolExecutor() as executor:
            futures = [executor.submit(process_row_group, self.parquet_input, i) for i in range(num_row_groups)]
            tables = [f.result() for f in futures]
        full_table = pa.concat_tables(tables)
        print(f"Concatenated table with {full_table.num_rows} rows.")

        # Create or load the Iceberg catalog
        catalog = self.get_catalog()
        os.makedirs(self.catalog_config["warehouse"], exist_ok=True)
        schema = self.infer_schema_from_table(full_table)
        partition_col = self.partition_field_name
        pfid = None
        for field in schema.fields:
            if field.name == partition_col:
                pfid = field.field_id
                break
        if pfid is None:
            raise ValueError(f"Partition field '{partition_col}' not found in schema.")
        partition_field = PartitionField(
            name=partition_col,
            source_id=pfid,
            field_id=PARTITION_FIELD_ID_START,
            transform=IdentityTransform()
        )
        spec = PartitionSpec(spec_id=INITIAL_PARTITION_SPEC_ID, fields=(partition_field,))
        try:
            catalog.create_namespace(self.namespace)
            print(f"Namespace '{self.namespace}' created.")
        except Exception as e:
            print(f"Namespace may already exist: {e}")
        table_identifier = f"{self.namespace}.{self.table_name}"
        table = catalog.create_table(table_identifier, schema=schema, partition_spec=spec)
        print(f"Iceberg table {table_identifier} created with partition spec on '{partition_col}'.")

        # Define forced schema to match the Iceberg table
        forced_schema = pa.schema([
            pa.field("id", pa.int32(), nullable=False),
            pa.field("group", pa.string(), nullable=False),
            pa.field("value1", pa.float32(), nullable=False),
            pa.field("value2", pa.int32(), nullable=False)
        ])
        full_table = full_table.cast(forced_schema)
        start_append = time.time()
        table.append(full_table)
        end_append = time.time()
        print(f"Appended dataset in a single operation in {end_append - start_append:.2f} seconds.")
        self.table = table

    def print_snapshot_history(self):
        if not hasattr(self, "table"):
            print("Table not loaded. Run run_multiprocess() first.")
            return
        print("Snapshot History:")
        for snap in self.table.history():
            print(snap)

if __name__ == "__main__":
    start_time = time.time()
    pipeline = MultiProcessPipeline(parquet_input="new_large_dataset.parquet", partition_field_name="group")
    pipeline.run_multiprocess()
    pipeline.print_snapshot_history()
    end_time = time.time()
    print(f"Total run time: {end_time - start_time:.2f} seconds")

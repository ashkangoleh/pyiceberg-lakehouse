from pyiceberg.catalog import load_catalog

config = {"uri": "sqlite:///catalog.db", "warehouse": "iceberg_warehouse"}
catalog = load_catalog("sqlite", **config)
table_identifier = "default.my_iceberg_table"
table = catalog.load_table(table_identifier)
print("Table Metadata Location:")
print(table.metadata_location)
print("\nSnapshot History:")
for snapshot in table.history():
    print(snapshot)
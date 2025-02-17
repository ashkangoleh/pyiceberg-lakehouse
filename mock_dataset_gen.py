import polars as pl
import numpy as np

# Parameters for large dataset
num_rows = 10_000_000  # 100 million rows
start_id =1

# Generate a large dataset with random data
data = {
    "id": np.arange(start_id, start_id + num_rows),
    "group": np.random.choice(["A", "B", "C", "D"], size=num_rows),
    "value1": np.random.rand(num_rows) * 100,
    "value2": np.random.randint(1, 1000, size=num_rows),
}

# Create a Polars DataFrame
df = pl.DataFrame(data)

# Save to parquet
file_path = "new_large_dataset.parquet"
df.write_parquet(file_path)
print(f"Parquet file '{file_path}' generated with {num_rows} rows.")

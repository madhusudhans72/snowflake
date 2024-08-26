import pandas as pd

# Create a DataFrame with structured data
data = {
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [25, 30, 35],
}

df = pd.DataFrame(data)

# Save the DataFrame to a Parquet file
df.to_parquet('cities2.parquet', engine='pyarrow')
print("Parquet file created: structured_data.parquet")

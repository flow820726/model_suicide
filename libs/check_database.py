import pandas as pd

# Load the uploaded file
file_path = 'year_dist_info_new.csv'
data = pd.read_csv(file_path)

# Display the first few rows to understand its structure
data.head(), data.columns

# Pivot the table
pivot_table = data.pivot(index='table', columns='year', values='ratio')

# Save the result and display a snippet
output_path = 'database_new.csv'
pivot_table.to_csv(output_path)

pivot_table.head(), output_path
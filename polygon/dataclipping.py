import pandas as pd

# Original file path
input_path = r"C:\Users\spenc\Downloads\Dev Files\ModularM1\polygon\csvoutputs\AMD_raw_trades2_20250203.csv"

# Output sample path
output_path = input_path.replace(".csv", "_small.csv")

try:
    # Read first 155000 rows (keeps header)
    df_sample = pd.read_csv(input_path, nrows=155000)
    
    # Save sample file
    df_sample.to_csv(output_path, index=False)
    print(f"Sample file created: {output_path}")
    print(f"Sample contains {len(df_sample)} rows")

except FileNotFoundError:
    print(f"Error: Input file not found at {input_path}")
except Exception as e:
    print(f"Error: {str(e)}")

# Verify creation
import os
if os.path.exists(output_path):
    print("\nVerification:")
    print(f"File size: {os.path.getsize(output_path) / 1024:.1f} KB")
    print(f"Created: {os.path.getctime(output_path)}")


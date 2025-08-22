import pandas as pd

# Read the Excel file
excel_file = "trading_cards_metrics.xlsx"
df = pd.read_excel(excel_file, sheet_name='Trading_Card_Metrics')

print("Excel File Preview:")
print("=" * 50)
print(f"Shape: {df.shape}")
print(f"Variables: {len(df)} rows")
print(f"Time periods: {df.shape[1] - 1} columns")
print()

print("Variables included:")
for i, var in enumerate(df['Variable'], 1):
    print(f"{i:2d}. {var}")
print()

print("First 5 columns of data:")
print(df.iloc[:, :6].to_string(index=False))
print()

print("Last 5 columns of data:")
print(df.iloc[:, -5:].to_string(index=False))
print()

print("Date range:")
date_columns = [col for col in df.columns if col != 'Variable']
print(f"From: {date_columns[0]}")
print(f"To: {date_columns[-1]}")
print(f"Total weeks: {len(date_columns)}")
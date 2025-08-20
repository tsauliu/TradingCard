import pandas as pd

# Read the Excel file
df = pd.read_excel('combined_trading_card_data.xlsx')

print("=== Excel File Preview ===")
print(f"Total rows: {len(df)}")
print(f"Columns: {list(df.columns)}")
print()

print("=== First 10 rows ===")
print(df.head(10))
print()

print("=== Data Summary ===")
print(f"Unique cards: {df['Filename'].nunique()}")
print(f"Cards: {df['Filename'].unique()}")
print(f"Metric types: {df['Metric_Type'].unique()}")
print(f"Date range: {df['Date'].min()} to {df['Date'].max()}")
print()

print("=== Sample by Card ===")
for card in df['Filename'].unique():
    card_data = df[df['Filename'] == card]
    print(f"\n{card}:")
    print(f"  Rows: {len(card_data)}")
    print(f"  Date range: {card_data['Date'].min()} to {card_data['Date'].max()}")
    print(f"  Metrics: {card_data['Metric_Type'].unique()}")
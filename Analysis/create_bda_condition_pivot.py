#!/usr/bin/env python3
"""
Create pivot tables from weekly tcg_prices_bda data grouped by CONDITION
Structure: 
- Market Price sheet - conditions as rows, dates as columns
- Quantity Sold sheet - conditions as rows, dates as columns  
- Product Count sheet - conditions as rows, dates as columns
"""

import pandas as pd
import numpy as np
import xlsxwriter
import os

def create_bda_condition_pivot():
    # Read the data
    input_file = '/home/caoliu/TradingCard/analysis/weekly_bda_by_condition.csv'
    
    print(f'Reading BDA condition data from {input_file}...')
    df = pd.read_csv(input_file)
    df['week_start_date'] = pd.to_datetime(df['week_start_date'])
    
    print(f'BDA condition data loaded: {len(df)} rows, {df["condition"].nunique()} conditions')
    
    # Calculate total quantity sold per condition for sorting
    condition_totals = df.groupby('condition')['total_quantity_sold'].sum().sort_values(ascending=False)
    sorted_conditions = condition_totals.index.tolist()
    
    print(f'Conditions by total quantity sold:')
    for condition in sorted_conditions:
        total_qty = condition_totals[condition]
        print(f'  {condition}: {total_qty:,} total quantity sold')
    
    # Create pivot tables
    price_pivot = df.pivot_table(
        index='condition',
        columns='week_start_date', 
        values='avg_market_price',
        aggfunc='first'
    )
    
    quantity_pivot = df.pivot_table(
        index='condition',
        columns='week_start_date',
        values='total_quantity_sold', 
        aggfunc='first'
    )
    
    product_pivot = df.pivot_table(
        index='condition',
        columns='week_start_date',
        values='unique_product_count', 
        aggfunc='first'
    )
    
    # Reorder by sorted conditions
    price_pivot = price_pivot.reindex(sorted_conditions)
    quantity_pivot = quantity_pivot.reindex(sorted_conditions)
    product_pivot = product_pivot.reindex(sorted_conditions)
    
    # Create output Excel file
    output_file = '/home/caoliu/TradingCard/analysis/tcg_bda_condition_pivot.xlsx'
    
    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        # Define formats
        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#D7E4BD',
            'border': 1,
            'align': 'center'
        })
        
        currency_format = workbook.add_format({
            'num_format': '$#,##0.00',
            'border': 1
        })
        
        number_format = workbook.add_format({
            'num_format': '#,##0',
            'border': 1
        })
        
        condition_format = workbook.add_format({
            'bold': True,
            'bg_color': '#F2F2F2',
            'border': 1,
            'align': 'left'
        })
        
        # Get date columns
        date_columns = sorted(price_pivot.columns)
        date_strings = [d.strftime('%Y-%m-%d') for d in date_columns]
        
        # Sheet 1: Market Price by Condition
        worksheet_price = workbook.add_worksheet('Market_Price')
        
        # Headers for Price sheet
        worksheet_price.write(0, 0, 'Condition', header_format)
        for col, date_str in enumerate(date_strings):
            worksheet_price.write(0, col + 1, date_str, header_format)
        
        # Price data
        for row, condition in enumerate(sorted_conditions):
            worksheet_price.write(row + 1, 0, condition, condition_format)
            
            for col, date in enumerate(date_columns):
                value = price_pivot.loc[condition, date] if date in price_pivot.columns else np.nan
                if pd.isna(value):
                    worksheet_price.write(row + 1, col + 1, '', currency_format)
                else:
                    worksheet_price.write(row + 1, col + 1, value, currency_format)
        
        # Set column widths for Price
        worksheet_price.set_column(0, 0, 18)  # Condition column
        worksheet_price.set_column(1, len(date_strings), 10)  # Date columns
        worksheet_price.freeze_panes(1, 1)
        
        # Sheet 2: Quantity Sold by Condition
        worksheet_quantity = workbook.add_worksheet('Quantity_Sold')
        
        # Headers for Quantity sheet
        worksheet_quantity.write(0, 0, 'Condition', header_format)
        for col, date_str in enumerate(date_strings):
            worksheet_quantity.write(0, col + 1, date_str, header_format)
        
        # Quantity data
        for row, condition in enumerate(sorted_conditions):
            worksheet_quantity.write(row + 1, 0, condition, condition_format)
            
            for col, date in enumerate(date_columns):
                value = quantity_pivot.loc[condition, date] if date in quantity_pivot.columns else np.nan
                if pd.isna(value):
                    worksheet_quantity.write(row + 1, col + 1, '', number_format)
                else:
                    worksheet_quantity.write(row + 1, col + 1, value, number_format)
        
        # Set column widths for Quantity
        worksheet_quantity.set_column(0, 0, 18)  # Condition column
        worksheet_quantity.set_column(1, len(date_strings), 12)  # Date columns
        worksheet_quantity.freeze_panes(1, 1)
        
        # Sheet 3: Product Count by Condition
        worksheet_products = workbook.add_worksheet('Product_Count')
        
        # Headers for Products sheet
        worksheet_products.write(0, 0, 'Condition', header_format)
        for col, date_str in enumerate(date_strings):
            worksheet_products.write(0, col + 1, date_str, header_format)
        
        # Products data
        for row, condition in enumerate(sorted_conditions):
            worksheet_products.write(row + 1, 0, condition, condition_format)
            
            for col, date in enumerate(date_columns):
                value = product_pivot.loc[condition, date] if date in product_pivot.columns else np.nan
                if pd.isna(value):
                    worksheet_products.write(row + 1, col + 1, '', number_format)
                else:
                    worksheet_products.write(row + 1, col + 1, value, number_format)
        
        # Set column widths for Products
        worksheet_products.set_column(0, 0, 18)  # Condition column
        worksheet_products.set_column(1, len(date_strings), 10)  # Date columns
        worksheet_products.freeze_panes(1, 1)
        
        # Sheet 4: Summary
        worksheet_summary = workbook.add_worksheet('Summary')
        
        summary_data = []
        for condition in sorted_conditions:
            total_quantity = condition_totals[condition]
            avg_price = price_pivot.loc[condition].mean()
            avg_products = product_pivot.loc[condition].mean()
            
            summary_data.append({
                'Condition': condition,
                'Total_Quantity_Sold': total_quantity,
                'Avg_Market_Price': avg_price,
                'Avg_Product_Count': avg_products
            })
        
        # Write summary headers
        headers = ['Condition', 'Total Quantity Sold', 'Avg Market Price', 'Avg Product Count']
        for col, header in enumerate(headers):
            worksheet_summary.write(0, col, header, header_format)
        
        # Write summary data
        for row, data in enumerate(summary_data):
            worksheet_summary.write(row + 1, 0, data['Condition'], condition_format)
            worksheet_summary.write(row + 1, 1, data['Total_Quantity_Sold'], number_format)
            worksheet_summary.write(row + 1, 2, data['Avg_Market_Price'], currency_format)
            worksheet_summary.write(row + 1, 3, data['Avg_Product_Count'], number_format)
        
        worksheet_summary.set_column(0, 0, 18)  # Condition
        worksheet_summary.set_column(1, 1, 18)  # Total Quantity
        worksheet_summary.set_column(2, 2, 15)  # Average Price
        worksheet_summary.set_column(3, 3, 15)  # Average Products
        worksheet_summary.freeze_panes(1, 0)
    
    print(f'\\nBDA condition pivot tables created successfully!')
    print(f'Output file: {output_file}')
    print(f'Structure:')
    print(f'- Market_Price Sheet: {len(sorted_conditions)} conditions x {len(date_columns)} weeks')
    print(f'- Quantity_Sold Sheet: {len(sorted_conditions)} conditions x {len(date_columns)} weeks') 
    print(f'- Product_Count Sheet: {len(sorted_conditions)} conditions x {len(date_columns)} weeks')
    print(f'- Summary Sheet: Condition overview with totals')
    
    return output_file

if __name__ == "__main__":
    create_bda_condition_pivot()
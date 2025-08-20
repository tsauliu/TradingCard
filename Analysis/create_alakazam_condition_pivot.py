#!/usr/bin/env python3
"""
Create pivot tables for Product 42346 (Alakazam) grouped by CONDITION
Same format as the general condition pivot
Structure: 
- Market Price sheet - conditions as rows, dates as columns
- Quantity Sold sheet - conditions as rows, dates as columns  
- SKU Count sheet - conditions as rows, dates as columns
"""

import pandas as pd
import numpy as np
import xlsxwriter
import os

def create_alakazam_condition_pivot():
    # Read the data
    input_file = '/home/caoliu/TradingCard/analysis/product_42346_weekly_by_condition.csv'
    
    print(f'Reading Alakazam condition data from {input_file}...')
    df = pd.read_csv(input_file)
    df['week_start_date'] = pd.to_datetime(df['week_start_date'])
    
    print(f'Alakazam condition data loaded: {len(df)} rows, {df["condition"].nunique()} conditions')
    
    # Calculate total quantity sold per condition for sorting
    condition_totals = df.groupby('condition')['total_quantity_sold'].sum().sort_values(ascending=False)
    sorted_conditions = condition_totals.index.tolist()
    
    print(f'Alakazam conditions by total quantity sold:')
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
    
    sku_pivot = df.pivot_table(
        index='condition',
        columns='week_start_date',
        values='unique_sku_count', 
        aggfunc='first'
    )
    
    # Reorder by sorted conditions
    price_pivot = price_pivot.reindex(sorted_conditions)
    quantity_pivot = quantity_pivot.reindex(sorted_conditions)
    sku_pivot = sku_pivot.reindex(sorted_conditions)
    
    # Create output Excel file
    output_file = '/home/caoliu/TradingCard/analysis/alakazam_condition_pivot.xlsx'
    
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
        
        title_format = workbook.add_format({
            'bold': True,
            'font_size': 14,
            'bg_color': '#4472C4',
            'font_color': 'white',
            'align': 'center'
        })
        
        # Get date columns
        date_columns = sorted(price_pivot.columns)
        date_strings = [d.strftime('%Y-%m-%d') for d in date_columns]
        
        # Sheet 1: Market Price by Condition
        worksheet_price = workbook.add_worksheet('Market_Price')
        
        # Title
        worksheet_price.merge_range(0, 0, 0, len(date_strings), 
                                  'Alakazam (Product 42346) - Market Price by Condition', title_format)
        
        # Headers for Price sheet
        worksheet_price.write(1, 0, 'Condition', header_format)
        for col, date_str in enumerate(date_strings):
            worksheet_price.write(1, col + 1, date_str, header_format)
        
        # Price data
        for row, condition in enumerate(sorted_conditions):
            worksheet_price.write(row + 2, 0, condition, condition_format)
            
            for col, date in enumerate(date_columns):
                value = price_pivot.loc[condition, date] if date in price_pivot.columns else np.nan
                if pd.isna(value):
                    worksheet_price.write(row + 2, col + 1, '', currency_format)
                else:
                    worksheet_price.write(row + 2, col + 1, value, currency_format)
        
        # Set column widths for Price
        worksheet_price.set_column(0, 0, 18)  # Condition column
        worksheet_price.set_column(1, len(date_strings), 10)  # Date columns
        worksheet_price.freeze_panes(2, 1)
        
        # Sheet 2: Quantity Sold by Condition
        worksheet_quantity = workbook.add_worksheet('Quantity_Sold')
        
        # Title
        worksheet_quantity.merge_range(0, 0, 0, len(date_strings), 
                                     'Alakazam (Product 42346) - Quantity Sold by Condition', title_format)
        
        # Headers for Quantity sheet
        worksheet_quantity.write(1, 0, 'Condition', header_format)
        for col, date_str in enumerate(date_strings):
            worksheet_quantity.write(1, col + 1, date_str, header_format)
        
        # Quantity data
        for row, condition in enumerate(sorted_conditions):
            worksheet_quantity.write(row + 2, 0, condition, condition_format)
            
            for col, date in enumerate(date_columns):
                value = quantity_pivot.loc[condition, date] if date in quantity_pivot.columns else np.nan
                if pd.isna(value):
                    worksheet_quantity.write(row + 2, col + 1, '', number_format)
                else:
                    worksheet_quantity.write(row + 2, col + 1, value, number_format)
        
        # Set column widths for Quantity
        worksheet_quantity.set_column(0, 0, 18)  # Condition column
        worksheet_quantity.set_column(1, len(date_strings), 10)  # Date columns
        worksheet_quantity.freeze_panes(2, 1)
        
        # Sheet 3: SKU Count by Condition
        worksheet_sku = workbook.add_worksheet('SKU_Count')
        
        # Title
        worksheet_sku.merge_range(0, 0, 0, len(date_strings), 
                                'Alakazam (Product 42346) - SKU Count by Condition', title_format)
        
        # Headers for SKU sheet
        worksheet_sku.write(1, 0, 'Condition', header_format)
        for col, date_str in enumerate(date_strings):
            worksheet_sku.write(1, col + 1, date_str, header_format)
        
        # SKU data
        for row, condition in enumerate(sorted_conditions):
            worksheet_sku.write(row + 2, 0, condition, condition_format)
            
            for col, date in enumerate(date_columns):
                value = sku_pivot.loc[condition, date] if date in sku_pivot.columns else np.nan
                if pd.isna(value):
                    worksheet_sku.write(row + 2, col + 1, '', number_format)
                else:
                    worksheet_sku.write(row + 2, col + 1, value, number_format)
        
        # Set column widths for SKU
        worksheet_sku.set_column(0, 0, 18)  # Condition column
        worksheet_sku.set_column(1, len(date_strings), 10)  # Date columns
        worksheet_sku.freeze_panes(2, 1)
        
        # Sheet 4: Summary
        worksheet_summary = workbook.add_worksheet('Summary')
        
        # Title
        worksheet_summary.merge_range(0, 0, 0, 3, 
                                    'Alakazam (Product 42346) - Summary by Condition', title_format)
        
        summary_data = []
        for condition in sorted_conditions:
            total_quantity = condition_totals[condition]
            avg_price = price_pivot.loc[condition].mean()
            avg_sku = sku_pivot.loc[condition].mean()
            
            summary_data.append({
                'Condition': condition,
                'Total_Quantity_Sold': total_quantity,
                'Avg_Market_Price': avg_price,
                'Avg_SKU_Count': avg_sku
            })
        
        # Write summary headers
        headers = ['Condition', 'Total Quantity Sold', 'Avg Market Price', 'Avg SKU Count']
        for col, header in enumerate(headers):
            worksheet_summary.write(1, col, header, header_format)
        
        # Write summary data
        for row, data in enumerate(summary_data):
            worksheet_summary.write(row + 2, 0, data['Condition'], condition_format)
            worksheet_summary.write(row + 2, 1, data['Total_Quantity_Sold'], number_format)
            worksheet_summary.write(row + 2, 2, data['Avg_Market_Price'], currency_format)
            worksheet_summary.write(row + 2, 3, data['Avg_SKU_Count'], number_format)
        
        worksheet_summary.set_column(0, 0, 18)  # Condition
        worksheet_summary.set_column(1, 1, 18)  # Total Quantity
        worksheet_summary.set_column(2, 2, 15)  # Average Price
        worksheet_summary.set_column(3, 3, 15)  # Average SKU
        worksheet_summary.freeze_panes(2, 0)
    
    print(f'\\nAlakazam condition pivot tables created successfully!')
    print(f'Output file: {output_file}')
    print(f'Structure:')
    print(f'- Market_Price Sheet: {len(sorted_conditions)} conditions x {len(date_columns)} weeks')
    print(f'- Quantity_Sold Sheet: {len(sorted_conditions)} conditions x {len(date_columns)} weeks') 
    print(f'- SKU_Count Sheet: {len(sorted_conditions)} conditions x {len(date_columns)} weeks')
    print(f'- Summary Sheet: Condition overview with totals')
    
    return output_file

if __name__ == "__main__":
    create_alakazam_condition_pivot()
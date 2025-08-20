#!/usr/bin/env python3
"""
Script to create single pivot table from weekly TCG data
Single sheet with all categories
Structure: 
1. Price section - categories as rows, dates as columns
2. Count section - categories as rows, dates as columns
"""

import pandas as pd
import numpy as np
import xlsxwriter
import os

def create_single_pivot():
    # Read the data
    input_file = '/home/caoliu/TradingCard/analysis/weekly_mid_prices_with_product_count.csv'
    print(f'Reading data from {input_file}...')
    
    df = pd.read_csv(input_file)
    df['week_start_date'] = pd.to_datetime(df['week_start_date'])
    
    print(f'Data loaded: {len(df)} rows, {df["category_id"].nunique()} categories')
    
    # Create pivot tables
    price_pivot = df.pivot_table(
        index='category_id',
        columns='week_start_date', 
        values='avg_mid_price',
        aggfunc='first'
    )
    
    count_pivot = df.pivot_table(
        index='category_id',
        columns='week_start_date',
        values='product_count', 
        aggfunc='first'
    )
    
    # Create output Excel file
    output_file = '/home/caoliu/TradingCard/analysis/tcg_single_pivot.xlsx'
    
    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        # Define formats
        header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#D7E4BD',
            'border': 1,
            'align': 'center'
        })
        
        section_header_format = workbook.add_format({
            'bold': True,
            'bg_color': '#4472C4',
            'font_color': 'white',
            'border': 1,
            'align': 'center',
            'font_size': 14
        })
        
        currency_format = workbook.add_format({
            'num_format': '$#,##0.00',
            'border': 1
        })
        
        number_format = workbook.add_format({
            'num_format': '#,##0',
            'border': 1
        })
        
        category_format = workbook.add_format({
            'bold': True,
            'bg_color': '#F2F2F2',
            'border': 1,
            'align': 'center'
        })
        
        date_format = workbook.add_format({
            'num_format': 'mm-dd',
            'border': 1,
            'align': 'center',
            'rotation': 45
        })
        
        # Create single worksheet
        worksheet = workbook.add_worksheet('TCG_Pivot')
        
        # Get date columns (same for both pivots)
        date_columns = sorted(price_pivot.columns)
        date_strings = [d.strftime('%Y-%m-%d') for d in date_columns]
        
        current_row = 0
        
        # Section 1: PRICES
        worksheet.merge_range(current_row, 0, current_row, len(date_strings), 
                            'AVERAGE MID PRICES BY CATEGORY', section_header_format)
        current_row += 1
        
        # Headers for price section
        worksheet.write(current_row, 0, 'Category ID', header_format)
        for col, date_str in enumerate(date_strings):
            worksheet.write(current_row, col + 1, date_str, header_format)
        current_row += 1
        
        # Price data
        for cat_id in sorted(price_pivot.index):
            worksheet.write(current_row, 0, f'Category {cat_id}', category_format)
            for col, date in enumerate(date_columns):
                value = price_pivot.loc[cat_id, date] if date in price_pivot.columns else np.nan
                if pd.isna(value):
                    worksheet.write(current_row, col + 1, '', currency_format)
                else:
                    worksheet.write(current_row, col + 1, value, currency_format)
            current_row += 1
        
        # Add spacing
        current_row += 2
        
        # Section 2: COUNTS
        worksheet.merge_range(current_row, 0, current_row, len(date_strings), 
                            'PRODUCT COUNTS BY CATEGORY', section_header_format)
        current_row += 1
        
        # Headers for count section
        worksheet.write(current_row, 0, 'Category ID', header_format)
        for col, date_str in enumerate(date_strings):
            worksheet.write(current_row, col + 1, date_str, header_format)
        current_row += 1
        
        # Count data
        for cat_id in sorted(count_pivot.index):
            worksheet.write(current_row, 0, f'Category {cat_id}', category_format)
            for col, date in enumerate(date_columns):
                value = count_pivot.loc[cat_id, date] if date in count_pivot.columns else np.nan
                if pd.isna(value):
                    worksheet.write(current_row, col + 1, '', number_format)
                else:
                    worksheet.write(current_row, col + 1, value, number_format)
            current_row += 1
        
        # Set column widths
        worksheet.set_column(0, 0, 15)  # Category column
        worksheet.set_column(1, len(date_strings), 10)  # Date columns
        
        # Freeze panes
        worksheet.freeze_panes(1, 1)
    
    print(f'\\nSingle pivot table created successfully!')
    print(f'Output file: {output_file}')
    print(f'Structure:')
    print(f'- Section 1: Prices - {len(price_pivot)} categories x {len(date_columns)} weeks')
    print(f'- Section 2: Counts - {len(count_pivot)} categories x {len(date_columns)} weeks')
    
    return output_file

if __name__ == "__main__":
    create_single_pivot()
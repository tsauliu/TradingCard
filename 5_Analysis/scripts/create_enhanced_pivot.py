#!/usr/bin/env python3
"""
Enhanced pivot table script with:
1. Display names from categories reference
2. Sort by product count descending
3. Separate sheets for ASP (prices) and Product Count
"""

import pandas as pd
import numpy as np
import xlsxwriter
import os

def create_enhanced_pivot():
    # Read the data
    input_file = '/home/caoliu/TradingCard/analysis/weekly_mid_prices_with_product_count.csv'
    categories_file = '/home/caoliu/TradingCard/analysis/categories_reference.csv'
    
    print(f'Reading data from {input_file}...')
    df = pd.read_csv(input_file)
    df['week_start_date'] = pd.to_datetime(df['week_start_date'])
    
    print(f'Reading categories reference from {categories_file}...')
    categories_df = pd.read_csv(categories_file)
    
    print(f'Data loaded: {len(df)} rows, {df["category_id"].nunique()} categories')
    
    # Create mapping of category_id to display name
    category_names = dict(zip(categories_df['categoryId'], categories_df['displayName']))
    
    # Calculate total product count per category for sorting
    category_totals = df.groupby('category_id')['product_count'].sum().sort_values(ascending=False)
    sorted_categories = category_totals.index.tolist()
    
    print(f'Top 5 categories by product count:')
    for cat_id in sorted_categories[:5]:
        display_name = category_names.get(cat_id, f'Unknown_{cat_id}')
        total_count = category_totals[cat_id]
        print(f'  {cat_id}: {display_name} - {total_count:,} products')
    
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
    
    # Reorder by sorted categories
    price_pivot = price_pivot.reindex(sorted_categories)
    count_pivot = count_pivot.reindex(sorted_categories)
    
    # Create output Excel file
    output_file = '/home/caoliu/TradingCard/analysis/tcg_enhanced_pivot.xlsx'
    
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
        
        category_format = workbook.add_format({
            'bold': True,
            'bg_color': '#F2F2F2',
            'border': 1,
            'align': 'left'
        })
        
        # Get date columns
        date_columns = sorted(price_pivot.columns)
        date_strings = [d.strftime('%Y-%m-%d') for d in date_columns]
        
        # Sheet 1: ASP (Average Selling Price)
        worksheet_asp = workbook.add_worksheet('ASP')
        
        # Headers for ASP sheet
        worksheet_asp.write(0, 0, 'Category ID', header_format)
        worksheet_asp.write(0, 1, 'Category Name', header_format)
        for col, date_str in enumerate(date_strings):
            worksheet_asp.write(0, col + 2, date_str, header_format)
        
        # ASP data
        for row, cat_id in enumerate(sorted_categories):
            display_name = category_names.get(cat_id, f'Unknown_{cat_id}')
            worksheet_asp.write(row + 1, 0, cat_id, number_format)
            worksheet_asp.write(row + 1, 1, display_name, category_format)
            
            for col, date in enumerate(date_columns):
                value = price_pivot.loc[cat_id, date] if date in price_pivot.columns else np.nan
                if pd.isna(value):
                    worksheet_asp.write(row + 1, col + 2, '', currency_format)
                else:
                    worksheet_asp.write(row + 1, col + 2, value, currency_format)
        
        # Set column widths for ASP
        worksheet_asp.set_column(0, 0, 12)  # Category ID column
        worksheet_asp.set_column(1, 1, 25)  # Category Name column
        worksheet_asp.set_column(2, len(date_strings) + 1, 10)  # Date columns
        worksheet_asp.freeze_panes(1, 2)
        
        # Sheet 2: Product Count
        worksheet_count = workbook.add_worksheet('Product_Count')
        
        # Headers for Count sheet
        worksheet_count.write(0, 0, 'Category ID', header_format)
        worksheet_count.write(0, 1, 'Category Name', header_format)
        for col, date_str in enumerate(date_strings):
            worksheet_count.write(0, col + 2, date_str, header_format)
        
        # Count data
        for row, cat_id in enumerate(sorted_categories):
            display_name = category_names.get(cat_id, f'Unknown_{cat_id}')
            worksheet_count.write(row + 1, 0, cat_id, number_format)
            worksheet_count.write(row + 1, 1, display_name, category_format)
            
            for col, date in enumerate(date_columns):
                value = count_pivot.loc[cat_id, date] if date in count_pivot.columns else np.nan
                if pd.isna(value):
                    worksheet_count.write(row + 1, col + 2, '', number_format)
                else:
                    worksheet_count.write(row + 1, col + 2, value, number_format)
        
        # Set column widths for Count
        worksheet_count.set_column(0, 0, 12)  # Category ID column
        worksheet_count.set_column(1, 1, 25)  # Category Name column
        worksheet_count.set_column(2, len(date_strings) + 1, 10)  # Date columns
        worksheet_count.freeze_panes(1, 2)
        
        # Sheet 3: Summary
        worksheet_summary = workbook.add_worksheet('Summary')
        
        summary_data = []
        for cat_id in sorted_categories:
            display_name = category_names.get(cat_id, f'Unknown_{cat_id}')
            total_products = category_totals[cat_id]
            avg_price = price_pivot.loc[cat_id].mean()
            
            summary_data.append({
                'Category_ID': cat_id,
                'Display_Name': display_name,
                'Total_Product_Count': total_products,
                'Avg_Price': avg_price
            })
        
        summary_df = pd.DataFrame(summary_data)
        
        # Write summary headers
        headers = ['Category ID', 'Display Name', 'Total Product Count', 'Average Price']
        for col, header in enumerate(headers):
            worksheet_summary.write(0, col, header, header_format)
        
        # Write summary data
        for row, data in enumerate(summary_data):
            worksheet_summary.write(row + 1, 0, data['Category_ID'], number_format)
            worksheet_summary.write(row + 1, 1, data['Display_Name'], category_format)
            worksheet_summary.write(row + 1, 2, data['Total_Product_Count'], number_format)
            worksheet_summary.write(row + 1, 3, data['Avg_Price'], currency_format)
        
        worksheet_summary.set_column(0, 0, 12)  # Category ID
        worksheet_summary.set_column(1, 1, 25)  # Display Name
        worksheet_summary.set_column(2, 2, 18)  # Total Count
        worksheet_summary.set_column(3, 3, 15)  # Average Price
        worksheet_summary.freeze_panes(1, 0)
    
    print(f'\\nEnhanced pivot tables created successfully!')
    print(f'Output file: {output_file}')
    print(f'Structure:')
    print(f'- ASP Sheet: {len(sorted_categories)} categories x {len(date_columns)} weeks (sorted by product count)')
    print(f'- Product_Count Sheet: {len(sorted_categories)} categories x {len(date_columns)} weeks (sorted by product count)')
    print(f'- Summary Sheet: Category overview with totals')
    
    return output_file

if __name__ == "__main__":
    create_enhanced_pivot()
#!/usr/bin/env python3
"""
Create pivot tables from weekly tcg_prices_bda data
Structure: 
- Market Price sheet - categories as rows, dates as columns
- Quantity Sold sheet - categories as rows, dates as columns  
- Product Count sheet - categories as rows, dates as columns
"""

import pandas as pd
import numpy as np
import xlsxwriter
import os

def create_bda_pivot():
    # Read the data
    input_file = '/home/caoliu/TradingCard/analysis/weekly_bda_data.csv'
    categories_file = '/home/caoliu/TradingCard/analysis/categories_reference.csv'
    
    print(f'Reading BDA data from {input_file}...')
    df = pd.read_csv(input_file)
    df['week_start_date'] = pd.to_datetime(df['week_start_date'])
    
    print(f'Reading categories reference from {categories_file}...')
    categories_df = pd.read_csv(categories_file)
    
    print(f'BDA data loaded: {len(df)} rows, {df["category_id"].nunique()} categories')
    
    # Create mapping of category_id to display name
    category_names = dict(zip(categories_df['categoryId'], categories_df['displayName']))
    
    # Calculate total quantity sold per category for sorting
    category_totals = df.groupby('category_id')['total_quantity_sold'].sum().sort_values(ascending=False)
    sorted_categories = category_totals.index.tolist()
    
    print(f'Categories by total quantity sold:')
    for cat_id in sorted_categories:
        display_name = category_names.get(cat_id, f'Unknown_{cat_id}')
        total_qty = category_totals[cat_id]
        print(f'  {cat_id}: {display_name} - {total_qty:,} total quantity sold')
    
    # Create pivot tables
    price_pivot = df.pivot_table(
        index='category_id',
        columns='week_start_date', 
        values='avg_market_price',
        aggfunc='first'
    )
    
    quantity_pivot = df.pivot_table(
        index='category_id',
        columns='week_start_date',
        values='total_quantity_sold', 
        aggfunc='first'
    )
    
    product_pivot = df.pivot_table(
        index='category_id',
        columns='week_start_date',
        values='unique_product_count', 
        aggfunc='first'
    )
    
    # Reorder by sorted categories
    price_pivot = price_pivot.reindex(sorted_categories)
    quantity_pivot = quantity_pivot.reindex(sorted_categories)
    product_pivot = product_pivot.reindex(sorted_categories)
    
    # Create output Excel file
    output_file = '/home/caoliu/TradingCard/analysis/tcg_bda_pivot.xlsx'
    
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
        
        # Sheet 1: Market Price
        worksheet_price = workbook.add_worksheet('Market_Price')
        
        # Headers for Price sheet
        worksheet_price.write(0, 0, 'Category ID', header_format)
        worksheet_price.write(0, 1, 'Category Name', header_format)
        for col, date_str in enumerate(date_strings):
            worksheet_price.write(0, col + 2, date_str, header_format)
        
        # Price data
        for row, cat_id in enumerate(sorted_categories):
            display_name = category_names.get(cat_id, f'Unknown_{cat_id}')
            worksheet_price.write(row + 1, 0, cat_id, number_format)
            worksheet_price.write(row + 1, 1, display_name, category_format)
            
            for col, date in enumerate(date_columns):
                value = price_pivot.loc[cat_id, date] if date in price_pivot.columns else np.nan
                if pd.isna(value):
                    worksheet_price.write(row + 1, col + 2, '', currency_format)
                else:
                    worksheet_price.write(row + 1, col + 2, value, currency_format)
        
        # Set column widths for Price
        worksheet_price.set_column(0, 0, 12)  # Category ID column
        worksheet_price.set_column(1, 1, 25)  # Category Name column
        worksheet_price.set_column(2, len(date_strings) + 1, 10)  # Date columns
        worksheet_price.freeze_panes(1, 2)
        
        # Sheet 2: Quantity Sold
        worksheet_quantity = workbook.add_worksheet('Quantity_Sold')
        
        # Headers for Quantity sheet
        worksheet_quantity.write(0, 0, 'Category ID', header_format)
        worksheet_quantity.write(0, 1, 'Category Name', header_format)
        for col, date_str in enumerate(date_strings):
            worksheet_quantity.write(0, col + 2, date_str, header_format)
        
        # Quantity data
        for row, cat_id in enumerate(sorted_categories):
            display_name = category_names.get(cat_id, f'Unknown_{cat_id}')
            worksheet_quantity.write(row + 1, 0, cat_id, number_format)
            worksheet_quantity.write(row + 1, 1, display_name, category_format)
            
            for col, date in enumerate(date_columns):
                value = quantity_pivot.loc[cat_id, date] if date in quantity_pivot.columns else np.nan
                if pd.isna(value):
                    worksheet_quantity.write(row + 1, col + 2, '', number_format)
                else:
                    worksheet_quantity.write(row + 1, col + 2, value, number_format)
        
        # Set column widths for Quantity
        worksheet_quantity.set_column(0, 0, 12)  # Category ID column
        worksheet_quantity.set_column(1, 1, 25)  # Category Name column
        worksheet_quantity.set_column(2, len(date_strings) + 1, 12)  # Date columns
        worksheet_quantity.freeze_panes(1, 2)
        
        # Sheet 3: Product Count
        worksheet_products = workbook.add_worksheet('Product_Count')
        
        # Headers for Products sheet
        worksheet_products.write(0, 0, 'Category ID', header_format)
        worksheet_products.write(0, 1, 'Category Name', header_format)
        for col, date_str in enumerate(date_strings):
            worksheet_products.write(0, col + 2, date_str, header_format)
        
        # Products data
        for row, cat_id in enumerate(sorted_categories):
            display_name = category_names.get(cat_id, f'Unknown_{cat_id}')
            worksheet_products.write(row + 1, 0, cat_id, number_format)
            worksheet_products.write(row + 1, 1, display_name, category_format)
            
            for col, date in enumerate(date_columns):
                value = product_pivot.loc[cat_id, date] if date in product_pivot.columns else np.nan
                if pd.isna(value):
                    worksheet_products.write(row + 1, col + 2, '', number_format)
                else:
                    worksheet_products.write(row + 1, col + 2, value, number_format)
        
        # Set column widths for Products
        worksheet_products.set_column(0, 0, 12)  # Category ID column
        worksheet_products.set_column(1, 1, 25)  # Category Name column
        worksheet_products.set_column(2, len(date_strings) + 1, 10)  # Date columns
        worksheet_products.freeze_panes(1, 2)
        
        # Sheet 4: Summary
        worksheet_summary = workbook.add_worksheet('Summary')
        
        summary_data = []
        for cat_id in sorted_categories:
            display_name = category_names.get(cat_id, f'Unknown_{cat_id}')
            total_quantity = category_totals[cat_id]
            avg_price = price_pivot.loc[cat_id].mean()
            avg_products = product_pivot.loc[cat_id].mean()
            
            summary_data.append({
                'Category_ID': cat_id,
                'Display_Name': display_name,
                'Total_Quantity_Sold': total_quantity,
                'Avg_Market_Price': avg_price,
                'Avg_Product_Count': avg_products
            })
        
        # Write summary headers
        headers = ['Category ID', 'Display Name', 'Total Quantity Sold', 'Avg Market Price', 'Avg Product Count']
        for col, header in enumerate(headers):
            worksheet_summary.write(0, col, header, header_format)
        
        # Write summary data
        for row, data in enumerate(summary_data):
            worksheet_summary.write(row + 1, 0, data['Category_ID'], number_format)
            worksheet_summary.write(row + 1, 1, data['Display_Name'], category_format)
            worksheet_summary.write(row + 1, 2, data['Total_Quantity_Sold'], number_format)
            worksheet_summary.write(row + 1, 3, data['Avg_Market_Price'], currency_format)
            worksheet_summary.write(row + 1, 4, data['Avg_Product_Count'], number_format)
        
        worksheet_summary.set_column(0, 0, 12)  # Category ID
        worksheet_summary.set_column(1, 1, 25)  # Display Name
        worksheet_summary.set_column(2, 2, 18)  # Total Quantity
        worksheet_summary.set_column(3, 3, 15)  # Average Price
        worksheet_summary.set_column(4, 4, 15)  # Average Products
        worksheet_summary.freeze_panes(1, 0)
    
    print(f'\\nBDA pivot tables created successfully!')
    print(f'Output file: {output_file}')
    print(f'Structure:')
    print(f'- Market_Price Sheet: {len(sorted_categories)} categories x {len(date_columns)} weeks')
    print(f'- Quantity_Sold Sheet: {len(sorted_categories)} categories x {len(date_columns)} weeks') 
    print(f'- Product_Count Sheet: {len(sorted_categories)} categories x {len(date_columns)} weeks')
    print(f'- Summary Sheet: Category overview with totals')
    
    return output_file

if __name__ == "__main__":
    create_bda_pivot()
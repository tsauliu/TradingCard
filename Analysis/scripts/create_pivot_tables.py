#!/usr/bin/env python3
"""
Script to create pivot tables from weekly TCG data
Creates formatted Excel file with pivot tables by category
Columns: dates, Rows: price & product count
"""

import pandas as pd
import numpy as np
from datetime import datetime
import xlsxwriter
import os

def create_pivot_tables():
    # Read the data
    input_file = '/home/caoliu/TradingCard/analysis/weekly_mid_prices_with_product_count.csv'
    print(f'Reading data from {input_file}...')
    
    df = pd.read_csv(input_file)
    df['week_start_date'] = pd.to_datetime(df['week_start_date'])
    
    print(f'Data loaded: {len(df)} rows, {df["category_id"].nunique()} categories')
    
    # Create output Excel file
    output_file = '/home/caoliu/TradingCard/analysis/tcg_pivot_tables.xlsx'
    
    # Create Excel writer with xlsxwriter engine for formatting
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
        
        date_format = workbook.add_format({
            'num_format': 'yyyy-mm-dd',
            'border': 1,
            'bold': True,
            'bg_color': '#F2F2F2'
        })
        
        # Get unique categories
        categories = sorted(df['category_id'].unique())
        
        # Create pivot table for each category
        for cat_id in categories:
            cat_data = df[df['category_id'] == cat_id].copy()
            
            if len(cat_data) == 0:
                continue
                
            print(f'Creating pivot table for category {cat_id}...')
            
            # Create pivot table - dates as columns, metrics as rows
            pivot_data = cat_data.pivot_table(
                index=['category_id'],
                columns='week_start_date',
                values=['avg_mid_price', 'product_count'],
                aggfunc='first'
            )
            
            # Flatten column names
            pivot_data.columns = [f'{col[1].strftime("%Y-%m-%d")}' for col in pivot_data.columns]
            
            # Create a more readable format - separate price and count rows
            dates = sorted(cat_data['week_start_date'].unique())
            date_strings = [d.strftime('%Y-%m-%d') for d in dates]
            
            # Create structured data
            structured_data = []
            
            # Price row
            price_row = ['Avg Mid Price']
            for date in dates:
                price = cat_data[cat_data['week_start_date'] == date]['avg_mid_price'].iloc[0] if len(cat_data[cat_data['week_start_date'] == date]) > 0 else np.nan
                price_row.append(price)
            structured_data.append(price_row)
            
            # Product count row
            count_row = ['Product Count']
            for date in dates:
                count = cat_data[cat_data['week_start_date'] == date]['product_count'].iloc[0] if len(cat_data[cat_data['week_start_date'] == date]) > 0 else np.nan
                count_row.append(count)
            structured_data.append(count_row)
            
            # Create DataFrame
            result_df = pd.DataFrame(structured_data, columns=['Metric'] + date_strings)
            
            # Write to Excel sheet
            sheet_name = f'Category_{cat_id}'
            result_df.to_excel(writer, sheet_name=sheet_name, index=False, startrow=1)
            
            # Get worksheet for formatting
            worksheet = writer.sheets[sheet_name]
            
            # Write title
            worksheet.write(0, 0, f'Category {cat_id} - Weekly Data', header_format)
            
            # Format headers
            for col_num, value in enumerate(result_df.columns):
                if col_num == 0:
                    worksheet.write(1, col_num, value, header_format)
                else:
                    worksheet.write(1, col_num, value, date_format)
            
            # Format data
            for row_num in range(len(result_df)):
                # Metric name
                worksheet.write(row_num + 2, 0, result_df.iloc[row_num, 0], header_format)
                
                # Data values
                for col_num in range(1, len(result_df.columns)):
                    value = result_df.iloc[row_num, col_num]
                    if pd.isna(value):
                        worksheet.write(row_num + 2, col_num, '', number_format)
                    elif row_num == 0:  # Price row
                        worksheet.write(row_num + 2, col_num, value, currency_format)
                    else:  # Count row
                        worksheet.write(row_num + 2, col_num, value, number_format)
            
            # Adjust column widths
            worksheet.set_column(0, 0, 15)  # Metric column
            worksheet.set_column(1, len(date_strings), 12)  # Date columns
        
        # Create summary sheet
        summary_data = df.groupby('category_id').agg({
            'avg_mid_price': ['mean', 'min', 'max'],
            'product_count': ['mean', 'min', 'max'],
            'week_start_date': ['min', 'max']
        }).round(2)
        
        summary_data.columns = ['Avg_Price_Mean', 'Avg_Price_Min', 'Avg_Price_Max',
                               'Product_Count_Mean', 'Product_Count_Min', 'Product_Count_Max',
                               'Date_Start', 'Date_End']
        summary_data.reset_index(inplace=True)
        
        summary_data.to_excel(writer, sheet_name='Summary', index=False)
        summary_worksheet = writer.sheets['Summary']
        
        # Format summary sheet
        for col_num, value in enumerate(summary_data.columns):
            summary_worksheet.write(0, col_num, value, header_format)
        
        # Format summary data
        for row_num in range(len(summary_data)):
            for col_num, value in enumerate(summary_data.iloc[row_num]):
                if col_num == 0:  # Category ID
                    summary_worksheet.write(row_num + 1, col_num, value, number_format)
                elif 'Price' in summary_data.columns[col_num]:  # Price columns
                    summary_worksheet.write(row_num + 1, col_num, value, currency_format)
                elif 'Date' in summary_data.columns[col_num]:  # Date columns
                    summary_worksheet.write(row_num + 1, col_num, value, date_format)
                else:  # Count columns
                    summary_worksheet.write(row_num + 1, col_num, value, number_format)
        
        summary_worksheet.set_column(0, len(summary_data.columns) - 1, 15)
    
    print(f'\\nPivot tables created successfully!')
    print(f'Output file: {output_file}')
    print(f'Created {len(categories)} category sheets plus summary')
    
    return output_file

if __name__ == "__main__":
    create_pivot_tables()
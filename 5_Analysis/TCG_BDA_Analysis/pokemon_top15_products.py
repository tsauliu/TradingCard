#!/usr/bin/env python3
"""
Top 15 Pokemon Products Analysis
Outputs:
1. ASP weighted by Volume and Condition for each card
2. Trading Volumes for each card
Also saves product metadata to text file
"""

import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import os
from openpyxl import Workbook
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side
from openpyxl.utils import get_column_letter

def add_formatted_section(ws, df, start_row, title, is_price=False):
    """Add formatted data section to worksheet"""
    thin_border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )
    
    # Add title
    title_cell = ws.cell(row=start_row, column=1, value=title)
    title_cell.font = Font(bold=True, size=14, color="FFFFFF")
    title_cell.fill = PatternFill(start_color="C5504B", end_color="C5504B", fill_type="solid")
    title_cell.alignment = Alignment(horizontal="center")
    
    # Merge title across columns
    if not df.empty:
        ws.merge_cells(start_row=start_row, start_column=1, 
                      end_row=start_row, end_column=len(df.columns))
    
    # Headers
    header_row = start_row + 2
    for col_idx, col_name in enumerate(df.columns, 1):
        cell = ws.cell(row=header_row, column=col_idx, value=col_name)
        cell.font = Font(bold=True, size=11, color="FFFFFF")
        cell.fill = PatternFill(start_color="ED7D31", end_color="ED7D31", fill_type="solid")
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
        cell.border = thin_border
    
    # Data rows
    data_start_row = header_row + 1
    for row_idx, row in enumerate(df.itertuples(index=False), data_start_row):
        for col_idx, value in enumerate(row, 1):
            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            cell.border = thin_border
            
            # Format numbers
            if isinstance(value, (int, float)) and col_idx > 1:
                if is_price:
                    cell.number_format = '$#,##0.00'
                else:
                    cell.number_format = '#,##0'
            
            # Alternate row colors
            if (row_idx - data_start_row) % 2 == 1:
                cell.fill = PatternFill(start_color="FCE4D6", end_color="FCE4D6", fill_type="solid")
            
            # Alignment
            if col_idx == 1:
                cell.alignment = Alignment(horizontal="left", vertical="center")
            else:
                cell.alignment = Alignment(horizontal="right", vertical="center")
    
    # Auto-adjust column widths
    for col_idx in range(1, len(df.columns) + 1):
        max_length = 10
        column_letter = get_column_letter(col_idx)
        
        for row in range(header_row, row_idx + 1):
            cell_value = ws.cell(row=row, column=col_idx).value
            if cell_value:
                # For product names, cap at 25 characters
                if col_idx == 1:
                    max_length = max(max_length, min(len(str(cell_value)) + 2, 30))
                else:
                    max_length = max(max_length, len(str(cell_value)) + 2)
        
        ws.column_dimensions[column_letter].width = min(max_length, 25)
    
    return row_idx + 3

def save_product_links(products_df, timestamp):
    """Save product links and metadata to text file"""
    output_dir = "../output"
    os.makedirs(output_dir, exist_ok=True)
    
    text_file = f"{output_dir}/{timestamp}_pokemon_product_links.txt"
    
    with open(text_file, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("TOP 15 POKEMON PRODUCTS (EN + JP) - METADATA AND LINKS\n")
        f.write(f"Generated: {timestamp}\n")
        f.write("=" * 80 + "\n\n")
        
        for idx, row in products_df.iterrows():
            f.write(f"Rank #{idx + 1}\n")
            f.write("-" * 40 + "\n")
            f.write(f"Product ID: {row['product_id']}\n")
            f.write(f"Product Name: {row['product_name']}\n")
            f.write(f"Group: {row['group_name']}\n")
            f.write(f"Lifecycle Quantity Sold: {row['lifecycle_quantity_sold']:,}\n")
            f.write(f"Lifecycle ASP: ${row['lifecycle_asp']:.2f}\n")
            f.write(f"TCGPlayer URL: {row['product_url']}\n")
            f.write("\n")
    
    print(f"Product links saved to: {text_file}")
    return text_file

def main():
    # Setup
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/home/caoliu/TradingCard/5_Analysis/service-account.json'
    client = bigquery.Client()
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    
    print("Fetching top 15 Pokemon products (EN + JP)...")
    
    # Get top 15 Pokemon products by lifecycle quantity
    top_products_query = """
    WITH first_week_per_product AS (
        SELECT 
            product_id,
            MIN(DATE_TRUNC(bucket_start_date, WEEK(MONDAY))) as first_week
        FROM `rising-environs-456314-a3.tcg_data.tcg_prices_bda`
        WHERE bucket_start_date >= '2024-01-01'
        GROUP BY product_id
    ),
    first_week_check AS (
        SELECT 
            p.product_id,
            fw.first_week,
            SUM(p.quantity_sold) as first_week_qty
        FROM `rising-environs-456314-a3.tcg_data.tcg_prices_bda` p
        INNER JOIN first_week_per_product fw 
            ON p.product_id = fw.product_id 
            AND DATE_TRUNC(p.bucket_start_date, WEEK(MONDAY)) = fw.first_week
        WHERE p.bucket_start_date >= '2024-01-01'
        GROUP BY p.product_id, fw.first_week
        HAVING SUM(p.quantity_sold) > 0
    ),
    pokemon_products AS (
        SELECT 
            bda.product_id,
            SUM(bda.quantity_sold) as lifecycle_quantity_sold,
            CASE 
                WHEN SUM(bda.quantity_sold) > 0 
                THEN SUM(bda.quantity_sold * bda.market_price) / SUM(bda.quantity_sold)
                ELSE 0 
            END as lifecycle_asp,
            MAX(meta.product_name) as product_name,
            MAX(meta.product_cleanName) as product_clean_name,
            MAX(meta.group_name) as group_name,
            MAX(meta.product_url) as product_url
        FROM `rising-environs-456314-a3.tcg_data.tcg_prices_bda` bda
        INNER JOIN `rising-environs-456314-a3.tcg_data.tcg_metadata` meta
            ON CAST(bda.product_id AS STRING) = CAST(meta.product_productId AS STRING)
        INNER JOIN first_week_check fwc
            ON bda.product_id = fwc.product_id
        WHERE meta.category_categoryId IN (3, 85)  -- Pokemon and Pokemon Japan categories
            AND bda.quantity_sold IS NOT NULL 
            AND bda.market_price > 0
            AND bda.bucket_start_date >= '2024-01-01'
        GROUP BY bda.product_id
        HAVING lifecycle_asp > 5  -- ASP > $5 filter
    )
    SELECT *
    FROM pokemon_products
    ORDER BY lifecycle_quantity_sold DESC
    LIMIT 15
    """
    
    print("Executing top products query...")
    top_products_df = client.query(top_products_query).to_dataframe()
    
    if top_products_df.empty:
        print("No products found matching criteria.")
        return
    
    print(f"Found {len(top_products_df)} top Pokemon products")
    
    # Save product links to text file
    save_product_links(top_products_df, timestamp)
    
    # Get product IDs for detailed queries
    product_ids = top_products_df['product_id'].tolist()
    product_ids_str = "', '".join([str(pid) for pid in product_ids])
    
    # Get weekly data for these products
    weekly_data_query = f"""
    SELECT 
        bda.product_id,
        MAX(meta.product_cleanName) as product_name,
        DATE_TRUNC(bda.bucket_start_date, WEEK(MONDAY)) as week_start,
        -- Weighted ASP
        CASE 
            WHEN SUM(bda.quantity_sold) > 0 
            THEN SUM(bda.quantity_sold * bda.market_price) / SUM(bda.quantity_sold)
            ELSE 0 
        END as weighted_asp,
        -- Total volume
        SUM(bda.quantity_sold) as total_volume
    FROM `rising-environs-456314-a3.tcg_data.tcg_prices_bda` bda
    LEFT JOIN `rising-environs-456314-a3.tcg_data.tcg_metadata` meta
        ON CAST(bda.product_id AS STRING) = CAST(meta.product_productId AS STRING)
    WHERE bda.product_id IN ('{product_ids_str}')
        AND bda.bucket_start_date >= '2024-01-01'
        AND bda.market_price > 0
        AND bda.quantity_sold IS NOT NULL
        AND bda.scrape_date = '2025-08-20'  -- Specific scrape_date filter
    GROUP BY bda.product_id, DATE_TRUNC(bda.bucket_start_date, WEEK(MONDAY))
    ORDER BY bda.product_id, week_start
    """
    
    print("Fetching weekly data for top products...")
    weekly_df = client.query(weekly_data_query).to_dataframe()
    
    if weekly_df.empty:
        print("No weekly data found.")
        return
    
    print(f"Retrieved {len(weekly_df)} weekly records")
    
    # Create pivot tables
    # 1. ASP pivot - products as rows, weeks as columns
    asp_pivot = weekly_df.pivot_table(
        index=['product_id', 'product_name'],
        columns='week_start',
        values='weighted_asp',
        fill_value=0
    ).round(2)
    
    # Format column headers as dates
    asp_pivot.columns = [col.strftime('%Y-%m-%d') for col in asp_pivot.columns]
    asp_pivot = asp_pivot.reset_index()
    
    # Create display names for products (truncated for readability)
    asp_pivot['Product'] = asp_pivot.apply(
        lambda x: f"{x['product_name'][:40]}... ({x['product_id']})" 
        if len(str(x['product_name'])) > 40 
        else f"{x['product_name']} ({x['product_id']})", 
        axis=1
    )
    
    # Reorder columns
    asp_columns = ['Product'] + [col for col in asp_pivot.columns if col not in ['Product', 'product_id', 'product_name']]
    asp_display = asp_pivot[asp_columns].copy()
    
    # 2. Volume pivot - products as rows, weeks as columns  
    volume_pivot = weekly_df.pivot_table(
        index=['product_id', 'product_name'],
        columns='week_start',
        values='total_volume',
        fill_value=0
    ).astype(int)
    
    # Format column headers as dates
    volume_pivot.columns = [col.strftime('%Y-%m-%d') for col in volume_pivot.columns]
    volume_pivot = volume_pivot.reset_index()
    
    # Create display names
    volume_pivot['Product'] = volume_pivot.apply(
        lambda x: f"{x['product_name'][:40]}... ({x['product_id']})" 
        if len(str(x['product_name'])) > 40 
        else f"{x['product_name']} ({x['product_id']})", 
        axis=1
    )
    
    # Reorder columns
    volume_columns = ['Product'] + [col for col in volume_pivot.columns if col not in ['Product', 'product_id', 'product_name']]
    volume_display = volume_pivot[volume_columns].copy()
    
    # Create output directory
    output_dir = "../output"
    os.makedirs(output_dir, exist_ok=True)
    
    # Create Excel file with two sections
    excel_file = f"{output_dir}/{timestamp}_pokemon_top15_products.xlsx"
    
    wb = Workbook()
    ws = wb.active
    ws.title = "Top 15 Pokemon Products"
    
    # Add formatted sections
    current_row = 1
    
    # Section 1: Weighted ASP
    current_row = add_formatted_section(
        ws, asp_display, current_row,
        "1. Average Selling Price (ASP) - Weighted by Volume and Condition",
        is_price=True
    )
    
    # Section 2: Trading Volumes
    current_row = add_formatted_section(
        ws, volume_display, current_row,
        "2. Trading Volumes",
        is_price=False
    )
    
    # Add metadata sheet
    ws_meta = wb.create_sheet("Product Metadata")
    
    # Prepare metadata for display
    meta_display = top_products_df[['product_id', 'product_name', 'group_name', 
                                    'lifecycle_quantity_sold', 'lifecycle_asp']].copy()
    meta_display.columns = ['Product ID', 'Product Name', 'Set/Group', 
                            'Lifetime Sales', 'Lifetime ASP']
    meta_display['Rank'] = range(1, len(meta_display) + 1)
    meta_display = meta_display[['Rank', 'Product ID', 'Product Name', 'Set/Group', 
                                  'Lifetime Sales', 'Lifetime ASP']]
    
    add_formatted_section(ws_meta, meta_display, 1, "Top 15 Pokemon Products (EN + JP) - Metadata", is_price=False)
    
    # Format lifetime ASP column as currency
    for row in range(4, 4 + len(meta_display)):  # Starting from data row
        ws_meta.cell(row=row, column=6).number_format = '$#,##0.00'
    
    # Save Excel file
    wb.save(excel_file)
    
    print(f"\nExcel file saved: {excel_file}")
    print(f"Number of products: {len(top_products_df)}")
    print(f"Week range: {asp_display.columns[1]} to {asp_display.columns[-1]}")
    
    # Print top 5 products summary
    print("\nTop 5 Pokemon Products:")
    print("-" * 60)
    for idx, row in top_products_df.head(5).iterrows():
        print(f"{idx+1}. {row['product_name'][:50]}")
        print(f"   ID: {row['product_id']} | Sales: {row['lifecycle_quantity_sold']:,} | ASP: ${row['lifecycle_asp']:.2f}")

if __name__ == "__main__":
    main()
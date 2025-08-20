#!/usr/bin/env python3
"""
Price History Visualization Script
Creates basic charts for extracted price data
"""

import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import sys
import os

def visualize_price_history(csv_file):
    """Create visualizations from price history CSV"""
    if not os.path.exists(csv_file):
        print(f"Error: {csv_file} not found")
        return
    
    # Read the CSV data
    df = pd.read_csv(csv_file)
    df['date'] = pd.to_datetime(df['date'])
    
    # Create figure with subplots
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle(f'Price History for {os.path.basename(csv_file).replace("_price_history.csv", "")}', fontsize=16)
    
    # Plot 1: All prices over time
    ax1 = axes[0, 0]
    price_columns = ['amazon', 'new', 'used', 'list_price', 'collectible']
    for col in price_columns:
        if col in df.columns:
            valid_data = df[df[col].notna()]
            if not valid_data.empty:
                ax1.plot(valid_data['date'], valid_data[col], label=col.title(), alpha=0.7)
    
    ax1.set_title('All Price Types Over Time')
    ax1.set_xlabel('Date')
    ax1.set_ylabel('Price ($)')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    # Plot 2: Amazon vs New prices
    ax2 = axes[0, 1]
    if 'amazon' in df.columns:
        amazon_data = df[df['amazon'].notna()]
        if not amazon_data.empty:
            ax2.plot(amazon_data['date'], amazon_data['amazon'], label='Amazon', linewidth=2)
    
    if 'new' in df.columns:
        new_data = df[df['new'].notna()]
        if not new_data.empty:
            ax2.plot(new_data['date'], new_data['new'], label='Marketplace New', alpha=0.7)
    
    ax2.set_title('Amazon vs Marketplace New Prices')
    ax2.set_xlabel('Date')
    ax2.set_ylabel('Price ($)')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    # Plot 3: Price distribution histogram
    ax3 = axes[1, 0]
    for col in price_columns:
        if col in df.columns:
            valid_prices = df[col].dropna()
            if not valid_prices.empty:
                ax3.hist(valid_prices, bins=20, alpha=0.5, label=col.title())
    
    ax3.set_title('Price Distribution')
    ax3.set_xlabel('Price ($)')
    ax3.set_ylabel('Frequency')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    # Plot 4: Recent 30 days (if available)
    ax4 = axes[1, 1]
    recent_date = df['date'].max() - pd.Timedelta(days=30)
    recent_df = df[df['date'] >= recent_date]
    
    for col in price_columns:
        if col in df.columns:
            recent_valid = recent_df[recent_df[col].notna()]
            if not recent_valid.empty:
                ax4.plot(recent_valid['date'], recent_valid[col], label=col.title(), marker='o', markersize=3)
    
    ax4.set_title('Recent 30 Days')
    ax4.set_xlabel('Date')
    ax4.set_ylabel('Price ($)')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    
    plt.tight_layout()
    
    # Save the plot
    plot_filename = csv_file.replace('.csv', '_visualization.png')
    plt.savefig(plot_filename, dpi=300, bbox_inches='tight')
    print(f"Visualization saved to: {plot_filename}")
    
    # Display basic statistics
    print("\n=== Price Statistics ===")
    for col in price_columns:
        if col in df.columns:
            valid_prices = df[col].dropna()
            if not valid_prices.empty:
                print(f"\n{col.title()} ({len(valid_prices)} data points):")
                print(f"  Min: ${valid_prices.min():.2f}")
                print(f"  Max: ${valid_prices.max():.2f}")
                print(f"  Mean: ${valid_prices.mean():.2f}")
                print(f"  Median: ${valid_prices.median():.2f}")
    
    plt.show()

def main():
    csv_file = "B001DIJ48C_price_history.csv"
    
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
    
    try:
        visualize_price_history(csv_file)
    except ImportError as e:
        print(f"Required libraries not installed: {e}")
        print("Install with: pip install pandas matplotlib")
    except Exception as e:
        print(f"Error creating visualization: {e}")

if __name__ == "__main__":
    main()
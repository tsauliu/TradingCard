#!/usr/bin/env python3
"""
Extract PSA card IDs and names from HTML table
"""

import re
import csv
from bs4 import BeautifulSoup

def extract_card_info(html_file):
    """Extract card names, IDs, and auction results from the HTML table"""
    
    with open(html_file, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find all table rows in the tbody
    tbody = soup.find('tbody')
    rows = tbody.find_all('tr') if tbody else []
    
    card_data = []
    
    for row in rows:
        # Get all td elements in the row
        cells = row.find_all('td')
        
        if len(cells) >= 3:
            # Extract card number from first cell
            card_number = cells[0].get_text().strip()
            
            # Extract link from second cell
            link = cells[1].find('a', {'data-testid': 'link'})
            if link:
                href = link.get('href', '')
                card_name = link.get_text().strip()
                
                # Extract ID from URL using regex
                # URL format: /auctionprices/tcg-cards/1999-pokemon-game/card-name/ID
                id_match = re.search(r'/(\d+)$', href)
                
                # Extract lifecycle sales count from third cell
                lifecycle_sales_count = cells[2].get_text().strip()
                
                if id_match and lifecycle_sales_count.isdigit():
                    card_id = id_match.group(1)
                    card_data.append({
                        'card_number': int(card_number),
                        'card_name': card_name,
                        'card_id': card_id,
                        'url': href,
                        'lifecycle_sales_count': int(lifecycle_sales_count)
                    })
                    print(f"Found: #{card_number} {card_name} - ID: {card_id} - Sales: {lifecycle_sales_count}")
    
    return card_data

def save_to_csv(card_data, filename='psa_card_list.csv'):
    """Save card data to CSV"""
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['card_number', 'card_name', 'card_id', 'url', 'lifecycle_sales_count']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for card in card_data:
            writer.writerow(card)
    
    print(f"✅ Saved {len(card_data)} cards to {filename}")

def main():
    """Main function"""
    
    print("Extracting PSA card information from list.html...")
    
    # Extract data
    card_data = extract_card_info('list.html')
    
    # Save to CSV
    save_to_csv(card_data)
    
    # Show summary
    print(f"\n📊 Summary:")
    print(f"Total cards found: {len(card_data)}")
    print(f"Total lifecycle sales: {sum(card['lifecycle_sales_count'] for card in card_data):,}")
    print(f"\nTop 5 cards by sales volume:")
    sorted_cards = sorted(card_data, key=lambda x: x['lifecycle_sales_count'], reverse=True)
    for i, card in enumerate(sorted_cards[:5]):
        print(f"  {i+1}. {card['card_name']} - {card['lifecycle_sales_count']:,} sales (ID: {card['card_id']})")

if __name__ == "__main__":
    main()
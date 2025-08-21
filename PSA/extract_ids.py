#!/usr/bin/env python3
"""
Extract PSA card IDs and names from HTML table
"""

import re
import csv
from bs4 import BeautifulSoup

def extract_card_info(html_file):
    """Extract card names and IDs from the HTML table"""
    
    with open(html_file, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find all links in the table
    links = soup.find_all('a', {'data-testid': 'link'})
    
    card_data = []
    
    for link in links:
        # Extract the href URL
        href = link.get('href', '')
        
        # Extract the card name (link text)
        card_name = link.get_text().strip()
        
        # Extract ID from URL using regex
        # URL format: /auctionprices/tcg-cards/1999-pokemon-game/card-name/ID
        id_match = re.search(r'/(\d+)$', href)
        
        if id_match:
            card_id = id_match.group(1)
            card_data.append({
                'card_name': card_name,
                'card_id': card_id,
                'url': href
            })
            print(f"Found: {card_name} - ID: {card_id}")
    
    return card_data

def save_to_csv(card_data, filename='psa_card_list.csv'):
    """Save card data to CSV"""
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['card_name', 'card_id', 'url']
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
    print(f"First few cards:")
    for i, card in enumerate(card_data[:5]):
        print(f"  {i+1}. {card['card_name']} (ID: {card['card_id']})")

if __name__ == "__main__":
    main()
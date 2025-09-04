#!/usr/bin/env python3

import requests
import json

pokemon_category = '3'

print('Fetching Pokemon groups/sets...')
r = requests.get(f'https://tcgcsv.com/tcgplayer/{pokemon_category}/groups')
all_groups = r.json()['results']
print(f'Total groups found: {len(all_groups)}')
print(f'First 3 groups:')
for i, g in enumerate(all_groups[:3]):
    print(f'  {i+1}. {g["name"]} (ID: {g["groupId"]})')

print('\n' + '='*50 + '\n')

for group in all_groups[:1]:  # Process only first group
    group_id = group['groupId']
    print(f'Processing group: {group["name"]} (ID: {group_id})')
    print('\n--- PRODUCTS ---')
    
    r = requests.get(f'https://tcgcsv.com/tcgplayer/{pokemon_category}/{group_id}/products')
    products = r.json()['results']
    print(f'Total products in this group: {len(products)}')
    
    # Show first 5 products with details
    for i, product in enumerate(products[:5]):
        print(f'{i+1}. ID: {product["productId"]} - Name: {product["name"]}')
        if 'extendedData' in product and product['extendedData']:
            for ext in product['extendedData']:
                print(f'   {ext["name"]}: {ext["value"][:100] if len(ext["value"]) > 100 else ext["value"]}')
    
    print('\n--- PRICES ---')
    r = requests.get(f'https://tcgcsv.com/tcgplayer/{pokemon_category}/{group_id}/prices')
    prices = r.json()['results']
    print(f'Total price entries: {len(prices)}')
    
    # Show first 5 prices
    for i, price in enumerate(prices[:5]):
        print(f'{i+1}. Product ID: {price["productId"]} - Type: {price.get("subTypeName", "N/A")} - Mid Price: ${price.get("midPrice", "N/A")}')
    
    # Show structure of one complete product and price entry
    if products:
        print('\n--- SAMPLE PRODUCT STRUCTURE ---')
        print(json.dumps(products[0], indent=2))
    
    if prices:
        print('\n--- SAMPLE PRICE STRUCTURE ---')
        print(json.dumps(prices[0], indent=2))
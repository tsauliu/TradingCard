# Reddit Search Criteria Documentation

This document describes the search patterns used to identify mentions of Pokémon cards, trading cards, and eBay-related content in Reddit submissions and comments.

## Search Categories

### 1. Pokemon Card Total Count
**Pattern**: `pok[eé]mon card[s]?`  
**Description**: Searches for mentions of "Pokémon card" or "Pokemon card" (with optional 's' for plural) in submission titles, post text, and comment bodies using case-insensitive matching.

### 2. Trading Card Total Count
**Pattern**: `trading card[s]?`  
**Description**: Searches for mentions of "trading card" (with optional 's' for plural) in submission titles, post text, and comment bodies using case-insensitive matching.

### 3. All Cards Total Count
**Pattern**: `pok[eé]mon card[s]?` OR `trading card[s]?`  
**Description**: Searches for mentions of either "Pokémon card(s)" OR "trading card(s)" to capture the combined total of card-related discussions without double counting.

### 4. eBay Total Count
**Pattern**: `ebay`  
**Description**: Searches for mentions of "eBay" in any form (eBay, EBAY, ebay, etc.) in submission titles, post text, and comment bodies using case-insensitive matching.

### 5. eBay Trading Total Count
**Pattern**: `ebay` AND `trading card[s]?`  
**Description**: Searches for content that mentions both "eBay" and "trading card(s)" within the same submission or comment, indicating discussions about trading cards on eBay.

### 6. eBay Pokemon Total Count
**Pattern**: `ebay` AND `pok[eé]mon card[s]?`  
**Description**: Searches for content that mentions both "eBay" and "Pokémon card(s)" within the same submission or comment, indicating discussions about Pokémon cards on eBay.

### 7. eBay All Cards Total Count
**Pattern**: `ebay` AND (`pok[eé]mon card[s]?` OR `trading card[s]?`)  
**Description**: Searches for content that mentions "eBay" along with either "Pokémon card(s)" OR "trading card(s)" to capture all eBay-related card discussions.

## Technical Implementation Notes

- All searches use case-insensitive matching via the `LOWER()` function
- Regex patterns support both accented (é) and regular (e) versions of "Pokémon"
- Optional 's' matching allows for both singular and plural forms
- For submissions, searches cover both title and selftext fields
- For comments, searches cover the body field
- Date range covers all available data in the Reddit dataset
- Results are aggregated by date and combined across submissions and comments
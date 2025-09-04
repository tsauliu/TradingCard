#!/usr/bin/env python3
"""
Save PSA grade list to CSV
"""

import csv

def save_psa_grades():
    """Save PSA grades to CSV file"""
    
    # PSA Grades from the scraper
    grades = [
        {"value": "10", "label": "PSA 10"},
        {"value": "9", "label": "PSA 9"}, 
        {"value": "8.5", "label": "PSA 8.5"},
        {"value": "8", "label": "PSA 8"},
        {"value": "7.5", "label": "PSA 7.5"},
        {"value": "7", "label": "PSA 7"},
        {"value": "6.5", "label": "PSA 6.5"},
        {"value": "6", "label": "PSA 6"},
        {"value": "5.5", "label": "PSA 5.5"},
        {"value": "5", "label": "PSA 5"},
        {"value": "4.5", "label": "PSA 4.5"},
        {"value": "4", "label": "PSA 4"},
        {"value": "3.5", "label": "PSA 3.5"},
        {"value": "3", "label": "PSA 3"},
        {"value": "2.5", "label": "PSA 2.5"},
        {"value": "2", "label": "PSA 2"},
        {"value": "1.5", "label": "PSA 1.5"},
        {"value": "1", "label": "PSA 1"},
        {"value": "0", "label": "Auth"}
    ]
    
    filename = 'psa_grades_list.csv'
    
    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['grade_value', 'grade_label']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        writer.writeheader()
        for grade in grades:
            writer.writerow({
                'grade_value': grade['value'],
                'grade_label': grade['label']
            })
    
    print(f"✅ Saved {len(grades)} PSA grades to {filename}")
    
    # Show the grades
    print("\n📊 PSA Grades List:")
    for i, grade in enumerate(grades, 1):
        print(f"  {i:2d}. {grade['label']} (value: {grade['value']})")

if __name__ == "__main__":
    save_psa_grades()
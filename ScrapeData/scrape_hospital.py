"""
Hospital Data Scraper for Thai Health Insurance Network Hospitals
URL: https://www.thaihealth.co.th/en/list-of-network-hospital-bangkok-area/

This script scrapes hospital information including:
- ชื่อสถานพยาบาล (Hospital Name in Thai)
- Hospital Name (English)
- หมายเลขโทรศัพท์ (Telephone Number)

Requirements:
    pip install requests beautifulsoup4 pandas openpyxl

Usage:
    python scrape_hospitals.py
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re

def scrape_hospitals(url):
    """
    Scrape hospital data from the Thai Health website
    
    Args:
        url (str): The URL to scrape
        
    Returns:
        list: List of dictionaries containing hospital data
    """
    
    # Send GET request with headers
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'utf-8'
        response.raise_for_status()
        print(f"✓ Successfully fetched the webpage")
    except requests.RequestException as e:
        print(f"✗ Error fetching the URL: {e}")
        return []
    
    # Parse HTML
    soup = BeautifulSoup(response.content, 'html.parser')
    
    hospitals = []
    
    # Find all tables
    tables = soup.find_all('table')
    print(f"Found {len(tables)} table(s) on the page")
    
    for table_idx, table in enumerate(tables):
        print(f"\nProcessing table {table_idx + 1}...")
        
        # Get all rows
        rows = table.find_all('tr')
        print(f"  Found {len(rows)} rows")
        
        # Track column indices
        thai_name_col = None
        eng_name_col = None
        phone_col = None
        
        for row_idx, row in enumerate(rows):
            cells = row.find_all(['td', 'th'])
            
            if len(cells) == 0:
                continue
            
            cell_texts = [cell.get_text(strip=True) for cell in cells]
            
            # Check if this is a header row
            row_text = ' '.join(cell_texts).lower()
            
            if 'ชื่อสถานพยาบาล' in row_text or 'hospital' in row_text:
                # This is a header row - identify columns
                print(f"  Header row found at row {row_idx + 1}")
                
                for idx, text in enumerate(cell_texts):
                    if 'ชื่อสถานพยาบาล' in text:
                        thai_name_col = idx
                        print(f"    Thai name column: {idx}")
                    elif 'list of network hospital' in text.lower() and eng_name_col is None:
                        eng_name_col = idx
                        print(f"    English name column: {idx}")
                    elif 'หมายเลขโทรศัพท์' in text or 'telephone' in text.lower():
                        phone_col = idx
                        print(f"    Phone column: {idx}")
                
                continue
            
            # Skip if we haven't found the header yet
            if thai_name_col is None:
                continue
            
            # Extract data rows
            if len(cell_texts) > max(thai_name_col or 0, eng_name_col or 0, phone_col or 0):
                thai_name = cell_texts[thai_name_col] if thai_name_col is not None else ''
                eng_name = cell_texts[eng_name_col] if eng_name_col is not None else ''
                phone = cell_texts[phone_col] if phone_col is not None else ''
                
                # Skip empty rows and header-like rows
                if thai_name and thai_name not in ['ชื่อสถานพยาบาล', '---']:
                    # Skip rows that are just numbers (row numbers)
                    if not thai_name.isdigit():
                        hospitals.append({
                            'ลำดับ': row_idx,
                            'ชื่อสถานพยาบาล (Thai)': thai_name,
                            'Hospital Name (English)': eng_name,
                            'หมายเลขโทรศัพท์ (Telephone)': phone
                        })
    
    return hospitals


def save_to_files(data, base_filename='bangkok_hospitals'):
    """
    Save data to CSV and Excel files
    
    Args:
        data (list): List of dictionaries
        base_filename (str): Base name for output files
    """
    if not data:
        print("No data to save.")
        return
    
    df = pd.DataFrame(data)
    
    # Save to CSV with UTF-8 encoding (with BOM for Excel compatibility)
    csv_filename = f'{base_filename}.csv'
    df.to_csv(csv_filename, index=False, encoding='utf-8-sig')
    print(f"✓ Data saved to '{csv_filename}'")
    
    # Save to Excel
    excel_filename = f'{base_filename}.xlsx'
    df.to_excel(excel_filename, index=False, engine='openpyxl')
    print(f"✓ Data saved to '{excel_filename}'")


def main():
    """Main function"""
    
    url = "https://www.thaihealth.co.th/en/list-of-network-hospital-bangkok-area/"
    
    print("=" * 70)
    print("Bangkok Hospital Network Data Scraper")
    print("=" * 70)
    print(f"\nTarget URL: {url}\n")
    
    # Scrape the data
    hospitals = scrape_hospitals(url)
    
    # Display results
    if hospitals:
        print(f"\n{'=' * 70}")
        print(f"✓ Successfully extracted {len(hospitals)} hospital records")
        print("=" * 70)
        
        # Display first 5 records as preview
        preview_count = min(5, len(hospitals))
        print(f"\nShowing first {preview_count} records:")
        print("-" * 70)
        
        for idx, hospital in enumerate(hospitals[:preview_count], 1):
            print(f"\nRecord {idx}:")
            print(f"  ชื่อสถานพยาบาล: {hospital['ชื่อสถานพยาบาล (Thai)']}")
            print(f"  Hospital Name: {hospital['Hospital Name (English)']}")
            print(f"  หมายเลขโทรศัพท์: {hospital['หมายเลขโทรศัพท์ (Telephone)']}")
        
        if len(hospitals) > preview_count:
            print(f"\n... and {len(hospitals) - preview_count} more records")
        
        print("\n" + "-" * 70)
        
        # Save to files
        print("\nSaving data to files...")
        save_to_files(hospitals)
        
        # Display summary statistics
        print(f"\n{'=' * 70}")
        print("Summary Statistics:")
        print(f"  Total hospitals: {len(hospitals)}")
        print(f"  Hospitals with phone numbers: {sum(1 for h in hospitals if h['หมายเลขโทรศัพท์ (Telephone)'])}")
        print("=" * 70)
        
    else:
        print("=" * 70)
        print("✗ No hospital data found.")
        print("=" * 70)
        print("\nPossible reasons:")
        print("  - The page structure might have changed")
        print("  - Network connection issues")
        print("  - The table format is different than expected")
    
    print("\n" + "=" * 70)
    print("Script completed")
    print("=" * 70)


if __name__ == "__main__":
    main()
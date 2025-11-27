"""
Fire Station Data Scraper for http://www.manoontham.com/policetelephone.html

This script scrapes fire station information including:
- ชื่อ (Name)
- หมายเลขโทรศัพท์ (Phone Number)
- โทรสาร (Fax Number)

Requirements:
    pip install requests beautifulsoup4 pandas openpyxl

Usage:
    python scrape_firestation.py
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd

def scrape_fire_stations(url):
    """
    Scrape fire station data from the given URL
    
    Args:
        url (str): The URL to scrape
        
    Returns:
        list: List of dictionaries containing fire station data
    """
    
    # Send GET request with headers to mimic a browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.encoding = 'utf-8'  # Important for Thai characters
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Error fetching the URL: {e}")
        return []
    
    # Parse HTML
    soup = BeautifulSoup(response.content, 'html.parser')
    
    fire_stations = []
    
    # Method 1: Look for tables with class 'mceVisualAid'
    tables = soup.find_all('table', class_='mceVisualAid')
    
    print(f"Found {len(tables)} tables with class 'mceVisualAid'")
    
    for table_idx, table in enumerate(tables):
        print(f"\nProcessing table {table_idx + 1}...")
        rows = table.find_all('tr')
        print(f"  Found {len(rows)} rows in this table")
        
        # Track if we found the header row
        header_found = False
        header_indices = {'name': 0, 'phone': 1, 'fax': 2}
        
        for row_idx, row in enumerate(rows):
            cells = row.find_all(['td', 'th'])
            
            if len(cells) == 0:
                continue
                
            cell_texts = [cell.get_text(strip=True) for cell in cells]
            
            # Check if this is a header row
            row_text = ''.join(cell_texts).lower()
            if 'ชื่อ' in row_text or 'หมายเลข' in row_text or 'โทรศัพท์' in row_text:
                header_found = True
                print(f"  Header row found at row {row_idx + 1}: {cell_texts}")
                
                # Try to identify column positions
                for idx, text in enumerate(cell_texts):
                    if 'ชื่อ' in text:
                        header_indices['name'] = idx
                    elif 'หมายเลข' in text or 'โทรศัพท์' in text:
                        header_indices['phone'] = idx
                    elif 'โทรสาร' in text or 'แฟกซ์' in text or 'fax' in text.lower():
                        header_indices['fax'] = idx
                
                print(f"  Column indices: {header_indices}")
                continue
            
            # If we have enough cells and this is a data row
            if len(cell_texts) >= 2:
                # Get the data based on column positions
                name = cell_texts[header_indices['name']] if header_indices['name'] < len(cell_texts) else ''
                phone = cell_texts[header_indices['phone']] if header_indices['phone'] < len(cell_texts) else ''
                fax = cell_texts[header_indices['fax']] if header_indices['fax'] < len(cell_texts) else ''
                
                # Only add if we have at least a name
                if name and name != '' and not any(skip in name.lower() for skip in ['ชื่อ', 'หมายเลข', 'โทรศัพท์']):
                    print(f"  Data row {row_idx + 1}: Name='{name[:30]}...', Phone='{phone}', Fax='{fax}'")
                    fire_stations.append({
                        'ชื่อ': name,
                        'หมายเลขโทรศัพท์': phone,
                        'โทรสาร': fax
                    })
    
    # Method 2: If no tables with that class, look for all tables
    if len(tables) == 0:
        print("\nNo tables with class 'mceVisualAid' found. Trying all tables...")
        all_tables = soup.find_all('table')
        print(f"Found {len(all_tables)} total tables")
        
        for table_idx, table in enumerate(all_tables):
            print(f"\nProcessing table {table_idx + 1}...")
            rows = table.find_all('tr')
            
            header_indices = {'name': 0, 'phone': 1, 'fax': 2}
            
            for row_idx, row in enumerate(rows):
                cells = row.find_all(['td', 'th'])
                
                if len(cells) == 0:
                    continue
                    
                cell_texts = [cell.get_text(strip=True) for cell in cells]
                
                # Skip header rows
                row_text = ''.join(cell_texts).lower()
                if 'ชื่อ' in row_text or 'หมายเลข' in row_text:
                    # Identify column positions
                    for idx, text in enumerate(cell_texts):
                        if 'ชื่อ' in text:
                            header_indices['name'] = idx
                        elif 'หมายเลข' in text or 'โทรศัพท์' in text:
                            header_indices['phone'] = idx
                        elif 'โทรสาร' in text or 'แฟกซ์' in text:
                            header_indices['fax'] = idx
                    continue
                
                # Extract data
                if len(cell_texts) >= 2:
                    name = cell_texts[header_indices['name']] if header_indices['name'] < len(cell_texts) else ''
                    phone = cell_texts[header_indices['phone']] if header_indices['phone'] < len(cell_texts) else ''
                    fax = cell_texts[header_indices['fax']] if header_indices['fax'] < len(cell_texts) else ''
                    
                    if name and name != '' and not any(skip in name.lower() for skip in ['ชื่อ', 'หมายเลข']):
                        fire_stations.append({
                            'ชื่อ': name,
                            'หมายเลขโทรศัพท์': phone,
                            'โทรสาร': fax
                        })
    
    return fire_stations


def save_to_files(data, base_filename='fire_stations'):
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
    
    url = "https://www.bangkokfire.go.th/phone-number/"
    
    print("=" * 70)
    print("Fire Station Data Scraper")
    print("=" * 70)
    print(f"\nFetching data from: {url}\n")
    
    # Scrape the data
    fire_stations = scrape_fire_stations(url)
    
    # Display results
    if fire_stations:
        print(f"\n{'=' * 70}")
        print(f"✓ Successfully extracted {len(fire_stations)} fire station records")
        print("=" * 70)
        
        # Display first 5 records as preview
        preview_count = min(5, len(fire_stations))
        print(f"\nShowing first {preview_count} records:")
        print("-" * 70)
        
        for idx, station in enumerate(fire_stations[:preview_count], 1):
            print(f"\nRecord {idx}:")
            print(f"  ชื่อ: {station['ชื่อ']}")
            print(f"  หมายเลขโทรศัพท์: {station['หมายเลขโทรศัพท์']}")
            print(f"  โทรสาร: {station['โทรสาร']}")
        
        if len(fire_stations) > 5:
            print(f"\n... and {len(fire_stations) - 5} more records")
        
        print("\n" + "-" * 70)
        
        # Save to files
        save_to_files(fire_stations)
        
    else:
        print("=" * 70)
        print("✗ No fire station data found.")
        print("=" * 70)
        print("\nPossible reasons:")
        print("  - The page structure might be different than expected")
        print("  - The table class 'mceVisualAid' might not exist")
        print("  - Network connection issues")
        print("\nTip: Try visiting the URL in your browser to verify the data exists.")
    
    print("\n" + "=" * 70)
    print("Script completed")
    print("=" * 70)


if __name__ == "__main__":
    main()
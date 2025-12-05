"""
Geocoding Script - Add Latitude and Longitude to CSV Files
============================================================

This script uses Google Maps Geocoding API to find coordinates for:
1. Bangkok hospitals (based on Hospital Name)
2. Bangkok fire stations (based on Station Name)

The coordinates are appended as new columns to the CSV files.

Usage:
    python geocode_locations.py --api_key YOUR_GOOGLE_MAPS_API_KEY
    
Or edit the API_KEY variable below and run:
    python geocode_locations.py

Author: Bangkok Emergency Services Analysis
Date: December 2024
"""

import pandas as pd
import requests
import time
import sys
import os
from typing import Tuple, Optional

# ============================================================================
# CONFIGURATION
# ============================================================================

# Add your Google Maps API key here
API_KEY = "AIzaSyCfj762u52yTA90Nvx_kxpVsgCEUGHTfaY"  # Or pass via command line: --api_key YOUR_KEY

# Input file paths
HOSPITALS_FILE = r"C:\Users\SorapatPun\Desktop\VScode\DSDE Project\2110531_DSDE_Final_Project\DE_Viz\input_csv_file\bangkok_hospitals.csv"
FIRE_STATIONS_FILE = r"C:\Users\SorapatPun\Desktop\VScode\DSDE Project\2110531_DSDE_Final_Project\DE_Viz\input_csv_file\fire_stations.csv"

# Output file paths (will overwrite originals or create new files)
HOSPITALS_OUTPUT = "bangkok_hospitals_geocoded.csv"
FIRE_STATIONS_OUTPUT = "fire_stations_geocoded.csv"

# Geocoding settings
DELAY_BETWEEN_REQUESTS = 0.2  # seconds (to avoid rate limits)
MAX_RETRIES = 3


# ============================================================================
# GEOCODING FUNCTIONS
# ============================================================================

def geocode_address(address: str, api_key: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Geocode an address using Google Maps Geocoding API.
    
    Args:
        address: Address or place name to geocode
        api_key: Google Maps API key
        
    Returns:
        Tuple of (latitude, longitude) or (None, None) if failed
    """
    base_url = "https://maps.googleapis.com/maps/api/geocode/json"
    
    # Add "Bangkok, Thailand" to improve accuracy
    search_query = f"{address}, Bangkok, Thailand"
    
    params = {
        "address": search_query,
        "key": api_key,
        "region": "th"  # Bias results to Thailand
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(base_url, params=params, timeout=10)
            data = response.json()
            
            if data['status'] == 'OK' and len(data['results']) > 0:
                location = data['results'][0]['geometry']['location']
                return location['lat'], location['lng']
            elif data['status'] == 'ZERO_RESULTS':
                print(f"  ⚠️  No results found for: {address}")
                return None, None
            else:
                print(f"  ⚠️  API returned status: {data['status']} for: {address}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(1)
                    continue
                return None, None
                
        except Exception as e:
            print(f"  ❌ Error geocoding {address}: {str(e)}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(1)
                continue
            return None, None
    
    return None, None


def geocode_hospitals(api_key: str) -> pd.DataFrame:
    """
    Geocode all hospitals in the bangkok_hospitals.csv file.
    
    Args:
        api_key: Google Maps API key
        
    Returns:
        DataFrame with added latitude and longitude columns
    """
    print("\n" + "="*70)
    print("🏥 GEOCODING BANGKOK HOSPITALS")
    print("="*70)
    
    # Load CSV
    df = pd.read_csv(HOSPITALS_FILE, encoding='utf-8-sig')
    print(f"✅ Loaded {len(df)} hospitals from CSV")
    
    # Check if columns already exist
    if 'latitude' not in df.columns:
        df['latitude'] = None
    if 'longitude' not in df.columns:
        df['longitude'] = None
    
    # Get column name for hospital name
    hospital_name_col = 'Hospital Name (English)'
    if hospital_name_col not in df.columns:
        # Try to find the right column
        possible_cols = [col for col in df.columns if 'hospital' in col.lower() or 'name' in col.lower()]
        if possible_cols:
            hospital_name_col = possible_cols[0]
        else:
            print(f"❌ Cannot find hospital name column in: {df.columns.tolist()}")
            return df
    
    print(f"📍 Geocoding column: {hospital_name_col}")
    print(f"⏱️  Estimated time: ~{len(df) * DELAY_BETWEEN_REQUESTS:.1f} seconds\n")
    
    # Geocode each hospital
    success_count = 0
    
    for idx, row in df.iterrows():
        hospital_name = row[hospital_name_col]
        
        # Skip if already geocoded
        if pd.notna(row['latitude']) and pd.notna(row['longitude']):
            print(f"[{idx+1}/{len(df)}] ⏭️  Skipping (already geocoded): {hospital_name}")
            success_count += 1
            continue
        
        print(f"[{idx+1}/{len(df)}] 🔍 Geocoding: {hospital_name}")
        
        lat, lng = geocode_address(hospital_name, api_key)
        
        if lat is not None and lng is not None:
            df.at[idx, 'latitude'] = lat
            df.at[idx, 'longitude'] = lng
            print(f"           ✅ Found: {lat:.6f}, {lng:.6f}")
            success_count += 1
        else:
            print(f"           ❌ Failed to geocode")
        
        # Rate limiting
        time.sleep(DELAY_BETWEEN_REQUESTS)
    
    print(f"\n✅ Successfully geocoded: {success_count}/{len(df)} hospitals")
    
    return df


def geocode_fire_stations(api_key: str) -> pd.DataFrame:
    """
    Geocode all fire stations in the fire_stations.csv file.
    
    Args:
        api_key: Google Maps API key
        
    Returns:
        DataFrame with added latitude and longitude columns
    """
    print("\n" + "="*70)
    print("🚒 GEOCODING BANGKOK FIRE STATIONS")
    print("="*70)
    
    # Load CSV
    df = pd.read_csv(FIRE_STATIONS_FILE, encoding='utf-8-sig')
    print(f"✅ Loaded {len(df)} fire stations from CSV")
    
    # Check if columns already exist
    if 'latitude' not in df.columns:
        df['latitude'] = None
    if 'longitude' not in df.columns:
        df['longitude'] = None
    
    # Get column name for station name
    station_name_col = 'ชื่อ'  # Thai for "name"
    if station_name_col not in df.columns:
        # Try to find the right column
        possible_cols = df.columns.tolist()
        if possible_cols:
            station_name_col = possible_cols[0]  # Use first column
        else:
            print(f"❌ Cannot find station name column")
            return df
    
    print(f"📍 Geocoding column: {station_name_col}")
    print(f"⏱️  Estimated time: ~{len(df) * DELAY_BETWEEN_REQUESTS:.1f} seconds\n")
    
    # Geocode each fire station
    success_count = 0
    
    for idx, row in df.iterrows():
        station_name = row[station_name_col]
        
        # Skip empty rows or header rows
        if pd.isna(station_name) or str(station_name).strip() == '':
            continue
        
        # Skip if already geocoded
        if pd.notna(row['latitude']) and pd.notna(row['longitude']):
            print(f"[{idx+1}/{len(df)}] ⏭️  Skipping (already geocoded): {station_name}")
            success_count += 1
            continue
        
        print(f"[{idx+1}/{len(df)}] 🔍 Geocoding: {station_name}")
        
        lat, lng = geocode_address(station_name, api_key)
        
        if lat is not None and lng is not None:
            df.at[idx, 'latitude'] = lat
            df.at[idx, 'longitude'] = lng
            print(f"           ✅ Found: {lat:.6f}, {lng:.6f}")
            success_count += 1
        else:
            print(f"           ❌ Failed to geocode")
        
        # Rate limiting
        time.sleep(DELAY_BETWEEN_REQUESTS)
    
    print(f"\n✅ Successfully geocoded: {success_count}/{len(df)} fire stations")
    
    return df


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main execution function."""
    
    # Check for API key
    api_key = API_KEY
    
    # Check command line arguments
    if len(sys.argv) > 2 and sys.argv[1] == '--api_key':
        api_key = sys.argv[2]
    
    if not api_key:
        print("\n" + "="*70)
        print("❌ ERROR: Google Maps API Key Required")
        print("="*70)
        print("\nPlease provide your API key in one of these ways:")
        print("1. Edit API_KEY variable in this script")
        print("2. Run: python geocode_locations.py --api_key YOUR_KEY")
        print("\nGet API key at: https://console.cloud.google.com/")
        print("Enable 'Geocoding API' for your project")
        print("="*70)
        sys.exit(1)
    
    print("\n" + "="*70)
    print("🚀 BANGKOK EMERGENCY SERVICES - GEOCODING SCRIPT")
    print("="*70)
    print(f"📂 Hospitals input: {HOSPITALS_FILE}")
    print(f"📂 Fire stations input: {FIRE_STATIONS_FILE}")
    print(f"💾 Hospitals output: {HOSPITALS_OUTPUT}")
    print(f"💾 Fire stations output: {FIRE_STATIONS_OUTPUT}")
    print("="*70)
    
    # Check if input files exist
    if not os.path.exists(HOSPITALS_FILE):
        print(f"\n❌ ERROR: Hospitals file not found: {HOSPITALS_FILE}")
        sys.exit(1)
    
    if not os.path.exists(FIRE_STATIONS_FILE):
        print(f"\n❌ ERROR: Fire stations file not found: {FIRE_STATIONS_FILE}")
        sys.exit(1)
    
    # Geocode hospitals
    hospitals_df = geocode_hospitals(api_key)
    
    # Save hospitals
    hospitals_df.to_csv(HOSPITALS_OUTPUT, index=False, encoding='utf-8-sig')
    print(f"💾 Saved to: {HOSPITALS_OUTPUT}")
    
    # Geocode fire stations
    fire_stations_df = geocode_fire_stations(api_key)
    
    # Save fire stations
    fire_stations_df.to_csv(FIRE_STATIONS_OUTPUT, index=False, encoding='utf-8-sig')
    print(f"💾 Saved to: {FIRE_STATIONS_OUTPUT}")
    
    # Summary
    print("\n" + "="*70)
    print("✅ GEOCODING COMPLETE!")
    print("="*70)
    
    # Count successful geocoding
    hospitals_success = hospitals_df['latitude'].notna().sum()
    stations_success = fire_stations_df['latitude'].notna().sum()
    
    print(f"🏥 Hospitals: {hospitals_success}/{len(hospitals_df)} geocoded successfully")
    print(f"🚒 Fire Stations: {stations_success}/{len(fire_stations_df)} geocoded successfully")
    print("\n📊 You can now use these files in your Streamlit app!")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()

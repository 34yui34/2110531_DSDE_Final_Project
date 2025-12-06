"""
Data Store Module for Bangkok Emergency Services
Loads static data from CSV files (hospitals, fire stations, incidents, population)
"""

import pandas as pd
import os


class DataStore:
    """
    Data store for loading Bangkok emergency services data.
    Handles hospitals, fire stations, fire incidents, and population data.
    """
    
    def __init__(self):
        """Initialize the data store."""
        self.hospitals_df = None
        self.fire_stations_df = None
        self.fire_disaster_df = None
        self.population_df = None
        
        # Define possible base paths for data
        self.base_paths = [
            "input_csv_file",  # Current directory
            "./input_csv_file",  # Explicit current
            "../input_csv_file",  # Parent directory
            "../../input_csv_file",  # Two levels up
            r"C:\Users\SorapatPun\Desktop\VScode\DSDE Project\2110531_DSDE_Final_Project\DE_Viz\input_csv_file",  # Absolute path
        ]
    
    def _find_file(self, filename):
        """
        Search for a file in multiple possible locations.
        
        Args:
            filename: Name of the file to find
            
        Returns:
            Full path to the file, or None if not found
        """
        for base_path in self.base_paths:
            full_path = os.path.join(base_path, filename)
            if os.path.exists(full_path):
                print(f"✅ Found {filename} at: {full_path}")
                return full_path
        
        print(f"❌ Could not find {filename} in any of these locations:")
        for base_path in self.base_paths:
            print(f"   - {os.path.join(base_path, filename)}")
        return None
    
    def load_hospitals(self):
        """Load hospital data from CSV."""
        # Try geocoded version first, then original
        geocoded_path = self._find_file("bangkok_hospitals_geocoded.csv")
        original_path = self._find_file("bangkok_hospitals.csv")
        
        if geocoded_path:
            self.hospitals_df = pd.read_csv(geocoded_path)
            print(f"📍 Loaded {len(self.hospitals_df)} hospitals (with coordinates)")
        elif original_path:
            self.hospitals_df = pd.read_csv(original_path)
            print(f"🏥 Loaded {len(self.hospitals_df)} hospitals (no coordinates)")
        else:
            print("⚠️ No hospital data found")
            self.hospitals_df = pd.DataFrame()
    
    def load_fire_stations(self):
        """Load fire station data from CSV."""
        # Try geocoded version first, then original
        geocoded_path = self._find_file("fire_stations_geocoded.csv")
        original_path = self._find_file("fire_stations.csv")
        
        if geocoded_path:
            self.fire_stations_df = pd.read_csv(geocoded_path)
            print(f"📍 Loaded {len(self.fire_stations_df)} fire stations (with coordinates)")
        elif original_path:
            self.fire_stations_df = pd.read_csv(original_path)
            print(f"🚒 Loaded {len(self.fire_stations_df)} fire stations (no coordinates)")
        else:
            print("⚠️ No fire station data found")
            self.fire_stations_df = pd.DataFrame()
    
    def load_fire_disaster(self):
        """Load fire disaster/incident data from CSV."""
        path = self._find_file("fire_disaster_data.csv")
        
        if path:
            self.fire_disaster_df = pd.read_csv(path)
            print(f"🔥 Loaded {len(self.fire_disaster_df)} fire incidents")
        else:
            print("⚠️ No fire disaster data found")
            self.fire_disaster_df = pd.DataFrame()
    
    def load_population(self):
        """Load population data from CSV."""
        path = self._find_file("population.csv")
        
        if path:
            self.population_df = pd.read_csv(path)
            print(f"👥 Loaded population data for {len(self.population_df)} districts")
        else:
            print("⚠️ No population data found")
            self.population_df = pd.DataFrame()
    
    def load_all_data(self):
        """Load all datasets."""
        print("📊 Loading all data...")
        self.load_hospitals()
        self.load_fire_stations()
        self.load_fire_disaster()
        self.load_population()
        print("✅ All data loaded successfully!")
    
    # Getter methods to return pandas DataFrames
    def get_hospitals_pandas(self):
        """Get hospitals data as pandas DataFrame."""
        return self.hospitals_df if self.hospitals_df is not None else pd.DataFrame()
    
    def get_fire_stations_pandas(self):
        """Get fire stations data as pandas DataFrame."""
        return self.fire_stations_df if self.fire_stations_df is not None else pd.DataFrame()
    
    def get_fire_disaster_pandas(self):
        """Get fire disaster data as pandas DataFrame."""
        return self.fire_disaster_df if self.fire_disaster_df is not None else pd.DataFrame()
    
    def get_population_pandas(self):
        """Get population data as pandas DataFrame."""
        return self.population_df if self.population_df is not None else pd.DataFrame()


# Test function
if __name__ == "__main__":
    print("Testing DataStore...")
    store = DataStore()
    store.load_all_data()
    
    print("\n📊 Data Summary:")
    print(f"Hospitals: {len(store.get_hospitals_pandas())} records")
    print(f"Fire Stations: {len(store.get_fire_stations_pandas())} records")
    print(f"Fire Incidents: {len(store.get_fire_disaster_pandas())} records")
    print(f"Population: {len(store.get_population_pandas())} records")
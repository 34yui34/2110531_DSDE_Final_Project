"""
PySpark Data Store Manager
===========================

This module provides a centralized PySpark data storage and retrieval system.
Other Python files can import this module to access the data.

Features:
- Loads all CSV files into PySpark DataFrames
- Converts to Pandas when needed
- Caches data for performance
- Provides easy access methods

Usage in other files:
    from data_store import DataStore
    
    store = DataStore()
    hospitals_df = store.get_hospitals_pandas()
    fire_stations_df = store.get_fire_stations_pandas()

Author: Bangkok Emergency Services Analysis
Date: December 2024
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
import pandas as pd
import os
from typing import Optional


class DataStore:
    """
    Centralized data storage using PySpark.
    Provides access to all Bangkok emergency services datasets.
    """
    
    _instance = None
    _spark = None
    _data_loaded = False
    
    # Not using DATA_DIR since we have full paths
    DATA_DIR = None
    
    # Full paths to your files - FIXED PATHS (removed duplicate dataset/DE_Viz/)
    HOSPITALS_FILE = "./dataset/webscraping/output_csv_file/bangkok_hospitals_geocoded.csv"
    HOSPITALS_FALLBACK = "./dataset/webscraping/input_csv_file/bangkok_hospitals.csv"
    
    FIRE_STATIONS_FILE = "./dataset/webscraping/output_csv_file/fire_stations_geocoded.csv"
    FIRE_STATIONS_FALLBACK = "./dataset/webscraping/input_csv_file/fire_stations.csv"
    
    FIRE_DISASTER_FILE = "./dataset/webscraping/input_csv_file/fire_disaster_data.csv"
    POPULATION_FILE = "./dataset/webscraping/input_csv_file/population.csv"
    
    def __new__(cls):
        """Singleton pattern - only one instance of DataStore."""
        if cls._instance is None:
            cls._instance = super(DataStore, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize the DataStore with Spark session."""
        if not self._spark:
            self._spark = self._create_spark_session()
            print("✅ PySpark session initialized")
    
    @staticmethod
    def _create_spark_session() -> SparkSession:
        """Create and configure Spark session."""
        spark = SparkSession.builder \
            .appName("BangkokEmergencyDataStore") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        return spark
    
    def _get_file_path(self, filename: str, fallback: Optional[str] = None) -> str:
        """
        Get file path, using fallback if primary doesn't exist.
        
        Args:
            filename: Primary file path
            fallback: Fallback file path if primary not found
            
        Returns:
            Full path to the file
        """
        # Check if primary file exists
        if os.path.exists(filename):
            print(f"✅ Found: {filename}")
            return filename
        
        # Try fallback if provided
        if fallback and os.path.exists(fallback):
            print(f"ℹ️  Primary file not found, using fallback: {fallback}")
            return fallback
        
        # Return primary even if doesn't exist (will show error when trying to load)
        print(f"⚠️  Warning: File not found: {filename}")
        if fallback:
            print(f"⚠️  Fallback also not found: {fallback}")
        return filename
    
    def _clean_column_names(self, df):
        """Clean column names by trimming whitespace."""
        for column in df.columns:
            cleaned_name = column.strip()
            if cleaned_name != column:
                df = df.withColumnRenamed(column, cleaned_name)
        return df
    
    def load_all_data(self):
        """Load all datasets into Spark DataFrames."""
        if self._data_loaded:
            print("ℹ️  Data already loaded")
            return
        
        print("\n🔄 Loading all datasets into PySpark...")
        print(f"📁 Current working directory: {os.getcwd()}\n")
        
        # Load hospitals
        hospitals_path = self._get_file_path(self.HOSPITALS_FILE, self.HOSPITALS_FALLBACK)
        try:
            self._hospitals_spark = self._spark.read.csv(
                hospitals_path, 
                header=True, 
                inferSchema=True,
                encoding="UTF-8"
            )
            self._hospitals_spark = self._clean_column_names(self._hospitals_spark)
            print(f"  ✅ Hospitals: {self._hospitals_spark.count()} rows")
        except Exception as e:
            print(f"  ❌ Error loading hospitals: {str(e)}")
            raise
        
        # Load fire stations
        stations_path = self._get_file_path(self.FIRE_STATIONS_FILE, self.FIRE_STATIONS_FALLBACK)
        try:
            self._fire_stations_spark = self._spark.read.csv(
                stations_path,
                header=True,
                inferSchema=True,
                encoding="UTF-8"
            )
            self._fire_stations_spark = self._clean_column_names(self._fire_stations_spark)
            print(f"  ✅ Fire Stations: {self._fire_stations_spark.count()} rows")
        except Exception as e:
            print(f"  ❌ Error loading fire stations: {str(e)}")
            raise
        
        # Load fire disaster data
        disaster_path = self._get_file_path(self.FIRE_DISASTER_FILE)
        try:
            self._fire_disaster_spark = self._spark.read.csv(
                disaster_path,
                header=True,
                inferSchema=True,
                encoding="UTF-8"
            )
            self._fire_disaster_spark = self._clean_column_names(self._fire_disaster_spark)
            # Filter out header row (row with _id = 1)
            self._fire_disaster_spark = self._fire_disaster_spark.filter(col("_id") != 1)
            print(f"  ✅ Fire Incidents: {self._fire_disaster_spark.count()} rows")
        except Exception as e:
            print(f"  ❌ Error loading fire disaster data: {str(e)}")
            raise
        
        # Load population data
        population_path = self._get_file_path(self.POPULATION_FILE)
        try:
            self._population_spark = self._spark.read.csv(
                population_path,
                header=True,
                inferSchema=True,
                encoding="UTF-8"
            )
            self._population_spark = self._clean_column_names(self._population_spark)
            print(f"  ✅ Population: {self._population_spark.count()} rows")
        except Exception as e:
            print(f"  ❌ Error loading population data: {str(e)}")
            raise
        
        self._data_loaded = True
        print("✅ All data loaded successfully!\n")
    
    # ========================================================================
    # SPARK DATAFRAME ACCESS METHODS
    # ========================================================================
    
    def get_hospitals_spark(self):
        """Get hospitals data as PySpark DataFrame."""
        if not self._data_loaded:
            self.load_all_data()
        return self._hospitals_spark
    
    def get_fire_stations_spark(self):
        """Get fire stations data as PySpark DataFrame."""
        if not self._data_loaded:
            self.load_all_data()
        return self._fire_stations_spark
    
    def get_fire_disaster_spark(self):
        """Get fire disaster data as PySpark DataFrame."""
        if not self._data_loaded:
            self.load_all_data()
        return self._fire_disaster_spark
    
    def get_population_spark(self):
        """Get population data as PySpark DataFrame."""
        if not self._data_loaded:
            self.load_all_data()
        return self._population_spark
    
    # ========================================================================
    # PANDAS DATAFRAME ACCESS METHODS (for Streamlit visualization)
    # ========================================================================
    
    def get_hospitals_pandas(self) -> pd.DataFrame:
        """Get hospitals data as Pandas DataFrame."""
        return self.get_hospitals_spark().toPandas()
    
    def get_fire_stations_pandas(self) -> pd.DataFrame:
        """Get fire stations data as Pandas DataFrame."""
        return self.get_fire_stations_spark().toPandas()
    
    def get_fire_disaster_pandas(self) -> pd.DataFrame:
        """Get fire disaster data as Pandas DataFrame."""
        return self.get_fire_disaster_spark().toPandas()
    
    def get_population_pandas(self) -> pd.DataFrame:
        """Get population data as Pandas DataFrame."""
        return self.get_population_spark().toPandas()
    
    def get_all_pandas(self) -> dict:
        """
        Get all datasets as Pandas DataFrames.
        
        Returns:
            Dictionary with keys: 'hospitals', 'fire_stations', 'fire_disaster', 'population'
        """
        return {
            'hospitals': self.get_hospitals_pandas(),
            'fire_stations': self.get_fire_stations_pandas(),
            'fire_disaster': self.get_fire_disaster_pandas(),
            'population': self.get_population_pandas()
        }
    
    # ========================================================================
    # UTILITY METHODS
    # ========================================================================
    
    def get_summary(self) -> dict:
        """Get summary statistics for all datasets."""
        if not self._data_loaded:
            self.load_all_data()
        
        return {
            'hospitals': {
                'count': self._hospitals_spark.count(),
                'columns': self._hospitals_spark.columns
            },
            'fire_stations': {
                'count': self._fire_stations_spark.count(),
                'columns': self._fire_stations_spark.columns
            },
            'fire_disaster': {
                'count': self._fire_disaster_spark.count(),
                'columns': self._fire_disaster_spark.columns
            },
            'population': {
                'count': self._population_spark.count(),
                'columns': self._population_spark.columns
            }
        }
    
    def stop(self):
        """Stop the Spark session."""
        if self._spark:
            self._spark.stop()
            print("🛑 Spark session stopped")


# ============================================================================
# CONVENIENCE FUNCTIONS (for quick access without instantiating class)
# ============================================================================

# Global instance
_store = None

def get_store() -> DataStore:
    """Get the global DataStore instance."""
    global _store
    if _store is None:
        _store = DataStore()
        _store.load_all_data()
    return _store


def get_hospitals() -> pd.DataFrame:
    """Quick access: Get hospitals as Pandas DataFrame."""
    return get_store().get_hospitals_pandas()


def get_fire_stations() -> pd.DataFrame:
    """Quick access: Get fire stations as Pandas DataFrame."""
    return get_store().get_fire_stations_pandas()


def get_fire_disaster() -> pd.DataFrame:
    """Quick access: Get fire disaster data as Pandas DataFrame."""
    return get_store().get_fire_disaster_pandas()


def get_population() -> pd.DataFrame:
    """Quick access: Get population data as Pandas DataFrame."""
    return get_store().get_population_pandas()


def get_all() -> dict:
    """Quick access: Get all datasets as Pandas DataFrames."""
    return get_store().get_all_pandas()


# ============================================================================
# EXAMPLE USAGE
# ============================================================================

if __name__ == "__main__":
    """
    Example usage when running this file directly.
    """
    print("="*70)
    print("PYSPARK DATA STORE - DEMO")
    print("="*70)
    
    # Method 1: Using the class
    store = DataStore()
    store.load_all_data()
    
    hospitals = store.get_hospitals_pandas()
    print(f"\n🏥 Hospitals DataFrame shape: {hospitals.shape}")
    print(f"Columns: {hospitals.columns.tolist()}")
    print("\nFirst 3 rows:")
    print(hospitals.head(3))
    
    # Method 2: Using convenience functions
    fire_stations = get_fire_stations()
    print(f"\n🚒 Fire Stations DataFrame shape: {fire_stations.shape}")
    print(f"Columns: {fire_stations.columns.tolist()}")
    
    # Get summary
    summary = store.get_summary()
    print("\n📊 Data Summary:")
    for dataset, info in summary.items():
        print(f"  {dataset}: {info['count']} rows, {len(info['columns'])} columns")
    
    # Clean up
    store.stop()
    
    print("\n✅ Demo complete!")
    print("="*70)
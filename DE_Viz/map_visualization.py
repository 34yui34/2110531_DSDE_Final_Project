"""
Bangkok Emergency Services - Interactive Map Dashboard
========================================================

This Streamlit application provides 7 different map visualizations:
1. Bangkok Districts Map (GeoJSON)
2. Hospital Locations with Info
3. Fire Station Locations with Info
4. Fire Incidents by District
5. Population by District (Male/Female)
6. Total Issues by District (NEW)
7. Issue Classification by District (NEW)

Usage:
    streamlit run map_visualization.py

Author: Bangkok Emergency Services Analysis
Date: December 2024
"""

import streamlit as st
import pandas as pd
import folium
from folium import plugins
from streamlit_folium import st_folium
import json
import os

# Import our data store
from data_store import DataStore

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="Bangkok Emergency Services Maps",
    page_icon="🗺️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main-header {
        font-size: 3.5rem;
        color: #FF4B4B;
        font-weight: bold;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.5rem;
        color: #666;
        margin-bottom: 1rem;
    }
    .map-description {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
        border-left: 4px solid #FF4B4B;
    }
    /* Hide the white divider line */
    .css-1d391kg, div[data-testid="stHorizontalBlock"] {
        border: none !important;
    }
    hr {
        display: none !important;
    }
    </style>
""", unsafe_allow_html=True)


# ============================================================================
# DATA LOADING
# ============================================================================

@st.cache_resource
def load_data():
    """Load all data using DataStore."""
    store = DataStore()
    store.load_all_data()
    return store


@st.cache_data
def load_geojson():
    """
    Load Bangkok districts GeoJSON.
    You need to provide a GeoJSON file with Bangkok districts.
    """
    geojson_path = "districts.geojson"
    
    # Check multiple locations - including current working directory and script directory
    script_dir = os.path.dirname(os.path.abspath(__file__)) if '__file__' in globals() else os.getcwd()
    
    possible_paths = [
        geojson_path,  # Current directory
        os.path.join(script_dir, geojson_path),  # Same folder as script
        os.path.join(os.getcwd(), geojson_path),  # Working directory
        f"/home/claude/{geojson_path}",
        f"/mnt/user-data/uploads/{geojson_path}",
        f"/mnt/user-data/outputs/{geojson_path}",
        r"C:\Users\SorapatPun\Desktop\VScode\DSDE Project\2110531_DSDE_Final_Project\DE_Viz\bangkok_districts.geojson"
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            print(f"✅ Found GeoJSON at: {path}")
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
    
    # Print where we looked
    print("❌ GeoJSON not found. Searched in:")
    for path in possible_paths:
        print(f"   - {path}")
    
    # Return None if not found
    return None


def get_available_complaint_files():
    """
    Get list of all available complaint CSV files in shared_data directory.
    Returns list of tuples: (display_name, file_path)
    """
    possible_base_paths = [
        "../DE_AI/shared_data/",
        "./shared_data/",
        "/shared_data/",
        r"C:\Users\SorapatPun\Desktop\VScode\DSDE Project\2110531_DSDE_Final_Project\DE_AI\shared_data\\",
    ]
    
    available_files = []
    
    for base_path in possible_base_paths:
        if os.path.exists(base_path):
            # Find all CSV files in the directory
            try:
                csv_files = [f for f in os.listdir(base_path) if f.endswith('.csv')]
                
                for csv_file in csv_files:
                    full_path = os.path.join(base_path, csv_file)
                    # Get file modification time for display
                    mod_time = os.path.getmtime(full_path)
                    mod_time_str = pd.Timestamp.fromtimestamp(mod_time).strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Create display name with timestamp
                    display_name = f"{csv_file} (Modified: {mod_time_str})"
                    available_files.append((display_name, full_path, mod_time))
            except Exception as e:
                print(f"Error reading directory {base_path}: {e}")
                continue
            
            if available_files:
                break  # Found files, stop searching
    
    # Sort by modification time (newest first)
    available_files.sort(key=lambda x: x[2], reverse=True)
    
    return [(name, path) for name, path, _ in available_files]


def load_complaints_data(file_path=None):
    """
    Load classified complaints data from CSV file.
    This function does NOT cache, so it always reads the latest file.
    
    Args:
        file_path: Specific path to CSV file. If None, uses default search.
    """
    # If specific path provided, use it
    if file_path:
        if os.path.exists(file_path):
            print(f"✅ Loading complaints data from: {file_path}")
            try:
                df = pd.read_csv(file_path)
                # Filter for Bangkok only
                if 'province' in df.columns:
                    df = df[df['province'] == 'กรุงเทพมหานคร'].copy()
                return df
            except Exception as e:
                print(f"❌ Error loading {file_path}: {e}")
                return pd.DataFrame()
        else:
            print(f"❌ File not found: {file_path}")
            return pd.DataFrame()
    
    # Original default search logic
    possible_paths = [
        "labeled_output.csv",  # Current directory
        "../DE_AI/shared_data/labeled_output.csv",  # From DE_Viz to DE_AI/shared_data
        r"C:\Users\SorapatPun\Desktop\VScode\DSDE Project\2110531_DSDE_Final_Project\DE_AI\shared_data\labeled_output.csv",  # Absolute Windows path
        "/shared_data/labeled_output.csv",  # Docker shared volume
        "/mnt/user-data/uploads/labeled_output.csv",  # Uploaded file
        "./dataset/labeled_output.csv",  # Dataset folder
        "../shared_data/labeled_output.csv",  # Parent shared_data
        "./shared_data/labeled_output.csv",  # Local shared_data
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            print(f"✅ Found complaints data at: {path}")
            try:
                df = pd.read_csv(path)
                # Filter for Bangkok only
                if 'province' in df.columns:
                    df = df[df['province'] == 'กรุงเทพมหานคร'].copy()
                return df
            except Exception as e:
                print(f"❌ Error loading {path}: {e}")
                continue
    
    print("❌ Complaints data not found. Searched in:")
    for path in possible_paths:
        print(f"   - {path}")
    
    return pd.DataFrame()  # Return empty DataFrame if not found


# ============================================================================
# MAP CREATION FUNCTIONS
# ============================================================================

def create_base_map(zoom_start=11):
    """Create a base Folium map centered on Bangkok."""
    bangkok_center = [13.7563, 100.5018]
    
    m = folium.Map(
        location=bangkok_center,
        zoom_start=zoom_start,
        tiles='OpenStreetMap'
    )
    
    return m


def create_map_1_districts(geojson_data):
    """
    MAP 1: Bangkok Districts (GeoJSON)
    Shows the administrative boundaries of Bangkok districts.
    """
    m = create_base_map(zoom_start=11)
    
    if geojson_data is None:
        st.error("""
        ❌ GeoJSON file not found! 
        
        Please download Bangkok districts GeoJSON and place it in the same directory as this script.
        Name it: bangkok_districts.geojson
        
        You can get Bangkok GeoJSON from sources like:
        - https://github.com/apisit/thailand.json
        - https://data.go.th/
        """)
        return m
    
    # Filter for Bangkok districts only
    bangkok_geojson = geojson_data.copy()
    
    # List of Bangkok district names (in Thai and English)
    bangkok_districts_th = [
        'พระนคร', 'ดุสิต', 'หนองจอก', 'บางรัก', 'บางเขน', 'บางกะปิ', 'ปทุมวัน',
        'ป้อมปราบศัตรูพ่าย', 'พระโขนง', 'มีนบุรี', 'ลาดกระบัง', 'ยานนาวา', 'สัมพันธวงศ์',
        'พญาไท', 'ธนบุรี', 'บางกอกใหญ่', 'ห้วยขวาง', 'คลองสาน', 'ตลิ่งชัน', 'บางกอกน้อย',
        'บางขุนเทียน', 'ภาษีเจริญ', 'หนองแขม', 'ราษฎร์บูรณะ', 'บางพลัด', 'ดินแดง',
        'บึงกุ่ม', 'สาทร', 'บางซื่อ', 'จตุจักร', 'บางคอแหลม', 'ประเวศ', 'คลองเตย',
        'สวนหลวง', 'จอมทอง', 'ดอนเมือง', 'ราชเทวี', 'ลาดพร้าว', 'วัฒนา', 'บางแค',
        'หลักสี่', 'สายไหม', 'คันนายาว', 'สะพานสูง', 'วังทองหลาง', 'คลองสามวา',
        'บางนา', 'ทวีวัฒนา', 'ทุ่งครุ', 'บางบอน'
    ]
    
    bangkok_districts_en = [
        'Phra Nakhon', 'Dusit', 'Nong Chok', 'Bang Rak', 'Bang Khen', 'Bang Kapi',
        'Pathum Wan', 'Pom Prap Sattru Phai', 'Phra Khanong', 'Min Buri', 'Lat Krabang',
        'Yan Nawa', 'Samphanthawong', 'Phaya Thai', 'Thon Buri', 'Bangkok Yai',
        'Huai Khwang', 'Khlong San', 'Taling Chan', 'Bangkok Noi', 'Bang Khun Thian',
        'Phasi Charoen', 'Nong Khaem', 'Rat Burana', 'Bang Phlat', 'Din Daeng',
        'Bueng Kum', 'Sathon', 'Bang Sue', 'Chatuchak', 'Bang Kho Laem', 'Prawet',
        'Khlong Toei', 'Suan Luang', 'Chom Thong', 'Don Mueang', 'Ratchathewi',
        'Lat Phrao', 'Watthana', 'Bang Khae', 'Lak Si', 'Sai Mai', 'Khan Na Yao',
        'Saphan Sung', 'Wang Thonglang', 'Khlong Sam Wa', 'Bang Na', 'Thawi Watthana',
        'Thung Khru', 'Bang Bon'
    ]
    
    # Filter features to only include Bangkok districts
    if 'features' in bangkok_geojson:
        bangkok_geojson['features'] = [
            f for f in bangkok_geojson['features']
            if f['properties'].get('amp_th') in bangkok_districts_th
            or f['properties'].get('amp_en') in bangkok_districts_en
        ]
    
    # Add GeoJSON layer with styling (only Bangkok districts)
    folium.GeoJson(
        bangkok_geojson,
        name='Bangkok Districts',
        style_function=lambda feature: {
            'fillColor': '#3186cc',
            'color': '#2c5aa0',
            'weight': 2,
            'fillOpacity': 0.3,
        },
        highlight_function=lambda feature: {
            'fillColor': '#ff6b6b',
            'color': '#c92a2a',
            'weight': 3,
            'fillOpacity': 0.6,
        },
        tooltip=folium.GeoJsonTooltip(
            fields=['amp_en', 'amp_th'],
            aliases=['District (EN):', 'District (TH):'],
            style="background-color: white; color: #333; font-family: arial; font-size: 12px; padding: 10px;"
        )
    ).add_to(m)
    
    folium.LayerControl().add_to(m)
    
    return m


def create_map_2_hospitals(hospitals_df):
    """
    MAP 2: Hospital Locations
    Shows hospitals with name and telephone on hover.
    """
    m = create_base_map(zoom_start=11)
    
    # Check if coordinates exist
    if 'latitude' not in hospitals_df.columns or 'longitude' not in hospitals_df.columns:
        st.warning("⚠️ No coordinates found. Please run geocode_locations.py first!")
        return m
    
    # Get column names
    name_col = 'Hospital Name (English)' if 'Hospital Name (English)' in hospitals_df.columns else hospitals_df.columns[1]
    phone_col = 'หมายเลขโทรศัพท์ (Telephone)' if 'หมายเลขโทรศัพท์ (Telephone)' in hospitals_df.columns else hospitals_df.columns[-1]
    
    # Add markers for each hospital
    for idx, row in hospitals_df.iterrows():
        if pd.notna(row['latitude']) and pd.notna(row['longitude']):
            # Create popup HTML
            popup_html = f"""
            <div style="font-family: Arial; width: 250px;">
                <h4 style="color: #FF4B4B; margin-bottom: 8px;">🏥 {row[name_col]}</h4>
                <p style="margin: 4px 0;"><strong>📞 Telephone:</strong><br>{row[phone_col]}</p>
            </div>
            """
            
            # Create tooltip (shown on hover)
            tooltip_text = f"🏥 {row[name_col]}"
            
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=tooltip_text,
                icon=folium.Icon(color='red', icon='plus-sign', prefix='glyphicon')
            ).add_to(m)
    
    return m


def create_map_3_fire_stations(fire_stations_df):
    """
    MAP 3: Fire Station Locations
    Shows fire stations with name and telephone on hover.
    """
    m = create_base_map(zoom_start=11)
    
    # Check if coordinates exist
    if 'latitude' not in fire_stations_df.columns or 'longitude' not in fire_stations_df.columns:
        st.warning("⚠️ No coordinates found. Please run geocode_locations.py first!")
        return m
    
    # Get column names
    name_col = 'ชื่อ' if 'ชื่อ' in fire_stations_df.columns else fire_stations_df.columns[0]
    phone_col = 'หมายเลขโทรศัพท์' if 'หมายเลขโทรศัพท์' in fire_stations_df.columns else fire_stations_df.columns[1]
    
    # Add markers for each fire station
    for idx, row in fire_stations_df.iterrows():
        if pd.notna(row['latitude']) and pd.notna(row['longitude']):
            # Create popup HTML
            popup_html = f"""
            <div style="font-family: Arial; width: 250px;">
                <h4 style="color: #FF6B35; margin-bottom: 8px;">🚒 {row[name_col]}</h4>
                <p style="margin: 4px 0;"><strong>📞 Telephone:</strong><br>{row[phone_col]}</p>
            </div>
            """
            
            # Create tooltip
            tooltip_text = f"🚒 {row[name_col]}"
            
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=folium.Popup(popup_html, max_width=300),
                tooltip=tooltip_text,
                icon=folium.Icon(color='orange', icon='fire', prefix='glyphicon')
            ).add_to(m)
    
    return m


def create_map_4_fire_incidents(geojson_data, fire_disaster_df):
    """
    MAP 4: Fire Incidents by District
    Shows district boundaries with incident count on hover.
    """
    m = create_base_map(zoom_start=11)
    
    if geojson_data is None:
        st.error("❌ GeoJSON file not found! Please add bangkok_districts.geojson")
        return m
    
    # Count incidents by district
    if 'District' in fire_disaster_df.columns:
        incident_counts = fire_disaster_df['District'].value_counts().to_dict()
    else:
        incident_counts = {}
        st.warning("⚠️ 'District' column not found in fire disaster data")
    
    # Calculate max for color scaling
    max_incidents = max(incident_counts.values()) if incident_counts else 1
    
    # Color scale function
    def get_color(incident_count):
        if incident_count == 0:
            return '#cccccc'
        elif incident_count < max_incidents * 0.25:
            return '#fff5eb'
        elif incident_count < max_incidents * 0.5:
            return '#ffb347'
        elif incident_count < max_incidents * 0.75:
            return '#ff8c42'
        else:
            return '#d62828'
    
    # Style function for each district
    def style_function(feature):
        # Try to match district name from GeoJSON with fire disaster data
        district_name_en = feature['properties'].get('amp_en', '')
        district_name_th = feature['properties'].get('amp_th', '')
        
        # Try matching both English and Thai names
        incident_count = incident_counts.get(district_name_en, 0)
        if incident_count == 0:
            incident_count = incident_counts.get(district_name_th, 0)
        
        return {
            'fillColor': get_color(incident_count),
            'color': '#2c5aa0',
            'weight': 2,
            'fillOpacity': 0.7,
        }
    
    # Highlight function
    def highlight_function(feature):
        return {
            'fillColor': '#ff6b6b',
            'color': '#c92a2a',
            'weight': 3,
            'fillOpacity': 0.9,
        }
    
    # Create tooltip function with incident count
    def create_tooltip_fields(feature):
        district_name_en = feature['properties'].get('amp_en', '')
        district_name_th = feature['properties'].get('amp_th', '')
        
        # Get incident count
        incident_count = incident_counts.get(district_name_en, 0)
        if incident_count == 0:
            incident_count = incident_counts.get(district_name_th, 0)
        
        # Update properties with incident count for tooltip
        feature['properties']['incidents'] = incident_count
        return feature
    
    # Update features with incident counts
    updated_geojson = geojson_data.copy()
    updated_geojson['features'] = [create_tooltip_fields(f) for f in updated_geojson['features']]
    
    # Add GeoJSON with incident data
    folium.GeoJson(
        updated_geojson,
        name='Fire Incidents by District',
        style_function=style_function,
        highlight_function=highlight_function,
        tooltip=folium.GeoJsonTooltip(
            fields=['amp_en', 'amp_th', 'incidents'],
            aliases=['District (EN):', 'District (TH):', '🔥 Fire Incidents:'],
            style="background-color: white; color: #333; font-family: arial; font-size: 12px; padding: 10px;"
        )
    ).add_to(m)
    
    # Add legend
    legend_html = f"""
    <div style="position: fixed; bottom: 50px; left: 50px; width: 200px; 
                background-color: white; border: 2px solid grey; z-index: 9999; 
                padding: 10px; font-size: 14px;">
        <p style="margin: 0 0 10px 0; font-weight: bold;">Fire Incidents</p>
        <p style="margin: 2px 0;"><span style="background-color: {get_color(0)}; padding: 5px 10px;">█</span> 0</p>
        <p style="margin: 2px 0;"><span style="background-color: {get_color(max_incidents*0.2)}; padding: 5px 10px;">█</span> Low</p>
        <p style="margin: 2px 0;"><span style="background-color: {get_color(max_incidents*0.5)}; padding: 5px 10px;">█</span> Medium</p>
        <p style="margin: 2px 0;"><span style="background-color: {get_color(max_incidents*0.8)}; padding: 5px 10px;">█</span> High</p>
        <p style="margin: 2px 0;"><span style="background-color: {get_color(max_incidents)}; padding: 5px 10px;">█</span> Very High</p>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))
    
    return m


def create_map_5_population(geojson_data, population_df):
    """
    MAP 5: Population by District (Male/Female)
    Shows district boundaries with population data on hover.
    """
    m = create_base_map(zoom_start=11)
    
    if geojson_data is None:
        st.error("❌ GeoJSON file not found! Please add bangkok_districts.geojson")
        return m
    
    # Create population dictionary - match by district name
    pop_dict = {}
    for idx, row in population_df.iterrows():
        district = row['DISTRICT']
        pop_dict[district] = {
            'total': row['TOTAL'],
            'male': row['MALE'],
            'female': row['FEMALE']
        }
    
    # Calculate max for color scaling
    max_population = population_df['TOTAL'].max() if len(population_df) > 0 else 1
    
    # Color scale function
    def get_color(population):
        if population == 0:
            return '#cccccc'
        elif population < max_population * 0.25:
            return '#e8f4f8'
        elif population < max_population * 0.5:
            return '#90caf9'
        elif population < max_population * 0.75:
            return '#42a5f5'
        else:
            return '#1565c0'
    
    # Style function
    def style_function(feature):
        # Try both English and Thai names
        district_name_en = feature['properties'].get('amp_en', '')
        district_name_th = feature['properties'].get('amp_th', '')
        
        # Try matching
        pop_data = pop_dict.get(district_name_en, None)
        if pop_data is None:
            pop_data = pop_dict.get(district_name_th, {'total': 0, 'male': 0, 'female': 0})
        
        return {
            'fillColor': get_color(pop_data['total']),
            'color': '#1565c0',
            'weight': 2,
            'fillOpacity': 0.7,
        }
    
    # Highlight function
    def highlight_function(feature):
        return {
            'fillColor': '#4fc3f7',
            'color': '#01579b',
            'weight': 3,
            'fillOpacity': 0.9,
        }
    
    # Add population data to features
    def add_population_to_feature(feature):
        district_name_en = feature['properties'].get('amp_en', '')
        district_name_th = feature['properties'].get('amp_th', '')
        
        # Try matching
        pop_data = pop_dict.get(district_name_en, None)
        if pop_data is None:
            pop_data = pop_dict.get(district_name_th, {'total': 0, 'male': 0, 'female': 0})
        
        # Add to properties
        feature['properties']['population_total'] = f"{pop_data['total']:,}"
        feature['properties']['population_male'] = f"{pop_data['male']:,}"
        feature['properties']['female'] = f"{pop_data['female']:,}"
        return feature
    
    # Update GeoJSON with population data
    updated_geojson = geojson_data.copy()
    updated_geojson['features'] = [add_population_to_feature(f) for f in updated_geojson['features']]
    
    # Add GeoJSON with population data
    folium.GeoJson(
        updated_geojson,
        name='Population by District',
        style_function=style_function,
        highlight_function=highlight_function,
        tooltip=folium.GeoJsonTooltip(
            fields=['amp_en', 'amp_th', 'population_total', 'population_male', 'female'],
            aliases=['District (EN):', 'District (TH):', '👥 Total:', '👨 Male:', '👩 Female:'],
            style="background-color: white; color: #333; font-family: arial; font-size: 12px; padding: 10px;"
        )
    ).add_to(m)
    
    # Add legend
    legend_html = f"""
    <div style="position: fixed; bottom: 50px; left: 50px; width: 200px; 
                background-color: white; border: 2px solid grey; z-index: 9999; 
                padding: 10px; font-size: 14px;">
        <p style="margin: 0 0 10px 0; font-weight: bold;">Population</p>
        <p style="margin: 2px 0;"><span style="background-color: {get_color(0)}; padding: 5px 10px;">█</span> 0</p>
        <p style="margin: 2px 0;"><span style="background-color: {get_color(max_population*0.2)}; padding: 5px 10px;">█</span> Low</p>
        <p style="margin: 2px 0;"><span style="background-color: {get_color(max_population*0.5)}; padding: 5px 10px;">█</span> Medium</p>
        <p style="margin: 2px 0;"><span style="background-color: {get_color(max_population*0.8)}; padding: 5px 10px;">█</span> High</p>
        <p style="margin: 2px 0;"><span style="background-color: {get_color(max_population)}; padding: 5px 10px;">█</span> Very High</p>
        <p style="margin: 10px 0 0 0; font-size: 12px; color: #666;">
            Hover over districts for details
        </p>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))
    
    return m


def create_map_6_total_issues(geojson_data, complaints_df):
    """
    MAP 6: Total Issues by District
    Shows total number of issues reported in each district.
    Hover over district to see total issue count.
    """
    m = create_base_map(zoom_start=11)
    
    if geojson_data is None:
        st.error("❌ GeoJSON file not found! Please add bangkok_districts.geojson")
        return m
    
    if complaints_df.empty:
        st.warning("⚠️ No complaints data available. Please run the AI Classifier first.")
        return m
    
    # Count total issues by district
    if 'district' not in complaints_df.columns:
        st.warning("⚠️ 'district' column not found in complaints data")
        return m
    
    issue_counts = complaints_df['district'].value_counts().to_dict()
    total_issues = len(complaints_df)
    
    # Calculate max for color scaling
    max_issues = max(issue_counts.values()) if issue_counts else 1
    
    # Color scale function - Red gradient (more issues = darker red)
    def get_color(issue_count):
        if issue_count == 0:
            return '#e0e0e0'  # Light gray for no issues
        elif issue_count < max_issues * 0.2:
            return '#ffcdd2'  # Very light red
        elif issue_count < max_issues * 0.4:
            return '#ef9a9a'  # Light red
        elif issue_count < max_issues * 0.6:
            return '#e57373'  # Medium red
        elif issue_count < max_issues * 0.8:
            return '#ef5350'  # Dark red
        else:
            return '#c62828'  # Very dark red (critical)
    
    # Style function
    def style_function(feature):
        district_name_th = feature['properties'].get('amp_th', '')
        issue_count = issue_counts.get(district_name_th, 0)
        
        return {
            'fillColor': get_color(issue_count),
            'color': '#424242',
            'weight': 2,
            'fillOpacity': 0.75,
        }
    
    # Highlight function
    def highlight_function(feature):
        return {
            'fillColor': '#ffd54f',  # Yellow highlight
            'color': '#000000',
            'weight': 3,
            'fillOpacity': 0.9,
        }
    
    # Add issue counts to features
    def add_issues_to_feature(feature):
        district_name_th = feature['properties'].get('amp_th', '')
        issue_count = issue_counts.get(district_name_th, 0)
        
        # Calculate percentage
        percentage = (issue_count / total_issues * 100) if total_issues > 0 else 0
        
        feature['properties']['total_issues'] = issue_count
        feature['properties']['percentage'] = f"{percentage:.1f}%"
        return feature
    
    # Update GeoJSON with issue counts
    updated_geojson = geojson_data.copy()
    updated_geojson['features'] = [add_issues_to_feature(f) for f in updated_geojson['features']]
    
    # Add GeoJSON layer
    folium.GeoJson(
        updated_geojson,
        name='Total Issues by District',
        style_function=style_function,
        highlight_function=highlight_function,
        tooltip=folium.GeoJsonTooltip(
            fields=['amp_en', 'amp_th', 'total_issues', 'percentage'],
            aliases=['District (EN):', 'District (TH):', '📋 Total Issues:', '📊 Percentage:'],
            style="background-color: white; color: #333; font-family: arial; font-size: 13px; padding: 12px; font-weight: 500;"
        )
    ).add_to(m)
    
    # Add legend
    legend_html = f"""
    <div style="position: fixed; bottom: 50px; left: 50px; width: 220px; 
                background-color: white; border: 2px solid #424242; z-index: 9999; 
                padding: 12px; font-size: 14px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.15);">
        <p style="margin: 0 0 12px 0; font-weight: bold; font-size: 15px; color: #c62828;">📋 Total Issues</p>
        <p style="margin: 4px 0; display: flex; align-items: center;">
            <span style="background-color: {get_color(0)}; padding: 8px 12px; margin-right: 8px; border-radius: 4px;">█</span> 
            <span>No Issues</span>
        </p>
        <p style="margin: 4px 0; display: flex; align-items: center;">
            <span style="background-color: {get_color(max_issues*0.15)}; padding: 8px 12px; margin-right: 8px; border-radius: 4px;">█</span> 
            <span>Very Low</span>
        </p>
        <p style="margin: 4px 0; display: flex; align-items: center;">
            <span style="background-color: {get_color(max_issues*0.35)}; padding: 8px 12px; margin-right: 8px; border-radius: 4px;">█</span> 
            <span>Low</span>
        </p>
        <p style="margin: 4px 0; display: flex; align-items: center;">
            <span style="background-color: {get_color(max_issues*0.55)}; padding: 8px 12px; margin-right: 8px; border-radius: 4px;">█</span> 
            <span>Medium</span>
        </p>
        <p style="margin: 4px 0; display: flex; align-items: center;">
            <span style="background-color: {get_color(max_issues*0.75)}; padding: 8px 12px; margin-right: 8px; border-radius: 4px;">█</span> 
            <span>High</span>
        </p>
        <p style="margin: 4px 0; display: flex; align-items: center;">
            <span style="background-color: {get_color(max_issues)}; padding: 8px 12px; margin-right: 8px; border-radius: 4px;">█</span> 
            <span>Critical</span>
        </p>
        <hr style="margin: 10px 0; border: none; border-top: 1px solid #ddd;">
        <p style="margin: 8px 0 0 0; font-size: 12px; color: #666;">
            <strong>Total:</strong> {total_issues:,} issues<br>
            <strong>Highest:</strong> {max_issues} issues
        </p>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))
    
    return m


def create_map_7_classification_detail(geojson_data, complaints_df):
    """
    MAP 7: Issue Classification by District
    Shows detailed breakdown of classifications (Sufficient, Not Sufficient, API Error, Uncertain)
    Hover over district to see detailed classification counts.
    """
    m = create_base_map(zoom_start=11)
    
    if geojson_data is None:
        st.error("❌ GeoJSON file not found! Please add bangkok_districts.geojson")
        return m
    
    if complaints_df.empty:
        st.warning("⚠️ No complaints data available. Please run the AI Classifier first.")
        return m
    
    # Check required columns
    if 'district' not in complaints_df.columns or 'llm_label' not in complaints_df.columns:
        st.warning("⚠️ Required columns not found in complaints data")
        return m
    
    # Group by district and classification
    district_classification = complaints_df.groupby(['district', 'llm_label']).size().unstack(fill_value=0)
    
    # Calculate severity score for coloring
    # Not Sufficient and API Error are high priority
    if 'Not Sufficient' in district_classification.columns:
        district_classification['severity_score'] = (
            district_classification.get('Not Sufficient', 0) * 3 +
            district_classification.get('API Error', 0) * 2.5 +
            district_classification.get('Uncertain (has partial evidence)', 0) * 1.5 +
            district_classification.get('Sufficient', 0) * 0.5
        )
    else:
        district_classification['severity_score'] = 0
    
    max_severity = district_classification['severity_score'].max() if len(district_classification) > 0 else 1
    
    # Color scale based on severity (mix of issues)
    def get_color(severity):
        if severity == 0:
            return '#e8f5e9'  # Very light green (no issues)
        elif severity < max_severity * 0.2:
            return '#fff9c4'  # Light yellow
        elif severity < max_severity * 0.4:
            return '#ffeb3b'  # Yellow
        elif severity < max_severity * 0.6:
            return '#ff9800'  # Orange
        elif severity < max_severity * 0.8:
            return '#ff5722'  # Deep orange
        else:
            return '#d32f2f'  # Red (critical)
    
    # Style function
    def style_function(feature):
        district_name_th = feature['properties'].get('amp_th', '')
        
        if district_name_th in district_classification.index:
            severity = district_classification.loc[district_name_th, 'severity_score']
        else:
            severity = 0
        
        return {
            'fillColor': get_color(severity),
            'color': '#424242',
            'weight': 2,
            'fillOpacity': 0.75,
        }
    
    # Highlight function
    def highlight_function(feature):
        return {
            'fillColor': '#64b5f6',  # Light blue highlight
            'color': '#000000',
            'weight': 3,
            'fillOpacity': 0.9,
        }
    
    # Add classification details to features
    def add_classification_to_feature(feature):
        district_name_th = feature['properties'].get('amp_th', '')
        
        if district_name_th in district_classification.index:
            stats = district_classification.loc[district_name_th]
            feature['properties']['sufficient'] = int(stats.get('Sufficient', 0))
            feature['properties']['not_sufficient'] = int(stats.get('Not Sufficient', 0))
            feature['properties']['api_error'] = int(stats.get('API Error', 0))
            feature['properties']['uncertain'] = int(stats.get('Uncertain (has partial evidence)', 0))
            feature['properties']['total'] = int(stats.sum() - stats.get('severity_score', 0))
        else:
            feature['properties']['sufficient'] = 0
            feature['properties']['not_sufficient'] = 0
            feature['properties']['api_error'] = 0
            feature['properties']['uncertain'] = 0
            feature['properties']['total'] = 0
        
        return feature
    
    # Update GeoJSON with classification data
    updated_geojson = geojson_data.copy()
    updated_geojson['features'] = [add_classification_to_feature(f) for f in updated_geojson['features']]
    
    # Add GeoJSON layer
    folium.GeoJson(
        updated_geojson,
        name='Issue Classification',
        style_function=style_function,
        highlight_function=highlight_function,
        tooltip=folium.GeoJsonTooltip(
            fields=['amp_en', 'amp_th', 'total', 'sufficient', 'not_sufficient', 'uncertain', 'api_error'],
            aliases=[
                'District (EN):', 
                'District (TH):', 
                '📊 Total Issues:', 
                '✅ Sufficient:', 
                '❌ Not Sufficient:', 
                '⚠️ Uncertain:', 
                '🔴 API Error:'
            ],
            style="background-color: white; color: #333; font-family: arial; font-size: 13px; padding: 12px; font-weight: 500;"
        )
    ).add_to(m)
    
    # Add detailed legend
    legend_html = f"""
    <div style="position: fixed; bottom: 50px; left: 50px; width: 240px; 
                background-color: white; border: 2px solid #424242; z-index: 9999; 
                padding: 12px; font-size: 13px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.15);">
        <p style="margin: 0 0 10px 0; font-weight: bold; font-size: 15px; color: #d32f2f;">🎯 Issue Severity</p>
        <p style="margin: 4px 0; display: flex; align-items: center;">
            <span style="background-color: {get_color(0)}; padding: 8px 12px; margin-right: 8px; border-radius: 4px;">█</span> 
            <span>No Issues</span>
        </p>
        <p style="margin: 4px 0; display: flex; align-items: center;">
            <span style="background-color: {get_color(max_severity*0.15)}; padding: 8px 12px; margin-right: 8px; border-radius: 4px;">█</span> 
            <span>Very Low</span>
        </p>
        <p style="margin: 4px 0; display: flex; align-items: center;">
            <span style="background-color: {get_color(max_severity*0.35)}; padding: 8px 12px; margin-right: 8px; border-radius: 4px;">█</span> 
            <span>Low</span>
        </p>
        <p style="margin: 4px 0; display: flex; align-items: center;">
            <span style="background-color: {get_color(max_severity*0.55)}; padding: 8px 12px; margin-right: 8px; border-radius: 4px;">█</span> 
            <span>Moderate</span>
        </p>
        <p style="margin: 4px 0; display: flex; align-items: center;">
            <span style="background-color: {get_color(max_severity*0.75)}; padding: 8px 12px; margin-right: 8px; border-radius: 4px;">█</span> 
            <span>High</span>
        </p>
        <p style="margin: 4px 0; display: flex; align-items: center;">
            <span style="background-color: {get_color(max_severity)}; padding: 8px 12px; margin-right: 8px; border-radius: 4px;">█</span> 
            <span>Critical</span>
        </p>
        <hr style="margin: 10px 0; border: none; border-top: 1px solid #ddd;">
        <p style="margin: 6px 0 2px 0; font-size: 12px; font-weight: bold; color: #555;">Classification Legend:</p>
        <p style="margin: 2px 0; font-size: 11px;">✅ <strong>Sufficient</strong> - Resolved</p>
        <p style="margin: 2px 0; font-size: 11px;">❌ <strong>Not Sufficient</strong> - Need action</p>
        <p style="margin: 2px 0; font-size: 11px;">⚠️ <strong>Uncertain</strong> - Partial info</p>
        <p style="margin: 2px 0; font-size: 11px;">🔴 <strong>API Error</strong> - Failed to classify</p>
        <hr style="margin: 8px 0; border: none; border-top: 1px solid #ddd;">
        <p style="margin: 4px 0 0 0; font-size: 11px; color: #666; font-style: italic;">
            Severity = Not Suff.×3 + API Err.×2.5 + Uncertain×1.5 + Sufficient×0.5
        </p>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))
    
    return m


# ============================================================================
# MAIN APPLICATION
# ============================================================================

def main():
    """Main Streamlit application."""
    
    # Header
    st.markdown('<p class="main-header">🗺️ Bangkok Emergency Services Map Dashboard</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Interactive geospatial visualization of hospitals, fire stations, incidents, population, and complaint analysis</p>', unsafe_allow_html=True)
    
    # Load data
    with st.spinner("Loading data..."):
        store = load_data()
        geojson_data = load_geojson()
        
        # Get available complaint files
        available_files = get_available_complaint_files()
        
        hospitals_df = store.get_hospitals_pandas()
        fire_stations_df = store.get_fire_stations_pandas()
        fire_disaster_df = store.get_fire_disaster_pandas()
        population_df = store.get_population_pandas()
    
    # Sidebar
    st.sidebar.title("🗺️ Map Selection")
    
    # ========================================================================
    # NEW: File selector in sidebar
    # ========================================================================
    st.sidebar.markdown("---")
    st.sidebar.subheader("📁 Data Source Selection")
    
    selected_file_path = None
    if available_files:
        st.sidebar.success(f"✅ Found {len(available_files)} CSV file(s)")
        
        file_options = ["Default (latest labeled_output.csv)"] + [name for name, _ in available_files]
        file_paths = [None] + [path for _, path in available_files]
        
        selected_index = st.sidebar.selectbox(
            "Select Complaint Data File:",
            range(len(file_options)),
            format_func=lambda x: file_options[x],
            key="file_selector"
        )
        
        selected_file_path = file_paths[selected_index]
        
        if selected_file_path:
            st.sidebar.info(f"📄 Using: {os.path.basename(selected_file_path)}")
        else:
            st.sidebar.info("📄 Using: Default search paths")
    else:
        st.sidebar.warning("⚠️ No CSV files found in shared_data")
    
    # Add refresh button
    if st.sidebar.button("🔄 Refresh File List", key="refresh_files"):
        st.rerun()
    
    # Load complaints with selected file
    complaints_df = load_complaints_data(selected_file_path)
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("Choose a map to visualize:")
    
    # ========================================================================
    # Map dropdown
    # ========================================================================
    map_options = [
        "1. Bangkok Districts (Boundaries)",
        "2. Hospital Locations",
        "3. Fire Station Locations",
        "4. Fire Incidents by District",
        "5. Population by District",
        "6. Total Issues by District",
        "7. Issue Classification by District"
    ]
    
    selected_map = st.sidebar.selectbox(
        "Select Map Type:",
        map_options,
        index=0
    )
    
    st.sidebar.markdown("---")
    
    # Show data statistics in sidebar
    st.sidebar.subheader("📊 Data Overview")
    st.sidebar.metric("🏥 Hospitals", len(hospitals_df))
    st.sidebar.metric("🚒 Fire Stations", len(fire_stations_df))
    st.sidebar.metric("🔥 Fire Incidents", len(fire_disaster_df))
    st.sidebar.metric("👥 Total Population", f"{population_df['TOTAL'].sum():,}")
    
    # Add complaints metrics
    if not complaints_df.empty:
        st.sidebar.metric("📋 Classified Issues", len(complaints_df))
        if 'llm_label' in complaints_df.columns:
            sufficient_count = (complaints_df['llm_label'] == 'Sufficient').sum()
            not_sufficient_count = (complaints_df['llm_label'] == 'Not Sufficient').sum()
            st.sidebar.metric("✅ Sufficient", sufficient_count)
            st.sidebar.metric("❌ Not Sufficient", not_sufficient_count)
    
    
    # Bangkok District Name Mapping (Thai to English)
    district_mapping = {
        'พระนคร': 'Phra Nakhon',
        'ดุสิต': 'Dusit',
        'หนองจอก': 'Nong Chok',
        'บางรัก': 'Bang Rak',
        'บางเขน': 'Bang Khen',
        'บางกะปิ': 'Bang Kapi',
        'ปทุมวัน': 'Pathum Wan',
        'ป้อมปราบศัตรูพ่าย': 'Pom Prap Sattru Phai',
        'พระโขนง': 'Phra Khanong',
        'มีนบุรี': 'Min Buri',
        'ลาดกระบัง': 'Lat Krabang',
        'ยานนาวา': 'Yan Nawa',
        'สัมพันธวงศ์': 'Samphanthawong',
        'พญาไท': 'Phaya Thai',
        'ธนบุรี': 'Thon Buri',
        'บางกอกใหญ่': 'Bangkok Yai',
        'ห้วยขวาง': 'Huai Khwang',
        'คลองสาน': 'Khlong San',
        'ตลิ่งชัน': 'Taling Chan',
        'บางกอกน้อย': 'Bangkok Noi',
        'บางขุนเทียน': 'Bang Khun Thian',
        'ภาษีเจริญ': 'Phasi Charoen',
        'หนองแขม': 'Nong Khaem',
        'ราษฎร์บูรณะ': 'Rat Burana',
        'บางพลัด': 'Bang Phlat',
        'ดินแดง': 'Din Daeng',
        'บึงกุ่ม': 'Bueng Kum',
        'สาทร': 'Sathon',
        'บางซื่อ': 'Bang Sue',
        'จตุจักร': 'Chatuchak',
        'บางคอแหลม': 'Bang Kho Laem',
        'ประเวศ': 'Prawet',
        'คลองเตย': 'Khlong Toei',
        'สวนหลวง': 'Suan Luang',
        'จอมทอง': 'Chom Thong',
        'ดอนเมือง': 'Don Mueang',
        'ราชเทวี': 'Ratchathewi',
        'ลาดพร้าว': 'Lat Phrao',
        'วัฒนา': 'Watthana',
        'บางแค': 'Bang Khae',
        'หลักสี่': 'Lak Si',
        'สายไหม': 'Sai Mai',
        'คันนายาว': 'Khan Na Yao',
        'สะพานสูง': 'Saphan Sung',
        'วังทองหลาง': 'Wang Thonglang',
        'คลองสามวา': 'Khlong Sam Wa',
        'บางนา': 'Bang Na',
        'ทวีวัฒนา': 'Thawi Watthana',
        'ทุ่งครุ': 'Thung Khru',
        'บางบอน': 'Bang Bon'
    }
    
    # Main content area
    if selected_map == map_options[0]:
        # Map 1: Districts
        st.markdown('<div class="map-description">', unsafe_allow_html=True)
        st.markdown("### 📍 Map 1: Bangkok Districts")
        st.markdown("This map shows the administrative boundaries of Bangkok's 50 districts.")
        st.markdown('</div>', unsafe_allow_html=True)
        
        map_obj = create_map_1_districts(geojson_data)
        st_folium(map_obj, width=1200, height=600)
        
    elif selected_map == map_options[1]:
        # Map 2: Hospitals
        st.markdown('<div class="map-description">', unsafe_allow_html=True)
        st.markdown("### 🏥 Map 2: Hospital Locations")
        st.markdown(f"Showing **{len(hospitals_df)}** hospitals in Bangkok. Hover over markers to see names, click for telephone numbers.")
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Check if geocoded
        has_coords = 'latitude' in hospitals_df.columns and hospitals_df['latitude'].notna().sum() > 0
        if not has_coords:
            st.warning("⚠️ **Coordinates not found!** Please run `python geocode_locations.py` first to add location data.")
        
        map_obj = create_map_2_hospitals(hospitals_df)
        st_folium(map_obj, width=1200, height=600)
        
    elif selected_map == map_options[2]:
        # Map 3: Fire Stations
        st.markdown('<div class="map-description">', unsafe_allow_html=True)
        st.markdown("### 🚒 Map 3: Fire Station Locations")
        st.markdown(f"Showing **{len(fire_stations_df)}** fire stations in Bangkok. Hover over markers to see names, click for telephone numbers.")
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Check if geocoded
        has_coords = 'latitude' in fire_stations_df.columns and fire_stations_df['latitude'].notna().sum() > 0
        if not has_coords:
            st.warning("⚠️ **Coordinates not found!** Please run `python geocode_locations.py` first to add location data.")
        
        map_obj = create_map_3_fire_stations(fire_stations_df)
        st_folium(map_obj, width=1200, height=600)
        
    elif selected_map == map_options[3]:
        # Map 4: Fire Incidents by District
        st.markdown('<div class="map-description">', unsafe_allow_html=True)
        st.markdown("### 🔥 Map 4: Fire Incidents by District")
        
        # Filter for Bangkok only
        bangkok_incidents = fire_disaster_df[fire_disaster_df['Province'] == 'กรุงเทพมหานคร'].copy() if 'Province' in fire_disaster_df.columns else fire_disaster_df
        
        st.markdown(f"Showing fire incident distribution across Bangkok districts. Darker colors indicate more incidents. Total Bangkok incidents: **{len(bangkok_incidents)}** (out of {len(fire_disaster_df)} total)")
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Show top Bangkok districts with bilingual names
        if 'District' in bangkok_incidents.columns:
            top_districts = bangkok_incidents['District'].value_counts().head(5)
            st.markdown("**Top 5 Bangkok Districts by Incidents:**")
            for district_th, count in top_districts.items():
                district_en = district_mapping.get(district_th, district_th)
                st.markdown(f"- {district_th} ({district_en}): **{count}** incidents")
        
        # Use Bangkok-filtered data for the map
        map_obj = create_map_4_fire_incidents(geojson_data, bangkok_incidents)
        st_folium(map_obj, width=1200, height=600)
        
    elif selected_map == map_options[4]:
        # Map 5: Population by District
        st.markdown('<div class="map-description">', unsafe_allow_html=True)
        st.markdown("### 👥 Map 5: Population by District")
        st.markdown(f"Showing population distribution across Bangkok. Total population: **{population_df['TOTAL'].sum():,}**")
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Show statistics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("👨 Male", f"{population_df['MALE'].sum():,}")
        with col2:
            st.metric("👩 Female", f"{population_df['FEMALE'].sum():,}")
        with col3:
            avg_pop = int(population_df['TOTAL'].mean())
            st.metric("📊 Avg per District", f"{avg_pop:,}")
        
        # Show top districts
        st.markdown("**Top 5 Most Populated Districts:**")
        top_pop = population_df.nlargest(5, 'TOTAL')
        for idx, row in top_pop.iterrows():
            st.markdown(f"- {row['DISTRICT']}: **{row['TOTAL']:,}** (Male: {row['MALE']:,}, Female: {row['FEMALE']:,})")
        
        map_obj = create_map_5_population(geojson_data, population_df)
        st_folium(map_obj, width=1200, height=600)
    
    elif selected_map == map_options[5]:
        # Map 6: Total Issues by District
        st.markdown('<div class="map-description">', unsafe_allow_html=True)
        st.markdown("### 📋 Map 6: Total Issues by District")
        
        if complaints_df.empty:
            st.warning("⚠️ **No complaints data available.** Please run the AI Classifier to generate classified data.")
            st.info("💡 **Tip:** Use the file selector in the sidebar to choose a different CSV file, or generate new data with the AI Classifier.")
        else:
            total_complaints = len(complaints_df)
            
            # Show which file is being used
            if selected_file_path:
                st.info(f"📂 **Currently viewing:** {os.path.basename(selected_file_path)}")
            else:
                st.info(f"📂 **Currently viewing:** Default (labeled_output.csv)")
            
            st.markdown(f"Showing the distribution of **{total_complaints:,}** reported issues across Bangkok districts. Hover over districts to see total issue counts.")
            
            # Show top 5 districts
            if 'district' in complaints_df.columns:
                top_issue_districts = complaints_df['district'].value_counts().head(5)
                st.markdown("**Top 5 Districts by Total Issues:**")
                col1, col2 = st.columns([3, 2])
                with col1:
                    for district_th, count in top_issue_districts.items():
                        district_en = district_mapping.get(district_th, district_th)
                        percentage = (count / total_complaints * 100)
                        st.markdown(f"- **{district_th}** ({district_en}): {count:,} issues ({percentage:.1f}%)")
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        map_obj = create_map_6_total_issues(geojson_data, complaints_df)
        st_folium(map_obj, width=1200, height=600)
    
    elif selected_map == map_options[6]:
        # Map 7: Issue Classification by District
        st.markdown('<div class="map-description">', unsafe_allow_html=True)
        st.markdown("### 🎯 Map 7: Issue Classification by District")
        
        if complaints_df.empty:
            st.warning("⚠️ **No complaints data available.** Please run the AI Classifier to generate classified data.")
            st.info("💡 **Tip:** Use the file selector in the sidebar to choose a different CSV file, or generate new data with the AI Classifier.")
        else:
            total_complaints = len(complaints_df)
            
            # Show which file is being used
            if selected_file_path:
                st.info(f"📂 **Currently viewing:** {os.path.basename(selected_file_path)}")
            else:
                st.info(f"📂 **Currently viewing:** Default (labeled_output.csv)")
            
            st.markdown(f"Showing detailed classification breakdown for **{total_complaints:,}** issues. Hover over districts to see: ✅ Sufficient, ❌ Not Sufficient, ⚠️ Uncertain, and 🔴 API Error counts.")
            
            # Show overall classification statistics
            if 'llm_label' in complaints_df.columns:
                st.markdown("**Overall Classification Summary:**")
                col1, col2, col3, col4 = st.columns(4)
                
                label_counts = complaints_df['llm_label'].value_counts()
                
                with col1:
                    sufficient = label_counts.get('Sufficient', 0)
                    st.metric("✅ Sufficient", f"{sufficient:,}", 
                             delta=f"{(sufficient/total_complaints*100):.1f}%", delta_color="off")
                
                with col2:
                    not_sufficient = label_counts.get('Not Sufficient', 0)
                    st.metric("❌ Not Sufficient", f"{not_sufficient:,}", 
                             delta=f"{(not_sufficient/total_complaints*100):.1f}%", delta_color="off")
                
                with col3:
                    uncertain = label_counts.get('Uncertain (has partial evidence)', 0)
                    st.metric("⚠️ Uncertain", f"{uncertain:,}", 
                             delta=f"{(uncertain/total_complaints*100):.1f}%", delta_color="off")
                
                with col4:
                    api_error = label_counts.get('API Error', 0)
                    st.metric("🔴 API Error", f"{api_error:,}", 
                             delta=f"{(api_error/total_complaints*100):.1f}%", delta_color="off")
                
                # Show top problem districts
                st.markdown("**Top 5 Districts with Most 'Not Sufficient' Issues:**")
                not_sufficient_by_district = complaints_df[complaints_df['llm_label'] == 'Not Sufficient']['district'].value_counts().head(5)
                for district_th, count in not_sufficient_by_district.items():
                    district_en = district_mapping.get(district_th, district_th)
                    st.markdown(f"- **{district_th}** ({district_en}): {count} unresolved issues")
        
        st.markdown('</div>', unsafe_allow_html=True)
        
        map_obj = create_map_7_classification_detail(geojson_data, complaints_df)
        st_folium(map_obj, width=1200, height=600)


# ============================================================================
# RUN APPLICATION
# ============================================================================

if __name__ == "__main__":
    main()
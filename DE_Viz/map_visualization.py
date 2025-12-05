"""
Bangkok Emergency Services - Interactive Map Dashboard
========================================================

This Streamlit application provides 5 different map visualizations:
1. Bangkok Districts Map (GeoJSON)
2. Hospital Locations with Info
3. Fire Station Locations with Info
4. Fire Incidents by District
5. Population by District (Male/Female)

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
        font-size: 2.5rem;
        color: #FF4B4B;
        font-weight: bold;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        margin-bottom: 2rem;
    }
    .map-description {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin-bottom: 1rem;
        border-left: 4px solid #FF4B4B;
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
    
    # Add GeoJSON layer with styling
    folium.GeoJson(
        geojson_data,
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


# ============================================================================
# MAIN APPLICATION
# ============================================================================

def main():
    """Main Streamlit application."""
    
    # Header
    st.markdown('<p class="main-header">🗺️ Bangkok Emergency Services Map Dashboard</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Interactive geospatial visualization of hospitals, fire stations, incidents, and population</p>', unsafe_allow_html=True)
    
    # Load data
    with st.spinner("Loading data..."):
        store = load_data()
        geojson_data = load_geojson()
        
        hospitals_df = store.get_hospitals_pandas()
        fire_stations_df = store.get_fire_stations_pandas()
        fire_disaster_df = store.get_fire_disaster_pandas()
        population_df = store.get_population_pandas()
    
    # Sidebar
    st.sidebar.title("🗺️ Map Selection")
    st.sidebar.markdown("Choose a map to visualize:")
    
    # Map dropdown
    map_options = [
        "1. Bangkok Districts (Boundaries)",
        "2. Hospital Locations",
        "3. Fire Station Locations",
        "4. Fire Incidents by District",
        "5. Population by District"
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
    
    st.sidebar.markdown("---")
    st.sidebar.info("""
    **💡 Tips:**
    - Hover over markers/districts for details
    - Click markers for full information
    - Use mouse wheel to zoom
    - Drag to pan the map
    """)
    
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
        st.markdown(f"Showing fire incident distribution across Bangkok districts. Darker colors indicate more incidents. Total incidents: **{len(fire_disaster_df)}**")
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Show top districts
        if 'District' in fire_disaster_df.columns:
            top_districts = fire_disaster_df['District'].value_counts().head(5)
            st.markdown("**Top 5 Districts by Incidents:**")
            for district, count in top_districts.items():
                st.markdown(f"- {district}: **{count}** incidents")
        
        map_obj = create_map_4_fire_incidents(geojson_data, fire_disaster_df)
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
    
    # Footer
    st.markdown("---")
    st.markdown("### 📝 Notes")
    st.info("""
    **About the Maps:**
    - Maps 1, 4, and 5 require a GeoJSON file for district boundaries
    - Maps 2 and 3 require geocoded coordinates (run geocode_locations.py)
    - All maps are interactive - zoom, pan, and click for more information
    
    **Data Sources:**
    - Hospital data: bangkok_hospitals.csv
    - Fire station data: fire_stations.csv
    - Fire incident data: fire_disaster_data.csv
    - Population data: population.csv
    """)


# ============================================================================
# RUN APPLICATION
# ============================================================================

if __name__ == "__main__":
    main()
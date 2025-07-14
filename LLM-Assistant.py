from groq import Groq
import json
import pandas as pd
from datetime import datetime, timedelta
import numpy as np
import os
from dotenv import load_dotenv

load_dotenv()
# Initialize client
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

def get_response(prompt: str):
    """Get response from Groq API"""
    try:
        chat_completion = client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": prompt,
                }
            ],
            model="llama-3.3-70b-versatile",
        )
        return chat_completion.choices[0].message.content
    except Exception as e:
        print(f"Error getting response: {e}")
        return None

def load_earthquake_data(file_path):
    """Load earthquake dataset from CSV file"""
    try:
        print(f"Loading earthquake dataset from: {file_path}")
        df = pd.read_csv(file_path)
        print(f"Dataset loaded successfully with {len(df)} rows and {len(df.columns)} columns")
        
        # Display basic info about the dataset
        print("\nDataset columns:")
        print(df.columns.tolist())
        print(f"\nDataset shape: {df.shape}")
        print(f"Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        return df
        
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found!")
        print("Please make sure the file path is correct.")
        return None
    except Exception as e:
        print(f"Error loading dataset: {e}")
        return None

def analyze_dataset(df):
    """Analyze the earthquake dataset and return key statistics"""
    print("\n=== DATASET ANALYSIS ===")
    
    # Basic statistics
    print(f"Total earthquakes: {len(df):,}")
    print(f"Columns: {len(df.columns)}")
    
    # Check for key columns and their statistics
    key_stats = {}
    
    # Magnitude analysis (using 'mag' column based on your dataset)
    if 'mag' in df.columns:
        mag_stats = df['mag'].describe()
        key_stats['magnitude'] = {
            'min': mag_stats['min'],
            'max': mag_stats['max'],
            'mean': mag_stats['mean'],
            'std': mag_stats['std']
        }
        print(f"\nMagnitude range: {mag_stats['min']:.2f} to {mag_stats['max']:.2f}")
        print(f"Average magnitude: {mag_stats['mean']:.2f}")
        print(f"Magnitude > 5.0: {len(df[df['mag'] > 5.0]):,} earthquakes")
        print(f"Magnitude > 6.0: {len(df[df['mag'] > 6.0]):,} earthquakes")
    
    # Depth analysis
    if 'depth' in df.columns:
        depth_stats = df['depth'].describe()
        key_stats['depth'] = {
            'min': depth_stats['min'],
            'max': depth_stats['max'],
            'mean': depth_stats['mean']
        }
        print(f"\nDepth range: {depth_stats['min']:.2f} to {depth_stats['max']:.2f} km")
        print(f"Average depth: {depth_stats['mean']:.2f} km")
        print(f"Shallow earthquakes (<10km): {len(df[df['depth'] < 10]):,}")
    
    # Time range analysis
    if 'time' in df.columns:
        try:
            df['time'] = pd.to_datetime(df['time'])
            key_stats['time_range'] = {
                'start': df['time'].min().strftime('%Y-%m-%d'),
                'end': df['time'].max().strftime('%Y-%m-%d')
            }
            print(f"\nTime range: {key_stats['time_range']['start']} to {key_stats['time_range']['end']}")
            
            # Yearly distribution
            yearly_counts = df['time'].dt.year.value_counts().sort_index()
            print("Yearly earthquake counts:")
            for year, count in yearly_counts.items():
                print(f"  {year}: {count:,}")
                
        except Exception as e:
            print(f"\nTime column found but couldn't parse dates: {e}")
    
    # Geographic analysis
    if 'latitude' in df.columns and 'longitude' in df.columns:
        lat_range = (df['latitude'].min(), df['latitude'].max())
        lon_range = (df['longitude'].min(), df['longitude'].max())
        key_stats['geography'] = {
            'lat_range': lat_range,
            'lon_range': lon_range
        }
        print(f"\nGeographic coverage:")
        print(f"  Latitude: {lat_range[0]:.2f} to {lat_range[1]:.2f}")
        print(f"  Longitude: {lon_range[0]:.2f} to {lon_range[1]:.2f}")
    
    # State analysis
    if 'state' in df.columns:
        state_counts = df['state'].value_counts().head(10)
        key_stats['top_states'] = state_counts.to_dict()
        print(f"\nTop 10 states by earthquake count:")
        for state, count in state_counts.items():
            print(f"  {state}: {count:,}")
    
    # County analysis
    if 'county' in df.columns:
        county_counts = df['county'].value_counts().head(5)
        print(f"\nTop 5 counties by earthquake count:")
        for county, count in county_counts.items():
            print(f"  {county}: {count:,}")
    
    # Magnitude type analysis
    if 'magType' in df.columns:
        mag_type_counts = df['magType'].value_counts()
        print(f"\nMagnitude types:")
        for mag_type, count in mag_type_counts.items():
            print(f"  {mag_type}: {count:,}")
    
    # Status analysis
    if 'status' in df.columns:
        status_counts = df['status'].value_counts()
        print(f"\nStatus distribution:")
        for status, count in status_counts.items():
            print(f"  {status}: {count:,}")
    
    return key_stats

def prepare_sample_for_analysis(df, sample_size=80):
    """Prepare a sample of the dataset for analysis"""
    print(f"\nPreparing sample of {sample_size} rows from {len(df)} total rows...")
    
    # Handle datetime columns for JSON serialization
    df_copy = df.copy()
    datetime_cols = df_copy.select_dtypes(include=["datetime64", "datetime64[ns]"]).columns
    for col in datetime_cols:
        df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Sample the data
    sample_data = df_copy.sample(n=min(sample_size, len(df_copy)), random_state=42).copy()
    
    # Convert to list of dictionaries for JSON serialization
    sample_data_dict = sample_data.to_dict(orient="records")
    
    return sample_data, sample_data_dict

if __name__ == "__main__":
    # Define your dataset path
    dataset_path = "../Dataset/Final/Earthquake[2020-2025]USA.csv"  # Update this path as needed
    
    # Load the earthquake dataset
    df = load_earthquake_data(dataset_path)
    
    if df is None:
        print("Failed to load dataset. Please check the file path and try again.")
        exit()
    
    # Analyze the dataset
    key_stats = analyze_dataset(df)
    
    # First prompt: how LLMs can be used for Data Warehousing
    print("\n" + "="*60)
    print("=== PROMPT 1: LLMs for Data Warehousing ===")
    print("="*60)
    
    prompt1 = """How can LLMs be used for Data Warehousing? Provide specific examples and benefits, 
    especially in the context of large-scale earthquake monitoring and seismic data analysis."""
    
    response1 = get_response(prompt1)
    if response1:
        print(response1)
    else:
        print("Failed to get response for prompt 1")
    
    print("\n" + "="*60)
    print("=== PROMPT 2: Earthquake Data Analysis ===")
    print("="*60)
    
    # Prepare sample for analysis
    sample_data, sample_data_dict = prepare_sample_for_analysis(df, sample_size=80)
    
    # Build comprehensive analysis prompt
    prompt2 = f"""
You are a senior data analyst creating a comprehensive Tableau dashboard for earthquake analysis.

DATASET OVERVIEW:
- Total records: {len(df):,} earthquakes (2020-2025 USA)
- Columns: {len(df.columns)} including: {', '.join(df.columns[:10])}...
- Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB
- Sample size for analysis: {len(sample_data)} records

SAMPLE DATA (first 6 records):
{json.dumps(sample_data_dict[:6], default=str, indent=2)}

DATASET STATISTICS:
{json.dumps(key_stats, indent=2, default=str)}

PROFESSOR'S REQUIREMENTS - PHASE 3: DATA VISUALIZATION
1. Determine the main story or insight from the dataset
2. Suggest visualizations (sheets/dashboards) using Tableau
3. Enable OLAP-style dynamic interaction: roll-up, drill-down, slice-and-dice
4. Structure recommendations for project presentation

ANALYSIS REQUIREMENTS:
Based on this massive earthquake dataset ({len(df):,} records), provide detailed recommendations for:

1. **PRIMARY INSIGHTS & STORIES**:
   - What are the key seismic patterns in the USA (2020-2025)?
   - Geographic hotspots and their characteristics
   - Temporal trends and seasonal variations
   - Magnitude-depth relationships and implications

2. **TABLEAU DASHBOARD ARCHITECTURE**:
   - Main dashboard layout with 6-8 key visualizations
   - Interactive map with drill-down capabilities
   - Time series analysis with multiple granularities
   - Statistical distribution charts
   - Correlation analysis visualizations

3. **OLAP FUNCTIONALITY**:
   - Roll-up: County → State → Region → National level analysis
   - Drill-down: Year → Month → Day → Hour granularity
   - Slice-and-dice: Filter by magnitude ranges, depth levels, geographic regions
   - Pivot capabilities for multi-dimensional analysis

4. **SPECIFIC CHART RECOMMENDATIONS**:
   - Geographic: Heat maps, symbol maps, filled maps
   - Temporal: Time series, calendar heatmaps, trend analysis
   - Statistical: Histograms, box plots, scatter plots
   - Comparative: Small multiples, dashboard filters

5. **DYNAMIC INTERACTIVITY**:
   - Parameter controls for magnitude thresholds
   - Date range selectors
   - Geographic filters (state, county, region)
   - Linked dashboards for seamless exploration

6. **KEY PERFORMANCE INDICATORS (KPIs)**:
   - Seismic activity metrics
   - Risk assessment indicators
   - Trend analysis summary statistics

7. **PROJECT ESSAY STRUCTURE**:
   - Introduction to seismic monitoring importance
   - Methodology and data processing approach
   - Key findings and visualizations
   - Implications for earthquake preparedness
   - Technical implementation details
   - Conclusions and recommendations

Please provide specific, actionable recommendations that leverage the full scale of this {len(df):,}-record dataset for comprehensive earthquake analysis and monitoring.
"""
    
    response2 = get_response(prompt2)
    if response2:
        print(response2)
    else:
        print("Failed to get response for prompt 2")
    
    # Save sample data for further analysis
    sample_filename = f'earthquake_sample_{len(sample_data)}_records.csv'
    sample_data.to_csv(sample_filename, index=False)
    print(f"\n" + "="*60)
    print(f"Sample data saved to '{sample_filename}' for Tableau analysis")
    print(f"Ready for import into Tableau with {len(sample_data)} representative records")
    print("="*60)
